package coordinator

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"forger/internal/config"
	"forger/internal/meta"
	"forger/internal/model"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Coordinator struct {
	cfg         config.Config
	db          *meta.Store
	client      *http.Client
	locks       sync.Map
	unreachable sync.Map
	cancel      context.CancelFunc
}
type keyLock struct{ mu sync.Mutex }

func New(cfg config.Config, db *meta.Store) *Coordinator {
	return &Coordinator{cfg: cfg, db: db, client: &http.Client{Timeout: cfg.RequestTimeout}}
}
func (c *Coordinator) Start(ctx context.Context) error {
	for id, u := range c.cfg.NodeURLs {
		if e := c.db.UpsertNode(ctx, id, u); e != nil {
			return e
		}
	}
	x, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	go c.periodic(x, c.cfg.HeartbeatInterval, c.healthTick)
	go c.periodic(x, 2*time.Second, c.repairTick)
	go c.periodic(x, c.cfg.IntegrityInterval, c.integrityTick)
	go c.periodic(x, c.cfg.ReconcileInterval, c.reconcileTick)
	go c.periodic(x, c.cfg.RebalanceInterval, c.rebalanceTick)
	return nil
}
func (c *Coordinator) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
}
func (c *Coordinator) periodic(ctx context.Context, d time.Duration, f func(context.Context)) {
	f(ctx)
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			f(ctx)
		}
	}
}
func (c *Coordinator) Handler() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })
	m.HandleFunc("/v1/objects/", c.objects)
	m.HandleFunc("/v1/objects", c.listObjects)
	m.HandleFunc("/v1/cluster", c.cluster)
	m.HandleFunc("/v1/events", c.events)
	m.HandleFunc("/v1/repairs", c.repairs)
	m.HandleFunc("/v1/admin/", c.admin)
	m.HandleFunc("/", c.dashboard)
	return m
}
func (c *Coordinator) healthTick(ctx context.Context) {
	nodes, e := c.db.Nodes(ctx)
	if e != nil {
		return
	}
	for _, n := range nodes {
		cap, used, ok := c.probe(ctx, n)
		st, e := c.db.RecordProbe(ctx, n.ID, ok, cap, used, c.cfg.HeartbeatFailures, c.cfg.OfflineFailures)
		if e == nil {
			// A heartbeat is a sample, not an event. Recording only transitions
			// keeps failures and recovery visible in the bounded event timeline.
			if st != n.State {
				c.db.Event(ctx, "NODE_HEALTH", "", 0, n.ID, fmt.Sprintf(`{"state":%q,"reachable":%t}`, st, ok))
			}
			if st == model.NodeUnreachable || st == model.NodeOffline {
				sinceAny, _ := c.unreachable.LoadOrStore(n.ID, time.Now())
				since := sinceAny.(time.Time)
				replace := st == model.NodeOffline && time.Since(since) >= c.cfg.RepairGrace
				c.degradeForNode(ctx, n.ID, replace)
			} else {
				c.unreachable.Delete(n.ID)
			}
			if st == model.NodeRecovering {
				c.reconcileTick(ctx)
			}
		}
	}
}
func (c *Coordinator) degradeForNode(ctx context.Context, nodeID string, replace bool) {
	versions, e := c.db.AllLatest(ctx)
	if e != nil {
		return
	}
	for _, v := range versions {
		rs, _ := c.db.Replicas(ctx, v.Key, v.Version)
		for _, r := range rs {
			if r.NodeID == nodeID && (r.State == model.ReplicaHealthy || r.State == model.ReplicaUnreachable) {
				if r.State == model.ReplicaHealthy {
					_ = c.db.SetReplicaState(ctx, v.Key, v.Version, nodeID, model.ReplicaUnreachable)
					_ = c.db.MarkObject(ctx, v.Key, model.ObjectDegraded)
					c.db.Event(ctx, "OBJECT_DEGRADED", v.Key, v.Version, nodeID, `{}`)
				}
				if replace {
					c.scheduleRepair(ctx, v.Key, v.Version, "NODE_OFFLINE")
				}
			}
		}
	}
}
func (c *Coordinator) probe(ctx context.Context, n model.Node) (int64, int64, bool) {
	req, e := http.NewRequestWithContext(ctx, http.MethodGet, n.BaseURL+"/health", nil)
	if e != nil {
		return 0, 0, false
	}
	res, e := c.client.Do(req)
	if e != nil {
		return 0, 0, false
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return 0, 0, false
	}
	var x struct {
		Capacity int64 `json:"capacity_bytes"`
		Used     int64 `json:"used_bytes"`
	}
	if json.NewDecoder(res.Body).Decode(&x) != nil {
		return 0, 0, false
	}
	return x.Capacity, x.Used, true
}
func (c *Coordinator) nodeDo(req *http.Request) (*http.Response, error) {
	if c.cfg.InternalToken != "" {
		req.Header.Set("X-Forger-Internal-Token", c.cfg.InternalToken)
	}
	return c.client.Do(req)
}
func (c *Coordinator) objects(w http.ResponseWriter, r *http.Request) {
	key, e := url.PathUnescape(strings.TrimPrefix(r.URL.Path, "/v1/objects/"))
	if e != nil || key == "" {
		http.Error(w, "object key", 400)
		return
	}
	switch r.Method {
	case http.MethodPut:
		c.put(w, r, key)
	case http.MethodGet:
		if r.URL.Query().Get("metadata") == "1" {
			c.head(w, r, key)
		} else {
			c.get(w, r, key)
		}
	case http.MethodHead:
		c.head(w, r, key)
	default:
		http.Error(w, "method", 405)
	}
}
func (c *Coordinator) lock(key string) *keyLock {
	x, _ := c.locks.LoadOrStore(key, &keyLock{})
	return x.(*keyLock)
}
func policy(r *http.Request, cfg config.Config) (int, int, string, error) {
	p := strings.ToUpper(r.URL.Query().Get("policy"))
	rf, min := cfg.DefaultRF, cfg.DefaultMinSuccess
	switch p {
	case "":
		p = "BALANCED"
	case "FAST":
		rf, min = 2, 1
	case "BALANCED":
		rf, min = 3, 2
	case "DURABLE":
		rf, min = 3, 3
	default:
		return 0, 0, "", fmt.Errorf("unknown policy")
	}
	if x := r.URL.Query().Get("rf"); x != "" {
		var e error
		rf, e = strconv.Atoi(x)
		if e != nil {
			return 0, 0, "", e
		}
	}
	if x := r.URL.Query().Get("min_success"); x != "" {
		var e error
		min, e = strconv.Atoi(x)
		if e != nil {
			return 0, 0, "", e
		}
	}
	if rf < 1 || min < 1 || min > rf {
		return 0, 0, "", fmt.Errorf("invalid rf/min_success")
	}
	return rf, min, p, nil
}
func (c *Coordinator) put(w http.ResponseWriter, r *http.Request, key string) {
	rf, min, pol, e := policy(r, c.cfg)
	if e != nil {
		http.Error(w, e.Error(), 400)
		return
	}
	l := c.lock(key)
	l.mu.Lock()
	defer l.mu.Unlock()
	if e := os.MkdirAll(filepath.Join(c.cfg.DataDir, "spool"), 0750); e != nil {
		http.Error(w, e.Error(), 500)
		return
	}
	f, e := os.CreateTemp(filepath.Join(c.cfg.DataDir, "spool"), "put-")
	if e != nil {
		http.Error(w, e.Error(), 500)
		return
	}
	spool := f.Name()
	defer os.Remove(spool)
	h := sha256.New()
	size, e := io.Copy(io.MultiWriter(f, h), r.Body)
	e2 := f.Close()
	if e != nil || e2 != nil {
		http.Error(w, "spool failed", 500)
		return
	}
	sum := hex.EncodeToString(h.Sum(nil))
	v, e := c.db.BeginVersion(r.Context(), key, rf, min, pol)
	if e != nil {
		http.Error(w, e.Error(), 500)
		return
	}
	nodes, e := c.place(r.Context(), rf, size, map[string]bool{})
	if e != nil {
		http.Error(w, e.Error(), 503)
		return
	}
	type ans struct {
		r   model.Replica
		err error
	}
	ch := make(chan ans, len(nodes))
	for _, n := range nodes {
		go func(n model.Node) {
			e := c.nodePut(r.Context(), n, key, v, spool, sum, size)
			ch <- ans{model.Replica{Key: key, Version: v, NodeID: n.ID, Path: "objects", Size: size, SHA256: sum}, e}
		}(n)
	}
	good := []model.Replica{}
	for range nodes {
		x := <-ch
		if x.err == nil {
			good = append(good, x.r)
		} else {
			c.db.Event(r.Context(), "REPLICA_WRITE_FAILED", key, v, x.r.NodeID, fmt.Sprintf(`{"error":%q}`, x.err.Error()))
		}
	}
	if len(good) < min {
		c.db.Event(r.Context(), "UPLOAD_FAILED", key, v, "", fmt.Sprintf(`{"verified":%d,"required":%d}`, len(good), min))
		http.Error(w, "minimum durability not met", 503)
		return
	}
	if e = c.db.CommitVersion(r.Context(), key, v, size, sum, rf, min, pol, good); e != nil {
		http.Error(w, e.Error(), 500)
		return
	}
	c.db.Event(r.Context(), "UPLOAD_COMMITTED", key, v, "", fmt.Sprintf(`{"replicas":%d,"desired":%d}`, len(good), rf))
	if len(good) < rf {
		c.scheduleRepair(r.Context(), key, v, "MISSING")
	}
	json.NewEncoder(w).Encode(map[string]any{"key": key, "version": v, "size": size, "sha256": sum, "verified_replicas": len(good), "desired_replicas": rf, "state": map[bool]string{true: "HEALTHY", false: "DEGRADED"}[len(good) >= rf]})
}
func (c *Coordinator) nodePath(key string, v int) string {
	return "/internal/objects/" + base64.RawURLEncoding.EncodeToString([]byte(key)) + "/" + strconv.Itoa(v)
}
func (c *Coordinator) nodePut(ctx context.Context, n model.Node, key string, v int, path, sum string, size int64) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()
	req, e := http.NewRequestWithContext(ctx, http.MethodPut, n.BaseURL+c.nodePath(key, v), f)
	if e != nil {
		return e
	}
	req.Header.Set("X-Forger-SHA256", sum)
	req.Header.Set("X-Forger-Size", strconv.FormatInt(size, 10))
	res, e := c.nodeDo(req)
	if e != nil {
		return e
	}
	defer res.Body.Close()
	if res.StatusCode != 201 {
		return fmt.Errorf("node %s: %s", n.ID, res.Status)
	}
	if res.Header.Get("X-Forger-SHA256") != sum {
		return fmt.Errorf("node acknowledgement checksum mismatch")
	}
	return nil
}
func (c *Coordinator) place(ctx context.Context, want int, size int64, exclude map[string]bool) ([]model.Node, error) {
	nodes, e := c.db.Nodes(ctx)
	if e != nil {
		return nil, e
	}
	out := []model.Node{}
	sort.Slice(nodes, func(i, j int) bool {
		ai := float64(nodes[i].UsedBytes) / float64(max(nodes[i].CapacityBytes, 1))
		aj := float64(nodes[j].UsedBytes) / float64(max(nodes[j].CapacityBytes, 1))
		return ai < aj
	})
	for _, n := range nodes {
		if exclude[n.ID] || n.State != model.NodeOnline || n.CapacityBytes-n.UsedBytes < size {
			continue
		}
		out = append(out, n)
		if len(out) == want {
			break
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no eligible storage nodes")
	}
	return out, nil
}
func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
func (c *Coordinator) get(w http.ResponseWriter, r *http.Request, key string) {
	o, v, reps, e := c.db.Latest(r.Context(), key)
	if e != nil {
		if e == sql.ErrNoRows {
			http.NotFound(w, r)
		} else {
			http.Error(w, e.Error(), 500)
		}
		return
	}
	nodes, _ := c.db.Nodes(r.Context())
	by := map[string]model.Node{}
	for _, n := range nodes {
		by[n.ID] = n
	}
	sort.SliceStable(reps, func(i, j int) bool {
		return by[reps[i].NodeID].State == model.NodeOnline && by[reps[j].NodeID].State != model.NodeOnline
	})
	for _, rp := range reps {
		if rp.State != model.ReplicaHealthy || by[rp.NodeID].State != model.NodeOnline {
			continue
		}
		tmp, e := os.CreateTemp(filepath.Join(c.cfg.DataDir, "spool"), "get-")
		if e != nil {
			http.Error(w, e.Error(), 500)
			return
		}
		path := tmp.Name()
		sum, n, err := c.nodeGetTo(r.Context(), by[rp.NodeID], key, v.Version, tmp)
		tmp.Close()
		if err == nil && sum == v.SHA256 && n == v.Size {
			defer os.Remove(path)
			w.Header().Set("X-Forger-Version", strconv.Itoa(v.Version))
			w.Header().Set("X-Forger-SHA256", sum)
			w.Header().Set("Content-Length", strconv.FormatInt(n, 10))
			f, _ := os.Open(path)
			defer f.Close()
			io.Copy(w, f)
			return
		}
		os.Remove(path)
		state := model.ReplicaMissing
		if err == nil {
			state = model.ReplicaCorrupted
		}
		_ = c.db.SetReplicaState(r.Context(), key, v.Version, rp.NodeID, state)
		c.db.Event(r.Context(), "READ_REPLICA_INVALID", key, v.Version, rp.NodeID, `{}`)
		c.scheduleRepair(r.Context(), key, v.Version, "READ_FAILOVER")
	}
	_ = o
	http.Error(w, "no verified replica available", 503)
}
func (c *Coordinator) nodeGetTo(ctx context.Context, n model.Node, key string, v int, dst io.Writer) (string, int64, error) {
	req, e := http.NewRequestWithContext(ctx, http.MethodGet, n.BaseURL+c.nodePath(key, v), nil)
	if e != nil {
		return "", 0, e
	}
	res, e := c.nodeDo(req)
	if e != nil {
		return "", 0, e
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return "", 0, fmt.Errorf("%s", res.Status)
	}
	h := sha256.New()
	nbytes, e := io.Copy(io.MultiWriter(dst, h), res.Body)
	return hex.EncodeToString(h.Sum(nil)), nbytes, e
}
func (c *Coordinator) head(w http.ResponseWriter, r *http.Request, key string) {
	o, v, reps, e := c.db.Latest(r.Context(), key)
	if e != nil {
		http.NotFound(w, r)
		return
	}
	json.NewEncoder(w).Encode(map[string]any{"object": o, "version": v, "replicas": reps})
}
func (c *Coordinator) listObjects(w http.ResponseWriter, r *http.Request) {
	x, e := c.db.Objects(r.Context())
	if e != nil {
		http.Error(w, e.Error(), 500)
		return
	}
	json.NewEncoder(w).Encode(x)
}
func (c *Coordinator) cluster(w http.ResponseWriter, r *http.Request) {
	x, e := c.db.Nodes(r.Context())
	if e != nil {
		http.Error(w, e.Error(), 500)
		return
	}
	json.NewEncoder(w).Encode(x)
}
func (c *Coordinator) events(w http.ResponseWriter, r *http.Request) {
	x, e := c.db.Events(r.Context(), 100)
	if e != nil {
		http.Error(w, e.Error(), 500)
		return
	}
	json.NewEncoder(w).Encode(x)
}
func (c *Coordinator) repairs(w http.ResponseWriter, r *http.Request) {
	x, e := c.db.Repairs(r.Context(), 100)
	if e != nil {
		http.Error(w, e.Error(), 500)
		return
	}
	json.NewEncoder(w).Encode(x)
}
func (c *Coordinator) scheduleRepair(ctx context.Context, key string, v int, reason string) {
	o, _, reps, e := c.db.Latest(ctx, key)
	if e != nil {
		return
	}
	nodes, e := c.db.Nodes(ctx)
	if e != nil {
		return
	}
	by := map[string]model.Node{}
	for _, n := range nodes {
		by[n.ID] = n
	}
	healthy := 0
	used := map[string]bool{}
	for _, r := range reps {
		used[r.NodeID] = true
		if r.State == model.ReplicaHealthy && by[r.NodeID].State == model.NodeOnline {
			healthy++
		}
	}
	if healthy >= o.ReplicationFactor {
		_ = c.db.MarkObject(ctx, key, model.ObjectHealthy)
		return
	}
	for _, r := range reps {
		if r.State != model.ReplicaHealthy && by[r.NodeID].State == model.NodeOnline {
			_ = c.db.QueueRepair(ctx, key, v, r.NodeID, reason)
			return
		}
	}
	targets, e := c.place(ctx, 1, 0, used)
	if e == nil && len(targets) > 0 {
		_ = c.db.QueueRepair(ctx, key, v, targets[0].ID, reason)
		_ = c.db.MarkObject(ctx, key, model.ObjectDegraded)
	}
}
func (c *Coordinator) repairTick(ctx context.Context) {
	j, e := c.db.ClaimRepair(ctx, 30*time.Second)
	if e != nil {
		return
	}
	ok, msg := c.runRepair(ctx, j)
	_ = c.db.FinishRepair(ctx, j.ID, ok, msg)
	if ok {
		// runRepair persists the selected source before the destination is
		// registered. Reload it so the observable event reflects the actual copy.
		source := j.Source
		if jobs, err := c.db.Repairs(ctx, 100); err == nil {
			for _, job := range jobs {
				if job.ID == j.ID {
					source = job.Source
					break
				}
			}
		}
		_, v, _, _ := c.db.Latest(ctx, j.Key)
		c.db.Event(ctx, "REPAIR_SUCCEEDED", j.Key, j.Version, j.Target, fmt.Sprintf(`{"source":%q,"target":%q,"reason":%q,"result":"verified","sha256":%q}`, source, j.Target, j.Reason, v.SHA256))
		c.scheduleRepair(ctx, j.Key, j.Version, "POST_REPAIR")
	} else {
		c.db.Event(ctx, "REPAIR_FAILED", j.Key, j.Version, j.Target, fmt.Sprintf(`{"error":%q}`, msg))
	}
}
func (c *Coordinator) runRepair(ctx context.Context, j meta.RepairJob) (bool, string) {
	o, v, reps, e := c.db.Latest(ctx, j.Key)
	if e != nil || v.Version != j.Version {
		return false, "version no longer current"
	}
	nodes, e := c.db.Nodes(ctx)
	if e != nil {
		return false, e.Error()
	}
	by := map[string]model.Node{}
	for _, n := range nodes {
		by[n.ID] = n
	}
	target, exists := by[j.Target]
	if !exists || target.State != model.NodeOnline {
		return false, "target unavailable"
	}
	var source model.Node
	found := false
	for _, r := range reps {
		n := by[r.NodeID]
		if r.State == model.ReplicaHealthy && n.State == model.NodeOnline && n.ID != target.ID && (j.Source == "" || j.Source == n.ID) {
			source = n
			found = true
			break
		}
	}
	if !found {
		return false, "no healthy source"
	}
	_ = c.db.UpdateRepairSource(ctx, j.ID, source.ID)
	if e := os.MkdirAll(filepath.Join(c.cfg.DataDir, "spool"), 0750); e != nil {
		return false, e.Error()
	}
	f, e := os.CreateTemp(filepath.Join(c.cfg.DataDir, "spool"), "repair-")
	if e != nil {
		return false, e.Error()
	}
	path := f.Name()
	defer os.Remove(path)
	sum, n, e := c.nodeGetTo(ctx, source, j.Key, j.Version, f)
	f.Close()
	if e != nil || sum != v.SHA256 || n != v.Size {
		return false, "source verification failed"
	}
	if e = c.nodePut(ctx, target, j.Key, j.Version, path, v.SHA256, v.Size); e != nil {
		return false, e.Error()
	}
	if e = c.db.RegisterReplica(ctx, model.Replica{Key: j.Key, Version: j.Version, NodeID: target.ID, Path: "objects", Size: v.Size, SHA256: v.SHA256}); e != nil {
		return false, e.Error()
	}
	if j.Reason == "REBALANCE" && source.ID != target.ID {
		latest, _ := c.db.Replicas(ctx, j.Key, j.Version)
		healthy := 0
		for _, r := range latest {
			if r.State == model.ReplicaHealthy {
				healthy++
			}
		}
		if healthy >= o.ReplicationFactor {
			req, _ := http.NewRequestWithContext(ctx, http.MethodDelete, source.BaseURL+c.nodePath(j.Key, j.Version), nil)
			res, er := c.nodeDo(req)
			if er == nil && res.StatusCode == 204 {
				res.Body.Close()
				_ = c.db.DeleteReplica(ctx, j.Key, j.Version, source.ID)
			} else if res != nil {
				res.Body.Close()
			}
		}
	}
	return true, ""
}
func (c *Coordinator) integrityTick(ctx context.Context) {
	versions, e := c.db.AllLatest(ctx)
	if e != nil {
		return
	}
	nodes, _ := c.db.Nodes(ctx)
	by := map[string]model.Node{}
	for _, n := range nodes {
		by[n.ID] = n
	}
	for _, v := range versions {
		rs, _ := c.db.Replicas(ctx, v.Key, v.Version)
		for _, r := range rs {
			if r.State != model.ReplicaHealthy || by[r.NodeID].State != model.NodeOnline {
				continue
			}
			valid, e := c.verifyNode(ctx, by[r.NodeID], v.Key, v.Version)
			if e != nil || !valid {
				_ = c.db.SetReplicaState(ctx, v.Key, v.Version, r.NodeID, model.ReplicaCorrupted)
				c.db.Event(ctx, "CORRUPTION_DETECTED", v.Key, v.Version, r.NodeID, `{}`)
				c.scheduleRepair(ctx, v.Key, v.Version, "INTEGRITY")
			}
		}
	}
}
func (c *Coordinator) verifyNode(ctx context.Context, n model.Node, key string, v int) (bool, error) {
	req, e := http.NewRequestWithContext(ctx, http.MethodPost, n.BaseURL+c.nodePath(key, v)+"/verify", nil)
	if e != nil {
		return false, e
	}
	res, e := c.nodeDo(req)
	if e != nil {
		return false, e
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return false, fmt.Errorf("%s", res.Status)
	}
	var x struct {
		Valid bool `json:"valid"`
	}
	e = json.NewDecoder(res.Body).Decode(&x)
	return x.Valid, e
}
func (c *Coordinator) reconcileTick(ctx context.Context) {
	versions, e := c.db.AllLatest(ctx)
	if e != nil {
		return
	}
	nodes, _ := c.db.Nodes(ctx)
	by := map[string]model.Node{}
	want := map[string]model.Version{}
	for _, n := range nodes {
		by[n.ID] = n
	}
	for _, v := range versions {
		want[v.Key] = v
	}
	for _, n := range nodes {
		if n.State != model.NodeOnline {
			continue
		}
		inv := c.inventory(ctx, n)
		for _, item := range inv {
			if v, ok := want[item.Key]; !ok {
				if first, err := c.db.ObserveOrphan(ctx, n.ID, item.Key, item.Version, item.SHA256); err == nil && first {
					c.db.Event(ctx, "ORPHAN_QUARANTINED", item.Key, item.Version, n.ID, `{"action":"retained"}`)
				}
				// Older immutable versions are retained legitimately and are not a
				// stale current replica. A stale current replica must claim the
				// authoritative version while disagreeing with its checksum/size.
			} else if item.Version == v.Version && (item.SHA256 != v.SHA256 || item.Size != v.Size) {
				replicas, _ := c.db.Replicas(ctx, v.Key, v.Version)
				for _, replica := range replicas {
					if replica.NodeID != n.ID || replica.State == model.ReplicaStale {
						continue
					}
					_ = c.db.SetReplicaState(ctx, v.Key, v.Version, n.ID, model.ReplicaStale)
					c.db.Event(ctx, "STALE_REPLICA_DETECTED", v.Key, v.Version, n.ID, fmt.Sprintf(`{"authoritative_version":%d,"expected_sha256":%q,"observed_sha256":%q}`, v.Version, v.SHA256, item.SHA256))
					break
				}
			}
		}
	}
	for _, v := range versions {
		rs, _ := c.db.Replicas(ctx, v.Key, v.Version)
		for _, r := range rs {
			n := by[r.NodeID]
			if n.State != model.NodeOnline {
				continue
			}
			if r.State == model.ReplicaUnreachable {
				valid, _ := c.verifyNode(ctx, n, v.Key, v.Version)
				if valid {
					_ = c.db.SetReplicaState(ctx, v.Key, v.Version, r.NodeID, model.ReplicaHealthy)
					c.db.Event(ctx, "REPLICA_RECOVERED", v.Key, v.Version, r.NodeID, `{}`)
				}
			}
			if r.State != model.ReplicaHealthy {
				continue
			}
			req, _ := http.NewRequestWithContext(ctx, http.MethodHead, n.BaseURL+c.nodePath(v.Key, v.Version), nil)
			res, e := c.nodeDo(req)
			if e != nil || res.StatusCode != 200 {
				if res != nil {
					res.Body.Close()
				}
				_ = c.db.SetReplicaState(ctx, v.Key, v.Version, r.NodeID, model.ReplicaMissing)
				c.db.Event(ctx, "REPLICA_MISSING", v.Key, v.Version, r.NodeID, `{}`)
				c.scheduleRepair(ctx, v.Key, v.Version, "RECONCILE")
			} else {
				res.Body.Close()
			}
		}
		// Re-evaluate every committed version after reconciliation. This covers
		// successful-but-degraded writes whose original target was temporarily
		// unavailable when the first repair scheduling attempt occurred.
		c.scheduleRepair(ctx, v.Key, v.Version, "RECONCILE")
		c.trimExcess(ctx, v)
	}
}
func (c *Coordinator) trimExcess(ctx context.Context, v model.Version) {
	o, _, rs, e := c.db.Latest(ctx, v.Key)
	if e != nil {
		return
	}
	nodes, _ := c.db.Nodes(ctx)
	by := map[string]model.Node{}
	for _, n := range nodes {
		by[n.ID] = n
	}
	good := []model.Replica{}
	for _, r := range rs {
		if r.State == model.ReplicaHealthy && by[r.NodeID].State == model.NodeOnline {
			good = append(good, r)
		}
	}
	if len(good) <= o.ReplicationFactor {
		return
	}
	sort.Slice(good, func(i, j int) bool { return by[good[i].NodeID].UsedBytes > by[good[j].NodeID].UsedBytes })
	victim := good[0]
	n := by[victim.NodeID]
	req, _ := http.NewRequestWithContext(ctx, http.MethodDelete, n.BaseURL+c.nodePath(v.Key, v.Version), nil)
	res, e := c.nodeDo(req)
	if e == nil && res.StatusCode == 204 {
		res.Body.Close()
		_ = c.db.DeleteReplica(ctx, v.Key, v.Version, victim.NodeID)
		c.db.Event(ctx, "EXCESS_REPLICA_REMOVED", v.Key, v.Version, victim.NodeID, `{}`)
	} else if res != nil {
		res.Body.Close()
	}
}
func (c *Coordinator) inventory(ctx context.Context, n model.Node) []model.InventoryItem {
	req, e := http.NewRequestWithContext(ctx, http.MethodGet, n.BaseURL+"/internal/inventory", nil)
	if e != nil {
		return nil
	}
	res, e := c.nodeDo(req)
	if e != nil || res.StatusCode != 200 {
		if res != nil {
			res.Body.Close()
		}
		return nil
	}
	defer res.Body.Close()
	var x []model.InventoryItem
	if json.NewDecoder(res.Body).Decode(&x) != nil {
		return nil
	}
	return x
}
func (c *Coordinator) rebalanceTick(ctx context.Context) {
	nodes, e := c.db.Nodes(ctx)
	if e != nil {
		return
	}
	var high *model.Node
	for i := range nodes {
		n := &nodes[i]
		u := float64(n.UsedBytes) / float64(max(n.CapacityBytes, 1))
		if n.State == model.NodeOnline && u > c.cfg.RebalanceHighWatermark && (high == nil || u > float64(high.UsedBytes)/float64(max(high.CapacityBytes, 1))) {
			high = n
		}
	}
	if high == nil {
		return
	}
	versions, _ := c.db.AllLatest(ctx)
	for _, v := range versions {
		rs, _ := c.db.Replicas(ctx, v.Key, v.Version)
		hosts := map[string]bool{}
		from := false
		for _, r := range rs {
			if r.State != model.ReplicaHealthy {
				continue
			}
			hosts[r.NodeID] = true
			if r.NodeID == high.ID {
				from = true
			}
		}
		if !from {
			continue
		}
		var target *model.Node
		for i := range nodes {
			n := &nodes[i]
			u := float64(n.UsedBytes) / float64(max(n.CapacityBytes, 1))
			if n.State != model.NodeOnline || hosts[n.ID] || u >= c.cfg.RebalanceLowWatermark || n.CapacityBytes-n.UsedBytes < v.Size {
				continue
			}
			if target == nil || u < float64(target.UsedBytes)/float64(max(target.CapacityBytes, 1)) {
				target = n
			}
		}
		if target != nil {
			_ = c.db.QueueRepairSource(ctx, v.Key, v.Version, target.ID, high.ID, "REBALANCE")
			c.db.Event(ctx, "REBALANCE_QUEUED", v.Key, v.Version, high.ID, fmt.Sprintf(`{"target":%q}`, target.ID))
			return
		}
	}
}
func (c *Coordinator) admin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", 405)
		return
	}
	if c.cfg.AdminToken != "" && r.Header.Get("X-Forger-Admin-Token") != c.cfg.AdminToken {
		http.Error(w, "admin authentication required", http.StatusUnauthorized)
		return
	}
	action := strings.TrimPrefix(r.URL.Path, "/v1/admin/")
	if strings.HasPrefix(action, "nodes/") {
		parts := strings.Split(action, "/")
		if len(parts) == 3 {
			id, op := parts[1], parts[2]
			nodes, _ := c.db.Nodes(r.Context())
			for _, n := range nodes {
				if n.ID == id && (op == "partition" || op == "corrupt" || op == "stale" || op == "capacity") {
					req, e := http.NewRequestWithContext(r.Context(), http.MethodPost, n.BaseURL+"/internal/faults/"+op, r.Body)
					if e == nil {
						res, e := c.nodeDo(req)
						if e == nil {
							res.Body.Close()
							w.WriteHeader(res.StatusCode)
							return
						}
					}
				}
			}
		}
		http.NotFound(w, r)
		return
	}
	switch action {
	case "scan":
		c.integrityTick(r.Context())
	case "repair":
		versions, _ := c.db.AllLatest(r.Context())
		for _, v := range versions {
			c.scheduleRepair(r.Context(), v.Key, v.Version, "MANUAL")
		}
	case "reconcile":
		c.reconcileTick(r.Context())
	case "rebalance":
		c.rebalanceTick(r.Context())
	default:
		http.NotFound(w, r)
		return
	}
	w.WriteHeader(204)
}
func (c *Coordinator) dashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	io.WriteString(w, dashboardHTML)
}
