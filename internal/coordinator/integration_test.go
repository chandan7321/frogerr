package coordinator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"forger/internal/config"
	"forger/internal/meta"
	"forger/internal/model"
	"forger/internal/node"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func sha256Hex(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

type harness struct {
	c      *Coordinator
	db     *meta.Store
	dbPath string
	h      http.Handler
	nodes  map[string]*httptest.Server
	dirs   map[string]string
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	nodes := map[string]*httptest.Server{}
	dirs := map[string]string{}
	urls := map[string]string{}
	for _, id := range []string{"n1", "n2", "n3", "n4"} {
		d := t.TempDir()
		s, e := node.New(config.Config{Mode: "node", NodeID: id, DataDir: d, CapacityBytes: 1 << 30})
		if e != nil {
			t.Fatal(e)
		}
		ts := httptest.NewServer(s.Handler())
		nodes[id] = ts
		dirs[id] = d
		urls[id] = ts.URL
	}
	dbPath := filepath.Join(t.TempDir(), "forger.db")
	db, e := meta.Open(dbPath)
	if e != nil {
		t.Fatal(e)
	}
	cfg := config.Config{DataDir: t.TempDir(), NodeURLs: urls, DefaultRF: 3, DefaultMinSuccess: 2, RequestTimeout: time.Second, HeartbeatInterval: time.Hour, IntegrityInterval: time.Hour, ReconcileInterval: time.Hour, RebalanceInterval: time.Hour, HeartbeatFailures: 2, OfflineFailures: 4, RebalanceHighWatermark: .8, RebalanceLowWatermark: .5}
	c := New(cfg, db)
	if e = c.Start(context.Background()); e != nil {
		t.Fatal(e)
	}
	c.healthTick(context.Background())
	t.Cleanup(func() {
		c.Stop()
		db.Close()
		for _, s := range nodes {
			s.Close()
		}
	})
	return &harness{c, db, dbPath, c.Handler(), nodes, dirs}
}
func (x *harness) put(t *testing.T, key string, b []byte, q string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodPut, "/v1/objects/"+key+q, bytes.NewReader(b))
	w := httptest.NewRecorder()
	x.h.ServeHTTP(w, r)
	return w
}
func (x *harness) get(t *testing.T, key string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, "/v1/objects/"+key, nil)
	w := httptest.NewRecorder()
	x.h.ServeHTTP(w, r)
	return w
}
func waitFor(t *testing.T, timeout time.Duration, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for asynchronous operation")
}

func TestPutGetReplication(t *testing.T) {
	x := newHarness(t)
	body := bytes.Repeat([]byte("forger"), 8192)
	if w := x.put(t, "alpha", body, "?policy=DURABLE"); w.Code != 200 {
		t.Fatalf("put=%d %s", w.Code, w.Body.String())
	}
	_, v, rs, e := x.db.Latest(context.Background(), "alpha")
	if e != nil || v.Version != 1 || len(rs) != 3 {
		t.Fatalf("metadata version=%v replicas=%d err=%v", v.Version, len(rs), e)
	}
	w := x.get(t, "alpha")
	if w.Code != 200 || !bytes.Equal(w.Body.Bytes(), body) {
		t.Fatalf("get failed: %d", w.Code)
	}
}

func TestConfigurableRFAndPartialWriteDurability(t *testing.T) {
	x := newHarness(t)
	req := httptest.NewRequest(http.MethodPost, "/internal/faults/partition", bytes.NewBufferString(`{"enabled":true}`))
	x.nodes["n3"].Config.Handler.ServeHTTP(httptest.NewRecorder(), req)
	w := x.put(t, "partial", []byte("safe"), "?rf=3&min_success=2")
	if w.Code != 200 {
		t.Fatalf("allowed partial write=%d %s", w.Code, w.Body.String())
	}
	_, _, rs, e := x.db.Latest(context.Background(), "partial")
	if e != nil || len(rs) != 2 {
		t.Fatalf("only verified replicas should commit: %d %v", len(rs), e)
	}
	w = x.put(t, "strict", []byte("safe"), "?rf=3&min_success=3")
	if w.Code == 200 {
		t.Fatal("strict write unexpectedly committed")
	}
}

func TestConcurrentSameKeyWritesAreImmutable(t *testing.T) {
	x := newHarness(t)
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			w := x.put(t, "same", []byte{byte(i)}, "?policy=DURABLE")
			if w.Code != 200 {
				t.Errorf("put %d: %d", i, w.Code)
			}
		}(i)
	}
	wg.Wait()
	_, v, _, e := x.db.Latest(context.Background(), "same")
	if e != nil || v.Version != 6 {
		t.Fatalf("latest version=%d err=%v", v.Version, e)
	}
}

func TestCorruptionReadFailoverAndRepair(t *testing.T) {
	x := newHarness(t)
	if w := x.put(t, "heal", []byte("correct-bytes"), "?policy=DURABLE"); w.Code != 200 {
		t.Fatal(w.Code)
	}
	req := httptest.NewRequest(http.MethodPost, "/internal/faults/corrupt", bytes.NewBufferString(`{"key":"heal","version":1}`))
	x.nodes["n1"].Config.Handler.ServeHTTP(httptest.NewRecorder(), req)
	w := x.get(t, "heal")
	if w.Code != 200 || w.Body.String() != "correct-bytes" {
		t.Fatalf("failover failed %d %q", w.Code, w.Body.String())
	}
	x.c.repairTick(context.Background())
	_, _, rs, e := x.db.Latest(context.Background(), "heal")
	if e != nil {
		t.Fatal(e)
	}
	healthy := 0
	for _, r := range rs {
		if r.State == "HEALTHY" {
			healthy++
		}
	}
	if healthy < 3 {
		t.Fatalf("repair did not restore RF: %d", healthy)
	}
}

func TestMetadataStorageReconciliation(t *testing.T) {
	x := newHarness(t)
	if w := x.put(t, "missing", []byte("x"), "?policy=DURABLE"); w.Code != 200 {
		t.Fatal(w.Code)
	}
	p := filepath.Join(x.dirs["n1"], "objects", base64.RawURLEncoding.EncodeToString([]byte("missing")), "1.data")
	if e := os.Remove(p); e != nil {
		t.Fatal(e)
	}
	x.c.reconcileTick(context.Background())
	_, _, rs, e := x.db.Latest(context.Background(), "missing")
	if e != nil {
		t.Fatal(e)
	}
	found := false
	for _, r := range rs {
		if r.NodeID == "n1" && r.State == "MISSING" {
			found = true
		}
	}
	if !found {
		t.Fatal("missing local file not reflected in metadata")
	}
}

func TestPartitionRecoveryAndRepairJobDeduplication(t *testing.T) {
	x := newHarness(t)
	req := httptest.NewRequest(http.MethodPost, "/internal/faults/partition", bytes.NewBufferString(`{"enabled":true}`))
	x.nodes["n2"].Config.Handler.ServeHTTP(httptest.NewRecorder(), req)
	x.c.healthTick(context.Background())
	x.c.healthTick(context.Background())
	nodes, _ := x.db.Nodes(context.Background())
	for _, n := range nodes {
		if n.ID == "n2" && n.State != "UNREACHABLE" {
			t.Fatalf("partition state=%s", n.State)
		}
	}
	if e := x.db.QueueRepair(context.Background(), "a", 1, "n4", "TEST"); e != nil {
		t.Fatal(e)
	}
	if e := x.db.QueueRepair(context.Background(), "a", 1, "n4", "TEST"); e != nil {
		t.Fatal(e)
	}
	var count int
	if e := x.db.DB.QueryRow(`SELECT count(*) FROM repair_jobs WHERE object_key='a' AND target_node_id='n4' AND state IN ('QUEUED','RUNNING')`).Scan(&count); e != nil || count != 1 {
		t.Fatalf("active job count=%d err=%v", count, e)
	}
	req = httptest.NewRequest(http.MethodPost, "/internal/faults/partition", bytes.NewBufferString(`{"enabled":false}`))
	x.nodes["n2"].Config.Handler.ServeHTTP(httptest.NewRecorder(), req)
	x.c.healthTick(context.Background())
	nodes, _ = x.db.Nodes(context.Background())
	for _, n := range nodes {
		if n.ID == "n2" && n.State != "RECOVERING" {
			t.Fatalf("expected recovery, got %s", n.State)
		}
	}
}

func TestSafeRebalanceMove(t *testing.T) {
	x := newHarness(t)
	if w := x.put(t, "move", bytes.Repeat([]byte("z"), 64), "?policy=DURABLE"); w.Code != 200 {
		t.Fatal(w.Code)
	}
	// Report n1 at full capacity through the same health path used in Docker;
	// this is a deterministic fixture, not a metadata-only imbalance.
	req := httptest.NewRequest(http.MethodPost, "/internal/faults/capacity", bytes.NewBufferString(`{"capacity_bytes":1}`))
	w := httptest.NewRecorder()
	x.nodes["n1"].Config.Handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("capacity fixture=%d", w.Code)
	}
	x.c.healthTick(context.Background())
	x.c.rebalanceTick(context.Background())
	x.c.repairTick(context.Background())
	waitFor(t, 3*time.Second, func() bool {
		_, _, rs, e := x.db.Latest(context.Background(), "move")
		if e != nil {
			return false
		}
		n1, n4 := false, false
		for _, r := range rs {
			n1 = n1 || r.NodeID == "n1"
			n4 = n4 || r.NodeID == "n4"
		}
		return !n1 && n4
	})
	_, _, rs, e := x.db.Latest(context.Background(), "move")
	if e != nil {
		t.Fatal(e)
	}
	n1, n4 := false, false
	for _, r := range rs {
		n1 = n1 || r.NodeID == "n1"
		n4 = n4 || r.NodeID == "n4"
	}
	if n1 || !n4 {
		t.Fatalf("unsafe/incomplete move n1=%t n4=%t", n1, n4)
	}
	if got := x.get(t, "move"); got.Code != http.StatusOK || !bytes.Equal(got.Body.Bytes(), bytes.Repeat([]byte("z"), 64)) {
		t.Fatalf("rebalanced replica does not serve verified bytes: %d", got.Code)
	}
}

func TestStaleReplicaDetectionReadAvoidanceAndRepair(t *testing.T) {
	x := newHarness(t)
	if w := x.put(t, "versioned", []byte("version-one"), "?policy=DURABLE"); w.Code != http.StatusOK {
		t.Fatal(w.Code)
	}
	if w := x.put(t, "versioned", []byte("version-two"), "?policy=DURABLE"); w.Code != http.StatusOK {
		t.Fatal(w.Code)
	}
	req := httptest.NewRequest(http.MethodPost, "/internal/faults/stale", bytes.NewBufferString(`{"key":"versioned","from_version":1,"to_version":2}`))
	w := httptest.NewRecorder()
	x.nodes["n3"].Config.Handler.ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("stale fault=%d", w.Code)
	}
	x.c.reconcileTick(context.Background())
	_, v, rs, err := x.db.Latest(context.Background(), "versioned")
	if err != nil || v.Version != 2 {
		t.Fatalf("latest=%d err=%v", v.Version, err)
	}
	for _, r := range rs {
		if r.NodeID == "n3" && r.State != "STALE" {
			t.Fatalf("n3=%s, want STALE", r.State)
		}
	}
	if got := x.get(t, "versioned"); got.Code != http.StatusOK || got.Body.String() != "version-two" {
		t.Fatalf("GET served stale data: %d %q", got.Code, got.Body.String())
	}
	x.c.repairTick(context.Background())
	_, _, rs, err = x.db.Latest(context.Background(), "versioned")
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rs {
		if r.NodeID == "n3" && r.State != "HEALTHY" {
			t.Fatalf("n3 was not repaired: %s", r.State)
		}
	}
	if got := x.get(t, "versioned"); got.Code != http.StatusOK || got.Body.String() != "version-two" {
		t.Fatalf("repaired object invalid: %d %q", got.Code, got.Body.String())
	}
}

func TestRepairJobAPIRecordsActualSource(t *testing.T) {
	x := newHarness(t)
	if w := x.put(t, "observable", []byte("correct"), "?policy=DURABLE"); w.Code != http.StatusOK {
		t.Fatal(w.Code)
	}
	req := httptest.NewRequest(http.MethodPost, "/internal/faults/corrupt", bytes.NewBufferString(`{"key":"observable","version":1}`))
	x.nodes["n1"].Config.Handler.ServeHTTP(httptest.NewRecorder(), req)
	x.c.integrityTick(context.Background())
	x.c.repairTick(context.Background())
	w := httptest.NewRecorder()
	x.h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/repairs", nil))
	if w.Code != http.StatusOK || !bytes.Contains(w.Body.Bytes(), []byte(`"source":"n`)) {
		t.Fatalf("repair API lacks actual source: %d %s", w.Code, w.Body.String())
	}
	events, err := x.db.Events(context.Background(), 50)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event["type"] == "REPAIR_SUCCEEDED" && event["key"] == "observable" {
			if !bytes.Contains([]byte(event["details"].(string)), []byte(`"source":"n`)) {
				t.Fatalf("repair event lacks source: %v", event)
			}
			return
		}
	}
	t.Fatal("missing repair success event")
}

func TestOrphanEventIsDeduplicated(t *testing.T) {
	x := newHarness(t)
	path := filepath.Join(t.TempDir(), "orphan")
	data := []byte("orphan-data")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	nodes, _ := x.db.Nodes(context.Background())
	var n1 model.Node
	for _, n := range nodes {
		if n.ID == "n1" {
			n1 = n
		}
	}
	if err := x.c.nodePut(context.Background(), n1, "orphan", 1, path, sha256Hex(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	x.c.reconcileTick(context.Background())
	x.c.reconcileTick(context.Background())
	events, err := x.db.Events(context.Background(), 100)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range events {
		if event["type"] == "ORPHAN_QUARANTINED" && event["key"] == "orphan" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("orphan events=%d, want 1", count)
	}
}

func TestCoordinatorRestartAndPersistentNodeData(t *testing.T) {
	x := newHarness(t)
	if w := x.put(t, "restart", []byte("persists"), "?policy=DURABLE"); w.Code != 200 {
		t.Fatal(w.Code)
	}
	x.c.Stop()
	if e := x.db.Close(); e != nil {
		t.Fatal(e)
	}
	db, e := meta.Open(x.dbPath)
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	_, v, rs, e := db.Latest(context.Background(), "restart")
	if e != nil || v.Version != 1 || len(rs) != 3 {
		t.Fatalf("metadata did not persist: %v", e)
	}
	// Confirm durable node-side storage remains accessible after its HTTP handler lifecycle is replaced.
	f, e := os.Open(filepath.Join(x.dirs["n1"], "objects", base64.RawURLEncoding.EncodeToString([]byte("restart")), "1.data"))
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	b, _ := io.ReadAll(f)
	if string(b) != "persists" {
		t.Fatal("node data did not persist")
	}
}
