package meta

import (
	"context"
	"database/sql"
	"fmt"
	"forger/internal/model"
	_ "modernc.org/sqlite"
	"time"
)

type Store struct{ DB *sql.DB }

func Open(path string) (*Store, error) {
	db, e := sql.Open("sqlite", path)
	if e != nil {
		return nil, e
	}
	db.SetMaxOpenConns(1)
	s := &Store{db}
	if e = s.migrate(context.Background()); e != nil {
		db.Close()
		return nil, e
	}
	return s, nil
}
func (s *Store) Close() error { return s.DB.Close() }
func (s *Store) migrate(ctx context.Context) error {
	stmts := []string{`PRAGMA journal_mode=WAL`, `PRAGMA foreign_keys=ON`, `PRAGMA busy_timeout=5000`,
		`CREATE TABLE IF NOT EXISTS nodes(id TEXT PRIMARY KEY, base_url TEXT NOT NULL UNIQUE, state TEXT NOT NULL, capacity_bytes INTEGER NOT NULL DEFAULT 0, used_bytes INTEGER NOT NULL DEFAULT 0,last_heartbeat_at TEXT, consecutive_failures INTEGER NOT NULL DEFAULT 0,created_at TEXT NOT NULL,updated_at TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS objects(object_key TEXT PRIMARY KEY,latest_version INTEGER NOT NULL,desired_replication_factor INTEGER NOT NULL,min_successful_replicas INTEGER NOT NULL,durability_policy TEXT NOT NULL,state TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS object_versions(object_key TEXT NOT NULL,version INTEGER NOT NULL,size_bytes INTEGER NOT NULL,sha256 TEXT NOT NULL,state TEXT NOT NULL,created_at TEXT NOT NULL,PRIMARY KEY(object_key,version),FOREIGN KEY(object_key) REFERENCES objects(object_key))`,
		`CREATE TABLE IF NOT EXISTS replicas(object_key TEXT NOT NULL,version INTEGER NOT NULL,node_id TEXT NOT NULL,storage_path TEXT NOT NULL,size_bytes INTEGER NOT NULL,sha256 TEXT NOT NULL,state TEXT NOT NULL,verified_at TEXT,created_at TEXT NOT NULL,updated_at TEXT NOT NULL,PRIMARY KEY(object_key,version,node_id),FOREIGN KEY(node_id) REFERENCES nodes(id))`,
		`CREATE TABLE IF NOT EXISTS repair_jobs(id INTEGER PRIMARY KEY AUTOINCREMENT,object_key TEXT NOT NULL,version INTEGER NOT NULL,target_node_id TEXT NOT NULL,source_node_id TEXT,reason TEXT NOT NULL,state TEXT NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,lease_until TEXT,last_error TEXT,created_at TEXT NOT NULL,updated_at TEXT NOT NULL)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS repair_active_unique ON repair_jobs(object_key,version,target_node_id) WHERE state IN ('QUEUED','RUNNING')`,
		`CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY AUTOINCREMENT,event_type TEXT NOT NULL,object_key TEXT,version INTEGER,node_id TEXT,details_json TEXT NOT NULL,created_at TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS orphan_observations(node_id TEXT NOT NULL,object_key TEXT NOT NULL,version INTEGER NOT NULL,sha256 TEXT NOT NULL,state TEXT NOT NULL,first_seen_at TEXT NOT NULL,last_seen_at TEXT NOT NULL,PRIMARY KEY(node_id,object_key,version,sha256))`,
		`CREATE INDEX IF NOT EXISTS replicas_lookup ON replicas(object_key,version,state)`, `CREATE INDEX IF NOT EXISTS events_at ON events(created_at DESC)`}
	for _, q := range stmts {
		if _, e := s.DB.ExecContext(ctx, q); e != nil {
			return e
		}
	}
	return nil
}
func now() string { return time.Now().UTC().Format(time.RFC3339Nano) }
func (s *Store) Event(ctx context.Context, typ, key string, v int, node, details string) {
	_, _ = s.DB.ExecContext(ctx, `INSERT INTO events(event_type,object_key,version,node_id,details_json,created_at) VALUES(?,?,?,?,?,?)`, typ, key, v, node, details, now())
}

// ObserveOrphan records discovery without generating repeated events on every scan.
// It returns true only for the first observation of the same physical orphan.
func (s *Store) ObserveOrphan(ctx context.Context, node, key string, version int, sha string) (bool, error) {
	t := now()
	res, e := s.DB.ExecContext(ctx, `INSERT INTO orphan_observations(node_id,object_key,version,sha256,state,first_seen_at,last_seen_at) VALUES(?,?,?,?,?,?,?) ON CONFLICT(node_id,object_key,version,sha256) DO NOTHING`, node, key, version, sha, "QUARANTINED", t, t)
	if e != nil {
		return false, e
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}
func (s *Store) UpsertNode(ctx context.Context, id, url string) error {
	t := now()
	_, e := s.DB.ExecContext(ctx, `INSERT INTO nodes(id,base_url,state,created_at,updated_at) VALUES(?,?,?, ?,?) ON CONFLICT(id) DO UPDATE SET base_url=excluded.base_url,updated_at=excluded.updated_at`, id, url, model.NodeOnline, t, t)
	return e
}
func (s *Store) Nodes(ctx context.Context) ([]model.Node, error) {
	rows, e := s.DB.QueryContext(ctx, `SELECT id,base_url,state,capacity_bytes,used_bytes,COALESCE(last_heartbeat_at,''),consecutive_failures FROM nodes ORDER BY id`)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []model.Node{}
	for rows.Next() {
		var n model.Node
		var ts string
		if e = rows.Scan(&n.ID, &n.BaseURL, &n.State, &n.CapacityBytes, &n.UsedBytes, &ts, &n.Failures); e != nil {
			return nil, e
		}
		if ts != "" {
			n.LastHeartbeat, _ = time.Parse(time.RFC3339Nano, ts)
		}
		out = append(out, n)
	}
	return out, rows.Err()
}
func (s *Store) RecordProbe(ctx context.Context, id string, ok bool, capacity, used int64, failLimit, offlineLimit int) (model.NodeState, error) {
	tx, e := s.DB.BeginTx(ctx, nil)
	if e != nil {
		return "", e
	}
	defer tx.Rollback()
	var f int
	var old model.NodeState
	if e = tx.QueryRowContext(ctx, `SELECT consecutive_failures,state FROM nodes WHERE id=?`, id).Scan(&f, &old); e != nil {
		return "", e
	}
	st := old
	if ok {
		if old == model.NodeUnreachable || old == model.NodeOffline {
			st = model.NodeRecovering
		} else {
			st = model.NodeOnline
		}
		f = 0
	} else {
		f++
		if f >= offlineLimit {
			st = model.NodeOffline
		} else if f >= failLimit {
			st = model.NodeUnreachable
		} else {
			st = model.NodeDegraded
		}
	}
	_, e = tx.ExecContext(ctx, `UPDATE nodes SET state=?,capacity_bytes=?,used_bytes=?,last_heartbeat_at=?,consecutive_failures=?,updated_at=? WHERE id=?`, st, capacity, used, now(), f, now(), id)
	if e != nil {
		return "", e
	}
	return st, tx.Commit()
}
func (s *Store) BeginVersion(ctx context.Context, key string, rf, min int, policy string) (int, error) {
	tx, e := s.DB.BeginTx(ctx, nil)
	if e != nil {
		return 0, e
	}
	defer tx.Rollback()
	var latest int
	e = tx.QueryRowContext(ctx, `SELECT latest_version FROM objects WHERE object_key=?`, key).Scan(&latest)
	t := now()
	if e == sql.ErrNoRows {
		latest = 0
		_, e = tx.ExecContext(ctx, `INSERT INTO objects(object_key,latest_version,desired_replication_factor,min_successful_replicas,durability_policy,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)`, key, 0, rf, min, policy, model.ObjectDegraded, t, t)
	} else if e == nil {
		_, e = tx.ExecContext(ctx, `UPDATE objects SET desired_replication_factor=?,min_successful_replicas=?,durability_policy=?,updated_at=? WHERE object_key=?`, rf, min, policy, t, key)
	}
	if e != nil {
		return 0, e
	}
	return latest + 1, tx.Commit()
}
func (s *Store) CommitVersion(ctx context.Context, key string, v int, size int64, sum string, rf, min int, policy string, reps []model.Replica) error {
	tx, e := s.DB.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	state := model.ObjectDegraded
	if len(reps) >= rf {
		state = model.ObjectHealthy
	}
	t := now()
	_, e = tx.ExecContext(ctx, `INSERT INTO object_versions(object_key,version,size_bytes,sha256,state,created_at) VALUES(?,?,?,?,?,?)`, key, v, size, sum, "COMMITTED", t)
	if e != nil {
		return e
	}
	for _, r := range reps {
		_, e = tx.ExecContext(ctx, `INSERT INTO replicas(object_key,version,node_id,storage_path,size_bytes,sha256,state,verified_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)`, key, v, r.NodeID, r.Path, r.Size, r.SHA256, model.ReplicaHealthy, t, t, t)
		if e != nil {
			return e
		}
	}
	_, e = tx.ExecContext(ctx, `UPDATE objects SET latest_version=?,desired_replication_factor=?,min_successful_replicas=?,durability_policy=?,state=?,updated_at=? WHERE object_key=? AND latest_version < ?`, v, rf, min, policy, state, t, key, v)
	if e != nil {
		return e
	}
	return tx.Commit()
}
func (s *Store) Latest(ctx context.Context, key string) (model.Object, model.Version, []model.Replica, error) {
	var o model.Object
	var ct, ut string
	e := s.DB.QueryRowContext(ctx, `SELECT object_key,latest_version,desired_replication_factor,min_successful_replicas,durability_policy,state,created_at,updated_at FROM objects WHERE object_key=?`, key).Scan(&o.Key, &o.LatestVersion, &o.ReplicationFactor, &o.MinSuccessful, &o.Policy, &o.State, &ct, &ut)
	if e != nil {
		return o, model.Version{}, nil, e
	}
	o.CreatedAt, _ = time.Parse(time.RFC3339Nano, ct)
	o.UpdatedAt, _ = time.Parse(time.RFC3339Nano, ut)
	var v model.Version
	var vt string
	e = s.DB.QueryRowContext(ctx, `SELECT object_key,version,size_bytes,sha256,state,created_at FROM object_versions WHERE object_key=? AND version=?`, key, o.LatestVersion).Scan(&v.Key, &v.Version, &v.Size, &v.SHA256, &v.State, &vt)
	if e != nil {
		return o, v, nil, e
	}
	v.CreatedAt, _ = time.Parse(time.RFC3339Nano, vt)
	rs, e := s.Replicas(ctx, key, v.Version)
	return o, v, rs, e
}
func (s *Store) Replicas(ctx context.Context, key string, v int) ([]model.Replica, error) {
	rows, e := s.DB.QueryContext(ctx, `SELECT object_key,version,node_id,storage_path,size_bytes,sha256,state,COALESCE(verified_at,'') FROM replicas WHERE object_key=? AND version=?`, key, v)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []model.Replica{}
	for rows.Next() {
		var r model.Replica
		var ts string
		if e = rows.Scan(&r.Key, &r.Version, &r.NodeID, &r.Path, &r.Size, &r.SHA256, &r.State, &ts); e != nil {
			return nil, e
		}
		if ts != "" {
			r.VerifiedAt, _ = time.Parse(time.RFC3339Nano, ts)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
func (s *Store) SetReplicaState(ctx context.Context, key string, v int, node string, state model.ReplicaState) error {
	_, e := s.DB.ExecContext(ctx, `UPDATE replicas SET state=?,updated_at=? WHERE object_key=? AND version=? AND node_id=?`, state, now(), key, v, node)
	return e
}
func (s *Store) QueueRepair(ctx context.Context, key string, v int, target, reason string) error {
	t := now()
	_, e := s.DB.ExecContext(ctx, `INSERT INTO repair_jobs(object_key,version,target_node_id,reason,state,created_at,updated_at) VALUES(?,?,?,?, 'QUEUED',?,?) ON CONFLICT DO NOTHING`, key, v, target, reason, t, t)
	return e
}
func (s *Store) QueueRepairSource(ctx context.Context, key string, v int, target, source, reason string) error {
	t := now()
	_, e := s.DB.ExecContext(ctx, `INSERT INTO repair_jobs(object_key,version,target_node_id,source_node_id,reason,state,created_at,updated_at) VALUES(?,?,?,?,?,'QUEUED',?,?) ON CONFLICT DO NOTHING`, key, v, target, source, reason, t, t)
	return e
}
func (s *Store) DeleteReplica(ctx context.Context, key string, v int, node string) error {
	_, e := s.DB.ExecContext(ctx, `DELETE FROM replicas WHERE object_key=? AND version=? AND node_id=?`, key, v, node)
	return e
}
func (s *Store) Objects(ctx context.Context) ([]model.Object, error) {
	rows, e := s.DB.QueryContext(ctx, `SELECT object_key,latest_version,desired_replication_factor,min_successful_replicas,durability_policy,state,created_at,updated_at FROM objects ORDER BY updated_at DESC`)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []model.Object{}
	for rows.Next() {
		var o model.Object
		var c, u string
		if e = rows.Scan(&o.Key, &o.LatestVersion, &o.ReplicationFactor, &o.MinSuccessful, &o.Policy, &o.State, &c, &u); e != nil {
			return nil, e
		}
		out = append(out, o)
	}
	return out, rows.Err()
}
func (s *Store) MarkObject(ctx context.Context, key string, state model.ObjectState) error {
	_, e := s.DB.ExecContext(ctx, `UPDATE objects SET state=?,updated_at=? WHERE object_key=?`, state, now(), key)
	return e
}
func (s *Store) RegisterReplica(ctx context.Context, r model.Replica) error {
	t := now()
	_, e := s.DB.ExecContext(ctx, `INSERT INTO replicas(object_key,version,node_id,storage_path,size_bytes,sha256,state,verified_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?, ?,?,?) ON CONFLICT(object_key,version,node_id) DO UPDATE SET storage_path=excluded.storage_path,size_bytes=excluded.size_bytes,sha256=excluded.sha256,state=excluded.state,verified_at=excluded.verified_at,updated_at=excluded.updated_at`, r.Key, r.Version, r.NodeID, r.Path, r.Size, r.SHA256, model.ReplicaHealthy, t, t, t)
	return e
}

type RepairJob struct {
	ID        int64  `json:"id"`
	Key       string `json:"key"`
	Version   int    `json:"version"`
	Target    string `json:"target"`
	Source    string `json:"source"`
	Reason    string `json:"reason"`
	State     string `json:"state"`
	Attempts  int    `json:"attempts"`
	LastError string `json:"error"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

func (s *Store) ClaimRepair(ctx context.Context, lease time.Duration) (RepairJob, error) {
	tx, e := s.DB.BeginTx(ctx, nil)
	if e != nil {
		return RepairJob{}, e
	}
	defer tx.Rollback()
	var j RepairJob
	var src sql.NullString
	e = tx.QueryRowContext(ctx, `SELECT id,object_key,version,target_node_id,source_node_id,reason,state,attempts FROM repair_jobs WHERE state='QUEUED' OR (state='RUNNING' AND lease_until < ?) ORDER BY created_at LIMIT 1`, now()).Scan(&j.ID, &j.Key, &j.Version, &j.Target, &src, &j.Reason, &j.State, &j.Attempts)
	if e != nil {
		return j, e
	}
	j.Source = src.String
	until := time.Now().UTC().Add(lease).Format(time.RFC3339Nano)
	res, e := tx.ExecContext(ctx, `UPDATE repair_jobs SET state='RUNNING',attempts=attempts+1,lease_until=?,updated_at=? WHERE id=? AND (state='QUEUED' OR lease_until < ?)`, until, now(), j.ID, now())
	if e != nil {
		return j, e
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return j, sql.ErrNoRows
	}
	e = tx.Commit()
	return j, e
}
func (s *Store) FinishRepair(ctx context.Context, id int64, ok bool, msg string) error {
	state := "FAILED"
	if ok {
		state = "SUCCEEDED"
	}
	_, e := s.DB.ExecContext(ctx, `UPDATE repair_jobs SET state=?,last_error=?,lease_until=NULL,updated_at=? WHERE id=?`, state, msg, now(), id)
	return e
}
func (s *Store) UpdateRepairSource(ctx context.Context, id int64, source string) error {
	_, e := s.DB.ExecContext(ctx, `UPDATE repair_jobs SET source_node_id=?,updated_at=? WHERE id=?`, source, now(), id)
	return e
}
func (s *Store) Repairs(ctx context.Context, limit int) ([]RepairJob, error) {
	rows, e := s.DB.QueryContext(ctx, `SELECT id,object_key,version,target_node_id,COALESCE(source_node_id,''),reason,state,attempts,COALESCE(last_error,''),created_at,updated_at FROM repair_jobs ORDER BY id DESC LIMIT ?`, limit)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []RepairJob{}
	for rows.Next() {
		var j RepairJob
		if e = rows.Scan(&j.ID, &j.Key, &j.Version, &j.Target, &j.Source, &j.Reason, &j.State, &j.Attempts, &j.LastError, &j.CreatedAt, &j.UpdatedAt); e != nil {
			return nil, e
		}
		out = append(out, j)
	}
	return out, rows.Err()
}
func (s *Store) AllLatest(ctx context.Context) ([]model.Version, error) {
	rows, e := s.DB.QueryContext(ctx, `SELECT v.object_key,v.version,v.size_bytes,v.sha256,v.state,v.created_at FROM object_versions v JOIN objects o ON o.object_key=v.object_key AND o.latest_version=v.version WHERE v.state='COMMITTED'`)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []model.Version{}
	for rows.Next() {
		var v model.Version
		var t string
		if e = rows.Scan(&v.Key, &v.Version, &v.Size, &v.SHA256, &v.State, &t); e != nil {
			return nil, e
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func (s *Store) Events(ctx context.Context, limit int) ([]map[string]any, error) {
	rows, e := s.DB.QueryContext(ctx, `SELECT id,event_type,COALESCE(object_key,''),COALESCE(version,0),COALESCE(node_id,''),details_json,created_at FROM events ORDER BY id DESC LIMIT ?`, limit)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id, v int
		var typ, key, node, d, t string
		if e = rows.Scan(&id, &typ, &key, &v, &node, &d, &t); e != nil {
			return nil, e
		}
		out = append(out, map[string]any{"id": id, "type": typ, "key": key, "version": v, "node": node, "details": d, "at": t})
	}
	return out, rows.Err()
}
func (s *Store) String() string { return fmt.Sprintf("sqlite") }
