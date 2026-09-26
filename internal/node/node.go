package node

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"forger/internal/config"
	"forger/internal/model"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Service struct {
	cfg              config.Config
	mu               sync.RWMutex
	partition        bool
	capacityOverride int64
}
type storedMeta struct {
	Key     string `json:"key"`
	Version int    `json:"version"`
	Size    int64  `json:"size"`
	SHA256  string `json:"sha256"`
}

func New(cfg config.Config) (*Service, error) {
	for _, d := range []string{"objects", "temp", "quarantine"} {
		if err := os.MkdirAll(filepath.Join(cfg.DataDir, d), 0750); err != nil {
			return nil, err
		}
	}
	return &Service{cfg: cfg}, nil
}
func (s *Service) Handler() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("/health", s.health)
	m.HandleFunc("/internal/objects/", s.object)
	m.HandleFunc("/internal/inventory", s.inventory)
	m.HandleFunc("/internal/faults/partition", s.partitionFault)
	m.HandleFunc("/internal/faults/corrupt", s.corrupt)
	m.HandleFunc("/internal/faults/stale", s.stale)
	m.HandleFunc("/internal/faults/capacity", s.capacityFault)
	return s.gate(s.requireInternalToken(m))
}
func (s *Service) requireInternalToken(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/internal/") && s.cfg.InternalToken != "" && r.Header.Get("X-Forger-Internal-Token") != s.cfg.InternalToken {
			http.Error(w, "internal authentication required", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}
func (s *Service) gate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.RLock()
		p := s.partition
		s.mu.RUnlock()
		if p && !strings.HasPrefix(r.URL.Path, "/internal/faults/partition") && !strings.HasPrefix(r.URL.Path, "/internal/faults/capacity") {
			http.Error(w, "simulated partition", http.StatusServiceUnavailable)
			return
		}
		next.ServeHTTP(w, r)
	})
}
func (s *Service) health(w http.ResponseWriter, r *http.Request) {
	used, _ := s.used()
	s.mu.RLock()
	capacity := s.cfg.CapacityBytes
	if s.capacityOverride > 0 {
		capacity = s.capacityOverride
	}
	s.mu.RUnlock()
	json.NewEncoder(w).Encode(map[string]any{"id": s.cfg.NodeID, "capacity_bytes": capacity, "used_bytes": used, "at": time.Now().UTC()})
}
func (s *Service) object(w http.ResponseWriter, r *http.Request) {
	key, ver, err := parseObjectPath(r.URL.Path)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	switch r.Method {
	case http.MethodPut:
		s.put(w, r, key, ver)
	case http.MethodGet:
		s.get(w, r, key, ver, false)
	case http.MethodHead:
		s.get(w, r, key, ver, true)
	case http.MethodDelete:
		s.delete(w, key, ver)
	case http.MethodPost:
		if strings.HasSuffix(r.URL.Path, "/verify") {
			s.verify(w, key, ver)
		} else {
			http.NotFound(w, r)
		}
	default:
		http.Error(w, "method", 405)
	}
}
func parseObjectPath(p string) (string, int, error) {
	p = strings.TrimPrefix(p, "/internal/objects/")
	p = strings.TrimSuffix(p, "/verify")
	a := strings.Split(strings.Trim(p, "/"), "/")
	if len(a) != 2 {
		return "", 0, errors.New("invalid object path")
	}
	keyb, e := base64.RawURLEncoding.DecodeString(a[0])
	if e != nil {
		return "", 0, e
	}
	v, e := strconv.Atoi(a[1])
	if e != nil || v < 1 {
		return "", 0, errors.New("invalid version")
	}
	return string(keyb), v, nil
}
func objectID(key string) string { return base64.RawURLEncoding.EncodeToString([]byte(key)) }
func (s *Service) paths(key string, v int) (string, string) {
	d := filepath.Join(s.cfg.DataDir, "objects", objectID(key))
	return filepath.Join(d, fmt.Sprintf("%d.data", v)), filepath.Join(d, fmt.Sprintf("%d.json", v))
}
func (s *Service) put(w http.ResponseWriter, r *http.Request, key string, v int) {
	expected := r.Header.Get("X-Forger-SHA256")
	if len(expected) != 64 {
		http.Error(w, "checksum required", 400)
		return
	}
	data, meta := s.paths(key, v)
	if err := os.MkdirAll(filepath.Dir(data), 0750); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	tmp, err := os.CreateTemp(filepath.Join(s.cfg.DataDir, "temp"), "upload-")
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, h), r.Body)
	cerr := tmp.Close()
	if err != nil || cerr != nil {
		http.Error(w, "write failed", 502)
		return
	}
	sum := hex.EncodeToString(h.Sum(nil))
	if sum != expected {
		http.Error(w, "checksum mismatch", 422)
		return
	}
	if err = os.Rename(tmpName, data); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	b, _ := json.Marshal(storedMeta{key, v, n, sum})
	if err = os.WriteFile(meta, b, 0640); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.Header().Set("X-Forger-SHA256", sum)
	w.Header().Set("X-Forger-Size", strconv.FormatInt(n, 10))
	w.WriteHeader(http.StatusCreated)
}
func (s *Service) get(w http.ResponseWriter, r *http.Request, key string, v int, head bool) {
	data, metaPath := s.paths(key, v)
	b, err := os.ReadFile(metaPath)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	var m storedMeta
	if json.Unmarshal(b, &m) != nil {
		http.Error(w, "bad metadata", 500)
		return
	}
	info, err := os.Stat(data)
	if err != nil || info.IsDir() {
		http.NotFound(w, r)
		return
	}
	if info.Size() != m.Size {
		http.Error(w, "stored size differs from metadata", http.StatusConflict)
		return
	}
	w.Header().Set("X-Forger-SHA256", m.SHA256)
	w.Header().Set("X-Forger-Size", strconv.FormatInt(m.Size, 10))
	if head {
		return
	}
	f, err := os.Open(data)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	defer f.Close()
	io.Copy(w, f)
}
func (s *Service) delete(w http.ResponseWriter, key string, v int) {
	d, m := s.paths(key, v)
	_ = os.Remove(d)
	_ = os.Remove(m)
	w.WriteHeader(http.StatusNoContent)
}
func (s *Service) verify(w http.ResponseWriter, key string, v int) {
	d, m := s.paths(key, v)
	b, err := os.ReadFile(m)
	if err != nil {
		http.Error(w, "not found", 404)
		return
	}
	var x storedMeta
	if json.Unmarshal(b, &x) != nil {
		http.Error(w, "bad metadata", 500)
		return
	}
	f, err := os.Open(d)
	if err != nil {
		http.Error(w, "not found", 404)
		return
	}
	defer f.Close()
	h := sha256.New()
	n, _ := io.Copy(h, f)
	sum := hex.EncodeToString(h.Sum(nil))
	json.NewEncoder(w).Encode(map[string]any{"valid": sum == x.SHA256 && n == x.Size, "sha256": sum, "size": n, "expected": x.SHA256})
}
func (s *Service) inventory(w http.ResponseWriter, r *http.Request) {
	items := []model.InventoryItem{}
	root := filepath.Join(s.cfg.DataDir, "objects")
	entries, _ := os.ReadDir(root)
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		ms, _ := filepath.Glob(filepath.Join(root, e.Name(), "*.json"))
		for _, p := range ms {
			b, er := os.ReadFile(p)
			if er != nil {
				continue
			}
			var m storedMeta
			if json.Unmarshal(b, &m) == nil {
				items = append(items, model.InventoryItem{Key: m.Key, Version: m.Version, Size: m.Size, SHA256: m.SHA256})
			}
		}
	}
	json.NewEncoder(w).Encode(items)
}
func (s *Service) partitionFault(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "method", 405)
		return
	}
	var x struct {
		Enabled bool `json:"enabled"`
	}
	if json.NewDecoder(r.Body).Decode(&x) != nil {
		http.Error(w, "json", 400)
		return
	}
	s.mu.Lock()
	s.partition = x.Enabled
	s.mu.Unlock()
	json.NewEncoder(w).Encode(x)
}

// capacityFault is a development-only capacity-reporting fixture. It changes
// the capacity reported to the coordinator; object IO and checksums remain real.
func (s *Service) capacityFault(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", 405)
		return
	}
	var x struct {
		CapacityBytes int64 `json:"capacity_bytes"`
	}
	if json.NewDecoder(r.Body).Decode(&x) != nil || x.CapacityBytes < 0 {
		http.Error(w, "json", 400)
		return
	}
	s.mu.Lock()
	s.capacityOverride = x.CapacityBytes
	s.mu.Unlock()
	json.NewEncoder(w).Encode(x)
}
func (s *Service) corrupt(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "method", 405)
		return
	}
	var x struct {
		Key     string `json:"key"`
		Version int    `json:"version"`
	}
	if json.NewDecoder(r.Body).Decode(&x) != nil {
		http.Error(w, "json", 400)
		return
	}
	p, _ := s.paths(x.Key, x.Version)
	f, e := os.OpenFile(p, os.O_RDWR, 0)
	if e != nil {
		http.NotFound(w, r)
		return
	}
	defer f.Close()
	buf := []byte{0}
	if _, e = f.ReadAt(buf, 0); e != nil {
		http.Error(w, e.Error(), 500)
		return
	}
	buf[0] ^= 0xff
	_, e = f.WriteAt(buf, 0)
	if e != nil {
		http.Error(w, e.Error(), 500)
		return
	}
	w.WriteHeader(204)
}
func (s *Service) stale(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "method", 405)
		return
	}
	var x struct {
		Key         string `json:"key"`
		FromVersion int    `json:"from_version"`
		ToVersion   int    `json:"to_version"`
	}
	if json.NewDecoder(r.Body).Decode(&x) != nil {
		http.Error(w, "json", 400)
		return
	}
	src, srcM := s.paths(x.Key, x.FromVersion)
	dst, dstM := s.paths(x.Key, x.ToVersion)
	b, e := os.ReadFile(src)
	if e != nil {
		http.NotFound(w, r)
		return
	}
	mb, e := os.ReadFile(srcM)
	if e != nil {
		http.NotFound(w, r)
		return
	}
	var m storedMeta
	if json.Unmarshal(mb, &m) != nil {
		http.Error(w, "metadata", 500)
		return
	}
	m.Version = x.ToVersion
	out, _ := json.Marshal(m)
	if e = os.WriteFile(dst, b, 0640); e == nil {
		e = os.WriteFile(dstM, out, 0640)
	}
	if e != nil {
		http.Error(w, e.Error(), 500)
		return
	}
	w.WriteHeader(204)
}
func (s *Service) used() (int64, error) {
	var total int64
	err := filepath.Walk(filepath.Join(s.cfg.DataDir, "objects"), func(_ string, i os.FileInfo, e error) error {
		if e == nil && !i.IsDir() {
			total += i.Size()
		}
		return nil
	})
	return total, err
}
