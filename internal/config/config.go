package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Mode                    string
	ListenAddr              string
	NodeID                  string
	DataDir                 string
	DatabasePath            string
	NodeURLs                map[string]string
	DefaultRF               int
	DefaultMinSuccess       int
	HeartbeatInterval       time.Duration
	HeartbeatFailures       int
	OfflineFailures         int
	RepairGrace             time.Duration
	IntegrityInterval       time.Duration
	ReconcileInterval       time.Duration
	RebalanceInterval       time.Duration
	RequestTimeout          time.Duration
	CapacityBytes           int64
	RebalanceHighWatermark  float64
	RebalanceLowWatermark   float64
}

func Load() (Config, error) {
	c := Config{
		Mode: env("MODE", "coordinator"), ListenAddr: env("LISTEN_ADDR", ":8080"), NodeID: env("NODE_ID", ""),
		DataDir: env("DATA_DIR", "/data"), DatabasePath: env("DATABASE_PATH", "/data/forger.db"),
		DefaultRF: envInt("DEFAULT_RF", 3), DefaultMinSuccess: envInt("DEFAULT_MIN_SUCCESS", 2),
		HeartbeatInterval: envDuration("HEARTBEAT_INTERVAL", 2*time.Second), HeartbeatFailures: envInt("HEARTBEAT_FAILURES", 2),
		OfflineFailures: envInt("OFFLINE_FAILURES", 5), RepairGrace: envDuration("REPAIR_GRACE", 8*time.Second),
		IntegrityInterval: envDuration("INTEGRITY_INTERVAL", 20*time.Second), ReconcileInterval: envDuration("RECONCILE_INTERVAL", 25*time.Second),
		RebalanceInterval: envDuration("REBALANCE_INTERVAL", 30*time.Second), RequestTimeout: envDuration("REQUEST_TIMEOUT", 8*time.Second),
		CapacityBytes: envInt64("CAPACITY_BYTES", 1073741824), RebalanceHighWatermark: envFloat("REBALANCE_HIGH", .80), RebalanceLowWatermark: envFloat("REBALANCE_LOW", .55),
		NodeURLs: parseNodes(os.Getenv("NODE_URLS")),
	}
	if c.Mode != "coordinator" && c.Mode != "node" { return c, fmt.Errorf("MODE must be coordinator or node") }
	if c.Mode == "node" && c.NodeID == "" { return c, fmt.Errorf("NODE_ID is required for node mode") }
	if c.DefaultRF < 1 || c.DefaultMinSuccess < 1 || c.DefaultMinSuccess > c.DefaultRF { return c, fmt.Errorf("invalid durability defaults") }
	return c, nil
}

func env(k, d string) string { if v := os.Getenv(k); v != "" { return v }; return d }
func envInt(k string, d int) int { v, e := strconv.Atoi(env(k, strconv.Itoa(d))); if e != nil { return d }; return v }
func envInt64(k string, d int64) int64 { v, e := strconv.ParseInt(env(k, strconv.FormatInt(d,10)), 10, 64); if e != nil { return d }; return v }
func envFloat(k string, d float64) float64 { v,e:=strconv.ParseFloat(env(k,fmt.Sprintf("%f",d)),64);if e!=nil{return d};return v }
func envDuration(k string, d time.Duration) time.Duration { v,e:=time.ParseDuration(env(k,d.String()));if e!=nil{return d};return v }
func parseNodes(raw string) map[string]string { out:=map[string]string{}; for _, p:=range strings.Split(raw,",") { if p=="" {continue}; x:=strings.SplitN(p,"=",2); if len(x)==2 {out[x[0]]=strings.TrimRight(x[1],"/")} }; return out }
