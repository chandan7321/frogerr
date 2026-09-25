package model

import "time"

type NodeState string
const ( NodeOnline NodeState="ONLINE"; NodeDegraded NodeState="DEGRADED"; NodeUnreachable NodeState="UNREACHABLE"; NodeOffline NodeState="OFFLINE"; NodeRecovering NodeState="RECOVERING" )
type ReplicaState string
const ( ReplicaPending ReplicaState="PENDING"; ReplicaHealthy ReplicaState="HEALTHY"; ReplicaStale ReplicaState="STALE"; ReplicaMissing ReplicaState="MISSING"; ReplicaCorrupted ReplicaState="CORRUPTED"; ReplicaUnreachable ReplicaState="UNREACHABLE" )
type ObjectState string
const ( ObjectHealthy ObjectState="HEALTHY"; ObjectDegraded ObjectState="DEGRADED"; ObjectUnavailable ObjectState="UNAVAILABLE" )
type Node struct { ID, BaseURL string; State NodeState; CapacityBytes, UsedBytes int64; LastHeartbeat time.Time; Failures int }
type Object struct { Key string; LatestVersion, ReplicationFactor, MinSuccessful int; Policy string; State ObjectState; CreatedAt, UpdatedAt time.Time }
type Version struct { Key string; Version int; Size int64; SHA256 string; State string; CreatedAt time.Time }
type Replica struct { Key string; Version int; NodeID, Path, SHA256 string; Size int64; State ReplicaState; VerifiedAt time.Time }
type InventoryItem struct { Key string `json:"key"`; Version int `json:"version"`; Size int64 `json:"size"`; SHA256 string `json:"sha256"` }
