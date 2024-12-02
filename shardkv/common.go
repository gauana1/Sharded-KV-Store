package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// TODO: You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
)

type Err string

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// TODO: You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Shard    int
	ClientID int64
	PrevID   int64
}

type TransferArgs struct {
	Shard int
	Dict  map[string]string
	Seq   int
}
type TransferReply struct {
	Err Err
}
type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// TODO: You'll have to add definitions here.
	Shard    int //have to know what shard to send this to
	ClientID int64
	PrevID   int64
}

type GetReply struct {
	Err   Err
	Value string
}
