package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// TODO: You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op string // "Put" or "Append"
	Id int64  // Unique request ID
	Me string // Clerk's unique identifier
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// TODO: You'll have to add definitions here.
	Id int64  // Unique request ID
	Me string // Clerk's unique identifier
}

type GetReply struct {
	Err   Err
	Value string
}

// TODO: Your RPC definitions here.
