package pbservice

import "time"

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

type TransferArgs struct{
	Id 				int64
	Me 				string
	Dict 			map[string]string //thing actually sent over
	SeenRequests 	map[int64] bool
}
type TransferReply struct{
	Err Err
}
// TODO: Your RPC definitions here.
func (pb *PBServer) TransferState(){
	//always transfer from current primary to current backup
	args := &TransferArgs{
		Id : 1, 
		Me: pb.me, 
		Dict: pb.dict,
	}	
	for {
		var reply TransferReply
		ok:= call(pb.currentView.Backup, "PBServer.Transfer", args, &reply)
		if ok{
			if reply.Err == OK{
				return 
			} 
		}
		time.Sleep(100*time.Millisecond)
	}
}
func (pb *PBServer) Transfer(args *TransferArgs, reply *TransferReply){
	pb.dict = args.Dict
	reply.Err = OK
}