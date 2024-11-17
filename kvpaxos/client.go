package kvpaxos

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"time"
	"net/rpc"
)

type Clerk struct {
	servers []string
	// TODO: You will have to modify this struct.
	me 			int64
	RequestID 	int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// TODO: You'll have to add code here.
	ck.me = nrand()
	ck.RequestID = 0
	return ck
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
func (ck *Clerk) Get(key string) string {
	// TODO: You will have to modify this function.
	ck.RequestID++
	args := &GetArgs{
		Key: key,
		ClientID: ck.me, 
		RequestID: ck.RequestID,
	}
	var reply GetReply
	for {
		for _, server:= range ck.servers{
			ok := call(server, "KVPaxos.Get", args, &reply)
			if ok{
				if reply.Err == OK{
					return reply.Value
				} else if reply.Err == ErrNoKey{
					return ""
				} else {
					//some other error
				}
			} 
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// shared by Put and Append.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.RequestID++
	args := &PutAppendArgs{
		Key: key,
		Op: op,
		Value: value,
		ClientID: ck.me, 
		RequestID: ck.RequestID,
	}
	var reply PutAppendReply
	for {
		for _, server:= range ck.servers{
			ok := call(server, "KVPaxos.PutAppend", args, &reply)
			if ok{
				if reply.Err == OK{
					return 
				} else {
					//some other error
				}
			} 
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
