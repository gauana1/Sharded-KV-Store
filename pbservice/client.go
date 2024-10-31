package pbservice

import (
	"crypto/rand"
	"cs134-24f-kv/viewservice"
	"fmt"
	"math/big"
	"net/rpc"
	"time"
)

type Clerk struct {
	vs *viewservice.Clerk
	// TODO: Your declarations here
	me      string
	primary string
	seqNum  int64
}

// this may come in handy.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// TODO: Your ck.* initializations here
	ck.me = me
	ck.seqNum = 0
	view, _ := ck.vs.Get()
	ck.primary = view.Primary

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

// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
func (ck *Clerk) Get(key string) string {
	ck.seqNum++
	args := &GetArgs{
		Key: key,
		Id:  ck.seqNum,
		Me:  ck.me,
	}
	for {
		if ck.primary == "" {
			view, _ := ck.vs.Get()
			ck.primary = view.Primary
		}

		var reply GetReply
		ok := call(ck.primary, "PBServer.Get", args, &reply)

		if ok {
			if reply.Err == OK {
				fmt.Println(reply.Value, "FD")
				return reply.Value
			} else if reply.Err == ErrNoKey {
				return ""
			} else if reply.Err == ErrWrongServer {
				// The server is not the primary; get the latest primary from viewservice
				view, _ := ck.vs.Get()
				ck.primary = view.Primary
			} else {
				// Some other error; retry
			}
		} else {
			// RPC failed; get latest view and retry
			view, _ := ck.vs.Get()
			ck.primary = view.Primary
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// send a Put or Append RPC
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.seqNum++
	args := &PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Id:    nrand(),
		Me:    ck.me,
	}
	for {
		if ck.primary == "" {
			view, _ := ck.vs.Get()
			ck.primary = view.Primary
		}
		var reply PutAppendReply
		ok := call(ck.primary, "PBServer.PutAppend", args, &reply)
		if ok {
			if reply.Err == OK {
				return
			} else if reply.Err == ErrWrongServer {
				// The server is not the primary; get the latest primary from viewservice
				view, _ := ck.vs.Get()
				ck.primary = view.Primary
			} else {
				// implement some other error; retry
			}
		} else {
			// RPC failed; get latest view and retry
			view, _ := ck.vs.Get()
			ck.primary = view.Primary
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// tell the primary to update key's value.
// must keep trying until it succeeds.
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// tell the primary to append to key's value.
// must keep trying until it succeeds.
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
