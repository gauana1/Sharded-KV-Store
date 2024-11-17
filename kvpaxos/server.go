package kvpaxos

import (
	"cs134-24f-kv/paxos"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// TODO: Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType    string
	Key       string
	Value     string
	ClientID  int64
	RequestID int64
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// TODO: Your definitions here.
	db   map[string]string
	seq  int // next paxos seq num to use
	last int // last paxos instance applied to state
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, exists := kv.db[args.Key]
	if exists {
		reply.Value = value
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
	}
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{
		OpType:    args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}
	seq := kv.seq
	kv.seq++
	for {
		kv.px.Start(seq, op)
		decidedOp := kv.waitAndCheck(seq)
		
		//Dupe handling Logic
		if decidedOp.ClientID == args.ClientID && decidedOp.RequestID == args.RequestID {
			reply.Err = OK
			kv.px.Done(seq)
			break
		} else {
			seq = kv.seq
			kv.seq++
		}
	}
	if args.Op == "Append" {
		value, exists := kv.db[args.Key]
		if exists {
			kv.db[args.Key] = value + args.Value
		} else {
			kv.db[args.Key] = value + args.Value
		}
	} else {
		kv.db[args.Key] = args.Value
	}
	return nil
}

func (kv *KVPaxos) waitAndCheck(seq int) Op {
	timeout := 10 * time.Millisecond
	for {
		status, val := kv.px.Status(seq)
		if status == paxos.Decided {
			return val.(Op)
		}
		time.Sleep(timeout)
		if timeout < 10*time.Second{
			timeout *= 2
		}
	}
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// TODO: Your initialization code here.
	kv.db = make(map[string]string)
	kv.seq = 0
	kv.last = -1
	
	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
