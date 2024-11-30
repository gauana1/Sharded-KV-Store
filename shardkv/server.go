package shardkv

import (
	"cs134-24f-kv/paxos"
	"cs134-24f-kv/shardmaster"
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
	// Your definitions here.
	OpType string
	Key    string //for put appends
	Value  string //for gets
	ReqId  int64
	Shard  int
	Err    Err
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// TODO: Your definitions here.
	current_config 	shardmaster.Config
	seq 		int
	db 			map[int]map[string]string

	
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	println("NO LOCK")
	// TODO: Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	println(args.Shard)
	for _, GID :=  range kv.current_config.Shards {
		print(GID, " ")
	}
	println("END", kv.gid)
	op := Op{
		OpType: "Get",
		Key : args.Key,
		Shard : args.Shard,
		ReqId: nrand(),
	}
	kv.seq ++ 
	for {
		kv.px.Start(kv.seq, op)
		decidedOp := kv.waitAndCheck(kv.seq)
		if decidedOp.ReqId == op.ReqId{
			kv.px.Done(kv.seq)
			break
		} else{
			kv.ApplyOp(decidedOp)
		}
		kv.seq ++ 
	}
	println(kv.current_config.Shards[op.Shard], "GID CORES")
	if kv.current_config.Shards[op.Shard] != kv.gid{
		reply.Err = ErrWrongGroup
		return nil
	}
	dict , exists := kv.db[args.Shard] ; if exists{
		//right shard, apply put
		value , e := dict[args.Key]; if e{
			reply.Value = value
			reply.Err = OK
		} else{
			reply.Err = ErrNoKey
		}
	} else{
		reply.Err = ErrWrongGroup
	}
	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// TODO: Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{
		OpType: args.Op,
		Key : args.Key,
		Value : args.Value,
		Shard :args.Shard,
		ReqId: nrand(),
	}
	kv.seq ++ 
	for {
		kv.px.Start(kv.seq, op)
		decidedOp := kv.waitAndCheck(kv.seq)
		if decidedOp.ReqId == op.ReqId{
			println("YO", op.ReqId, op.OpType, op.Key, op.Value)
			kv.px.Done(kv.seq)
			break
		} else{
			kv.ApplyOp(decidedOp)
		}
		kv.seq ++ 
	}
	if kv.current_config.Shards[op.Shard] != kv.gid{
		println("ERR")
		reply.Err = ErrWrongGroup
		return nil
	}
	_, ex := kv.db[args.Shard]; if !ex{
		kv.db[args.Shard] = make(map[string]string)
	}
	if op.OpType == "Put"{
		kv.db[args.Shard][args.Key] = args.Value
		reply.Err = OK
	} else if op.OpType == "Append"{
		_, exists := kv.db[args.Shard][args.Key]; if exists{
			kv.db[args.Shard][args.Key] += args.Value
			reply.Err = OK
		} else {
			kv.db[args.Shard][args.Key] = args.Value
			reply.Err = OK
		}
	}
	return nil
}

// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
func (kv *ShardKV) tick() { // i think you can block when you tick, since then when calling other get appends you don't have to catch up as much 
	kv.mu.Lock()
	defer kv.mu.Unlock()
	latest_config := kv.sm.Query(-1)
	if latest_config.Num <= kv.current_config.Num{
		return 
	}
	op := Op{
		OpType: "Change",
		ReqId: nrand(),
	}
	kv.seq ++ 
	for {
		println("FUCK")
		kv.px.Start(kv.seq, op)
		decidedOp := kv.waitAndCheck(kv.seq)
		println(op.ReqId, decidedOp.ReqId, "IDS iN TCK")
		if decidedOp.ReqId == op.ReqId{
			kv.px.Done(kv.seq)
			break
		} else{
			kv.ApplyOp(decidedOp)
		}
		kv.seq ++ 
	}
	kv.ApplyOp(op)
}

func(kv *ShardKV) Transfer(args *TransferArgs, reply *TransferReply) error {
	//first we have to transfer state 
	kv.db[args.Shard] = args.Dict //update the dict
	kv.px.Done(args.Seq) //call done
	reply.Err = OK
	return nil
}

func(kv *ShardKV) ApplyOp(op Op){
	if op.OpType == "Get"{
		//do nothing on gets
	} else if op.OpType == "Put"{
		//still check if right shard, else can ignore the put
		_ , exists := kv.db[op.Shard] ; if exists{ 
			//right shard, apply put
			kv.db[op.Shard][op.Key] = op.Value
		}
	} else if op.OpType == "Append"{
		_ , exists := kv.db[op.Shard] ; if exists{
			//right shard, apply put
			_ , exists := kv.db[op.Shard][op.Key]; if exists{
				kv.db[op.Shard][op.Key] += op.Value
			} else{
				kv.db[op.Shard][op.Key] = op.Value
			}
		}
	} else if op.OpType == "Change"{
		latest_config := kv.sm.Query(-1)
		if latest_config.Num > kv.current_config.Num{
			old_config := kv.current_config
			//now do a diff between old and new config and find out if this GID got 
			//kicked out 
			kv.current_config = latest_config
			for shard, GID := range old_config.Shards{
				if GID == kv.gid && latest_config.Shards[shard] != kv.gid{
					//have to transfer state
					new_gid :=latest_config.Shards[shard]  
					args := &TransferArgs{
						Shard:shard, 
						Dict : kv.db[shard], 
						Seq : kv.seq, 
					}
					var reply TransferReply
					for _, server := range latest_config.Groups[new_gid]{
						for {
							ok := call(server, "ShardKV.Transfer", args, &reply)
							if ok && reply.Err == OK{
								break
							}
							time.Sleep(100 * time.Millisecond)
						}

					}
				}
			}
			//have to do some rpc stuff here
		}//else do nothing
	}
}

func (kv *ShardKV) waitAndCheck(seq int) Op {
	timeout := 10 * time.Millisecond
	for {
		status, val := kv.px.Status(seq)
		if status == paxos.Decided {
			return val.(Op)
		}
		time.Sleep(timeout)
		if timeout < 10*time.Second {
			timeout *= 2
		}
	}
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//
//	servers that implement the shardmaster.
//
// servers[] contains the ports of the servers
//
//	in this replica group.
//
// Me is the index of this server in servers[].
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// TODO: Your initialization code here.
	kv.seq = 0 
	kv.db = make(map[int]map[string]string)
	// Don't call Join().

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
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
