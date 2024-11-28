package shardmaster

import (
	"cs134-24f-kv/paxos"
	"encoding/gob"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs []Config // indexed by config num
	seq     int
}

type Op struct {
	// TODO: Your data here.
	/*
		Op name
	*/
	OpType  string
	GID     int64
	Servers []string
	Shard   int
	Num     int
	ReqId   int64
}

func Balance(new_shards *[10]int64, gid_map map[int64]int) {
	//first find the max assigned GID within old_shards
	//do an initial purge of the newshards based on what is not in gid_map
	for i, GID := range new_shards{
		_ , exists := gid_map[GID] 
		if !exists{
			new_shards[i] = 0
		}
	}
	for _, GID := range new_shards {
		print(GID, " ")
	}
	println("BEFORE")
	for {
		//rest values
		for GID := range gid_map{
			gid_map[GID] = 0 
		}
		var ct bool = false
		for _, GID := range new_shards {
			if GID != 0 {
				_, exists := gid_map[GID]
				if !exists {
					gid_map[GID] = 0 //if it doesn't exist it got removed
				} else {
					gid_map[GID] += 1
				}
			} else {
				ct = true
			}

		}
		//if we have some missing values, max_ct = inf and max_GID = -1
		var max_ct int = 0
		var max_GID int64 = 0
		var min_ct int = 100
		var min_GID int64 = 0
		if ct { //if stuff is empty set max and reassigned min
			max_ct = math.MaxInt64
			max_GID = 0
		} 
		for GID, count := range gid_map {
			if count > max_ct {
				max_ct = count
				max_GID = GID
			}
			if count < min_ct {
				min_ct = count
				min_GID = GID
			}
		}
		//now we can do the replacement
		var replace_shard int = 0
		for shard, GID := range new_shards {
			if GID == max_GID {
				//we can replace
				replace_shard = shard
				break
			}
		}
		// if max_GID != 0 {
		// 	gid_map[max_GID] -= 1
		// }
		// gid_map[min_GID] += 1

		if max_ct - min_ct > 1{
			new_shards[replace_shard] = min_GID
		}
		var min_counter int = gid_map[min_GID]
		var temp bool = false
		var total int = 0
		for _, count := range gid_map {
			if (min_counter-count) <= 1 && (min_counter-count) >= -1 {
				total += count
				continue
			} else {
				temp = true
			}
		}
		if !temp && total == NShards {
			//valid set
			break
		}
		
	}
	for _, GID := range new_shards {
		print(GID, " ")
	}
	println("AFTER")
}

func CopyMap[K comparable, V any](original map[K]V) map[K]V {
	copied := make(map[K]V)
	for key, value := range original {
		copied[key] = value
	}

	return copied
}

func (sm *ShardMaster) ApplyOp(op Op) {
	n := len(sm.configs)
	new_group := CopyMap(sm.configs[n-1].Groups)
	old_shards := sm.configs[n-1].Shards
	if op.OpType == "Join" {
		new_group[op.GID] = op.Servers
		println(len(new_group), "JOIN", sm.me, op.GID)
		for GID, _ := range new_group {
			print(GID, " ")
		}
		println("GROUP")
		gid_map := make(map[int64]int)
		for GID, _ := range new_group {
			gid_map[GID] = 0
		}
		Balance(&old_shards, gid_map) //modifies old_shards
		new_config := Config{Num: n, Groups: new_group, Shards: old_shards}
		sm.configs = append(sm.configs, new_config)
	} else if op.OpType == "Leave" {
		delete(new_group, op.GID)
		gid_map := make(map[int64]int)
		for GID, _ := range new_group {
			gid_map[GID] = 0
		}
		println("GROUP")
		println(len(new_group), "LEAVE", sm.me, op.GID)
		for GID, _ := range new_group {
			print(GID, " ")
		}
		println()
		Balance(&old_shards, gid_map) //modifies the old_shards
		new_config := Config{Num: n, Groups: new_group, Shards: old_shards}
		sm.configs = append(sm.configs, new_config)
	} else if op.OpType == "Move" {
		println("MOVEING", op.GID, op.Shard, sm.me)
		old_shards[op.Shard] = op.GID
		new_config := Config{Num: n, Groups: new_group, Shards: old_shards}
		sm.configs = append(sm.configs, new_config)
	} else {
		//do nothing for queries
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// TODO: Your code here.
	//shardmaster has to create a new config
	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := Op{
		OpType:  "Join",
		GID:     args.GID,
		Servers: args.Servers,
		Shard:   -1,
		Num:     -1,
		ReqId:   nrand(),
	}
	for {
		sm.px.Start(sm.seq, op)
		decidedOp := sm.waitAndCheck(sm.seq)
		if decidedOp.GID == args.GID && decidedOp.ReqId == op.ReqId {
			sm.px.Done(sm.seq)
			sm.seq++
			break
		} else {
			sm.ApplyOp(decidedOp)
		}
		sm.seq++
	}
	sm.ApplyOp(op)
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// TODO: Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := Op{
		OpType:  "Leave",
		GID:     args.GID,
		Servers: nil,
		Shard:   -1,
		Num:     -1,
		ReqId:   nrand(),
	}
	for {
		sm.px.Start(sm.seq, op)
		decidedOp := sm.waitAndCheck(sm.seq)
		if decidedOp.GID == args.GID && decidedOp.ReqId == op.ReqId {
			sm.px.Done(sm.seq)
			sm.seq++
			break
		} else {
			sm.ApplyOp(decidedOp)
		}
		sm.seq++
	}
	sm.ApplyOp(op)
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// TODO: Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := Op{
		OpType:  "Move",
		GID:     args.GID,
		Servers: nil,
		Shard:   args.Shard,
		Num:     -1,
		ReqId:   nrand(),
	}
	for {
		sm.px.Start(sm.seq, op)
		decidedOp := sm.waitAndCheck(sm.seq)
		if decidedOp.GID == args.GID && decidedOp.ReqId == op.ReqId {
			sm.px.Done(sm.seq)
			sm.seq++
			break
		} else {
			sm.ApplyOp(decidedOp)
		}
		sm.seq++
	}
	sm.ApplyOp(op)
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// TODO: Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	//have to catch up
	op := Op{
		OpType:  "Query",
		GID:     -1,
		Servers: nil,
		Shard:   -1,
		Num:     args.Num,
		ReqId:   nrand(),
	}
	for {
		sm.px.Start(sm.seq, op)
		decidedOp := sm.waitAndCheck(sm.seq)
		if decidedOp.ReqId == op.ReqId {
			sm.px.Done(sm.seq)
			sm.seq++
			break
		} else {
			sm.ApplyOp(decidedOp)
		}
		sm.seq++
	}
	if args.Num >= len(sm.configs) || args.Num == -1 {
		reply.Config = sm.configs[len(sm.configs)-1]
	} else {
		reply.Config = sm.configs[args.Num]
	}
	return nil
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}
func (sm *ShardMaster) waitAndCheck(seq int) Op {
	timeout := 10 * time.Millisecond
	for {
		status, val := sm.px.Status(seq)
		if status == paxos.Decided {
			return val.(Op)
		}
		time.Sleep(timeout)
		if timeout < 10*time.Second {
			timeout *= 2
		}
	}
}
func nrand() int64 {
	return rand.Int63()
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me
	sm.seq = 0
	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
