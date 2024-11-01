package pbservice

import (
	"cs134-24f-kv/viewservice"
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

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// TODO: Your declarations here.
	currentView  viewservice.View
	dict         map[string]string
	seenRequests map[int64]bool
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// TODO: Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	// fmt.Println(pb.dict)
	view, _ := pb.vs.Get()
	if pb.me != view.Primary {
		reply.Err = ErrWrongServer
		return nil
	}
	if pb.me != pb.currentView.Primary {
		reply.Err = ErrWrongServer
		return nil
	}

	value, exists := pb.dict[args.Key]
	// fmt.Println(value, exists, "get")
	if !exists {
		reply.Err = ErrNoKey
	} else {
		reply.Err = OK
		reply.Value = value
	}
	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// TODO: Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.me != pb.currentView.Primary {
		reply.Err = ErrWrongServer
		return nil
	}

	// At-most-once semantics
	if pb.seenRequests[args.Id] {
		reply.Err = OK
		return nil
	}

	// Forward the operation to the backup
	if pb.currentView.Backup != "" {
		var backupReply PutAppendReply
		ok := call(pb.currentView.Backup, "PBServer.ForwardPutAppend", args, &backupReply)
		if !ok || backupReply.Err != OK {
			fmt.Printf("Failed to replicate to backup: %v\n", pb.currentView.Backup)
			// Proceed anyway
		}
	}

	// Apply the operation locally
	if args.Op == "Append" {
		value, _ := pb.dict[args.Key]
		pb.dict[args.Key] = value + args.Value
	} else if args.Op == "Put" {
		pb.dict[args.Key] = args.Value
	} else {
		reply.Err = "Unknown Operation"
		return nil
	}

	// Mark the request as processed
	pb.seenRequests[args.Id] = true
	reply.Err = OK
	return nil
}

func (pb *PBServer) ForwardPutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// TODO: Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.me != pb.currentView.Backup {
		reply.Err = ErrWrongServer
		return nil
	}

	// At-most-once semantics
	if pb.seenRequests[args.Id] {
		reply.Err = OK
		return nil
	}

	// Apply the operation
	if args.Op == "Append" {
		value, _ := pb.dict[args.Key]
		pb.dict[args.Key] = value + args.Value
	} else if args.Op == "Put" {
		pb.dict[args.Key] = args.Value
	} else {
		reply.Err = "Unknown operation"
		return nil
	}

	// Mark the request as processed
	pb.seenRequests[args.Id] = true
	reply.Err = OK
	return nil
}

func (pb *PBServer) ReceiveState(args *TransferArgs, reply *TransferReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	//backup hasn't ticked yet not up to date with the current state
	pb.dict = args.Dict
	pb.seenRequests = args.SeenRequests
	reply.Err = OK
	return nil
}

// ping the viewserver periodically.
// if view changed:
//
//	transition to new view.
//	manage transfer of state from primary to new backup.
func (pb *PBServer) tick() {
	// TODO: Your code here.
	pb.mu.Lock()
	myViewNum := pb.currentView.Viewnum
	pb.mu.Unlock()

	view, err := pb.vs.Ping(myViewNum)
	if err != nil {
		// Handle error
		return
	}

	pb.mu.Lock()
	oldBackup := pb.currentView.Backup
	pb.currentView = view
	if pb.me == pb.currentView.Primary {
		// If the backup has changed, transfer state to new backup
		if oldBackup != view.Backup && view.Backup != "" {
			go pb.TransferStateToBackup()
		}
	}
	pb.mu.Unlock()
}

func (pb *PBServer) TransferStateToBackup() {
	for {
		pb.mu.Lock()
		args := &TransferArgs{
			Id:           nrand(),
			Me:           pb.me,
			Dict:         pb.copyDict(),
			SeenRequests: pb.copySeenRequests(),
		}
		backup := pb.currentView.Backup
		pb.mu.Unlock()

		var reply TransferReply
		ok := call(backup, "PBServer.ReceiveState", args, &reply)
		if ok && reply.Err == OK {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Helper methods to safely copy maps
func (pb *PBServer) copyDict() map[string]string {
	newDict := make(map[string]string)
	for k, v := range pb.dict {
		newDict[k] = v
	}
	return newDict
}

func (pb *PBServer) copySeenRequests() map[int64]bool {
	newSeen := make(map[int64]bool)
	for k, v := range pb.seenRequests {
		newSeen[k] = v
	}
	return newSeen
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.dict = make(map[string]string)
	// TODO: Your pb.* initializations here.
	pb.seenRequests = make(map[int64]bool)
	pb.currentView = viewservice.View{Viewnum: 0}

	rpcs := rpc.NewServer()
	rpcs.Register(pb)
	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
