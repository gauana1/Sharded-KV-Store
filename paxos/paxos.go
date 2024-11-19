package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
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

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	instances   map[int]*PaxosInstance
	instancesMu sync.Mutex
	doneValues  []int
	doneMu      sync.Mutex
}

// holds state for each Paxos instance via sequence number
type PaxosInstance struct {
	mu      sync.Mutex
	Decided bool
	Value   interface{}
	N_p     int         // Highest prepare seen.
	N_a     int         // Highest accept seen.
	V_a     interface{} // Value associated with N_a.
}

// args for Prepare RPC.
type PrepareArgs struct {
	Seq  int
	N    int
	Done int
	Me   int
	Val  interface{}
}

// reply for Prepare RPC.
type PrepareReply struct {
	Seq  int
	N    int
	OK   bool
	N_a  int
	V_a  interface{}
	Done int
	Me   int
}

// argument for Accept RPC.
type AcceptArgs struct {
	Seq  int
	N    int
	V    interface{}
	Done int
	Me   int
}

// reply for Accept RPC.
type AcceptReply struct {
	Seq  int
	N    int
	OK   bool
	Done int
	Me   int
}

// argument for Decided RPC.
type DecidedArgs struct {
	Seq  int
	V    interface{}
	Done int
	Me   int
}

// reply for Decided RPC.
type DecidedReply struct {
	Done int
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()
	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}
	fmt.Println(err)
	return false
}

// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
func (px *Paxos) Start(seq int, v interface{}) {
	if seq < px.Min() {
		return
	}
	px.instancesMu.Lock()
	if _, ok := px.instances[seq]; !ok {
		px.instances[seq] = &PaxosInstance{
			N_p:     0,
			N_a:     0,
			Decided: false,
			Value:   nil,
			V_a:     v,
		}
	} 
	px.instancesMu.Unlock()
	time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
	go px.proposer(seq, v)
}

// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
func (px *Paxos) Done(seq int) {
	// TODO: Your code here.
	// px.doneMu.Lock()
	// defer px.doneMu.Unlock()
	// if seq > px.doneValues[px.me] {
	// 	px.doneValues[px.me] = seq
	// }
	px.updateDoneValues(px.me, seq)
}

// the application wants to know the
// highest instance sequence known to
// this peer.
func (px *Paxos) Max() int {
	// Your code here.
	px.instancesMu.Lock()
	defer px.instancesMu.Unlock()
	max := -1
	for seq := range px.instances {
		if seq > max {
			max = seq
		}
	}
	return max
}

// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
func (px *Paxos) Min() int {
	// TODO: You code here.
	px.doneMu.Lock()
	defer px.doneMu.Unlock()

	// if len(px.doneValues) == 0 {
	// 	// Return a default value or handle the error appropriately
	// 	return 0
	// }

	min := px.doneValues[px.me]
	for _, doneSeq := range px.doneValues {
		if doneSeq < min {
			min = doneSeq
		}
	}
	px.instancesMu.Lock()
	defer px.instancesMu.Unlock()
	for seq := range px.instances {
		if seq < min {
			delete(px.instances, seq)
		}
	}
	return min + 1
}

// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// TODO: Your code here.
	minSeq := px.Min()
	if seq < minSeq {
		return Forgotten, nil
	}

	px.instancesMu.Lock()
	defer px.instancesMu.Unlock()
	instance, exists := px.instances[seq]
	if !exists {
		return Pending, nil
	}

	instance.mu.Lock()
	defer instance.mu.Unlock()
	if instance.Decided {
		return Decided, instance.Value
	}
	return Pending, nil
}

// proposer runs the proposer logic for a Paxos instance.
func (px *Paxos) proposer(seq int, v interface{}) {
	// Initialize proposal number.
	n := rand.Intn(1000000)
	for !px.isdead() { //TODO: figure out later why
		minSeq := px.Min()
		if seq < minSeq {
			return
		}

		// Prepare Phase
		prepareOKs := 0
		var v_prime interface{} = v
		maxN_a := -1

		for _, peer := range px.peers {
			args := PrepareArgs{
				Seq:  seq,
				N:    n,
				Done: px.doneValues[px.me],
				Me:   px.me,
				Val:  v_prime,
			}
			var reply PrepareReply
			var ok bool
			var err error
			if peer == px.peers[px.me] {
				err = px.Prepare(&args, &reply)
				ok = (err == nil) //no need to send rpc to self
			} else {
				ok = call(peer, "Paxos.Prepare", &args, &reply) //send rpc to peer
			}
			if ok {
				px.updateDoneValues(reply.Me, reply.Done)
				if reply.OK {
					prepareOKs++
					if reply.N_a > maxN_a { 
						maxN_a = reply.N_a
						v_prime = reply.V_a
					}
				} else {
					if reply.N > n {
						n = reply.N 
					}
				}
			} else {
				// Retry later if RPC failed.
			}
		}		
		if prepareOKs <= len(px.peers)/2 { //dont' get majority
			n += 1 //increment sequence number
			time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
			continue
		}
		// Accept Phase
		acceptOKs := 0
		for _, peer := range px.peers {
			args := AcceptArgs{
				Seq:  seq,
				N:    n,
				V:    v_prime,
				Done: px.doneValues[px.me],
				Me:   px.me,
			}
			var reply AcceptReply
			var ok bool
			var err error
			if peer == px.peers[px.me] {
				err = px.Accept(&args, &reply)
				ok = (err == nil)
			} else {
				ok = call(peer, "Paxos.Accept", &args, &reply)
			}
			if ok {
				px.updateDoneValues(reply.Me, reply.Done)
				if reply.OK {
					acceptOKs++
				} else {
					if reply.N > n {
						n = reply.N
					}
				}
			} else {
				// Retry later if RPC failed.
			}

		}
		if acceptOKs <= len(px.peers)/2 {
			n += 1
			time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
			continue
		}
		// Decide Phase
		for _, peer := range px.peers {
			args := DecidedArgs{
				Seq:  seq,
				V:    v_prime,
				Done: px.doneValues[px.me],
				Me:   px.me,
			}
			var reply DecidedReply
			// var err error
			if peer == px.peers[px.me] {
				// err = px.Decided(&args, &reply)
				px.Decided(	&args, &reply)
			} else {
				call(peer, "Paxos.Decided", &args, &reply)
			}
		}
		break
	}
}

// Prepare handles the Prepare RPC from proposers.
func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
	px.instancesMu.Lock()
	if _, exists := px.instances[args.Seq]; !exists {
		px.instances[args.Seq] = &PaxosInstance{Decided: false, Value:nil, V_a:args.Val}
	}
	instance := px.instances[args.Seq]
	px.instancesMu.Unlock()

	instance.mu.Lock()
	defer instance.mu.Unlock()

	reply.Seq = args.Seq
	reply.N = args.N
	reply.Me = px.me
	if args.N > instance.N_p {
		instance.N_p = args.N
		reply.OK = true
		reply.N_a = instance.N_a
		reply.V_a = instance.V_a
	} else {
		reply.OK = false
		reply.N = instance.N_p
	}

	px.updateDoneValues(args.Me, args.Done)
	reply.Done = px.doneValues[px.me]
	return nil
}

// Accept handles the Accept RPC from proposers.
func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	px.instancesMu.Lock()
	if _, exists := px.instances[args.Seq]; !exists {
		px.instances[args.Seq] = &PaxosInstance{}
	}
	instance := px.instances[args.Seq]
	px.instancesMu.Unlock()

	instance.mu.Lock()
	defer instance.mu.Unlock()

	reply.Seq = args.Seq
	reply.N = args.N
	reply.Me = px.me

	if args.N >= instance.N_p {
		instance.N_p = args.N
		instance.N_a = args.N
		instance.V_a = args.V
		reply.OK = true
	} else {
		reply.OK = false
		reply.N = instance.N_p
	}

	px.updateDoneValues(args.Me, args.Done)
	reply.Done = px.doneValues[px.me]
	return nil
}

// Decided handles the Decided RPC from proposers.
func (px *Paxos) Decided(args *DecidedArgs, reply *DecidedReply) error {
	px.instancesMu.Lock()
	if _, exists := px.instances[args.Seq]; !exists {
		px.instances[args.Seq] = &PaxosInstance{}
	}
	instance := px.instances[args.Seq]
	px.instancesMu.Unlock()

	instance.mu.Lock()
	instance.Decided = true
	instance.Value = args.V
	instance.mu.Unlock()

	px.updateDoneValues(args.Me, args.Done)
	reply.Done = px.doneValues[px.me]
	return nil
}

// updateDoneValues updates the doneValues slice with the latest done value from a peer.
func (px *Paxos) updateDoneValues(peer int, done int) {
	px.doneMu.Lock()
	if peer >= 0 && peer < len(px.doneValues) {
		if done > px.doneValues[peer] {
			px.doneValues[peer] = done
		}
	}
	px.doneMu.Unlock()
}

// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

// has this peer been asked to shut down?
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// TODO: Your initialization code here.
	px.instances = make(map[int]*PaxosInstance)
	px.doneValues = make([]int, len(peers))
	for i := range px.doneValues {
		px.doneValues[i] = -1
	}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
