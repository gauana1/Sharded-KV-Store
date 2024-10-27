package viewservice

import (
	// "crypto/x509"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	// "slices"
	"sync"
	"sync/atomic"
	"time"
)
type NodeState struct {
    time int  // The time since the last ping
    Restarted    bool // Indicates if the node has restarted
}
type State struct{
	state_number int // in order to keep track of the current version
	primary string
	backup string
}
type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string


	// TODO: Your declarations here.
	
	//a ping from other servers
	time_map map[string] NodeState
	view State
	primary_acked bool //keep track if the primary acked, reset if a new primary is going to be elected

}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	// TODO: Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()
	if (args.Viewnum == 0){
		//this means this particular server just restarted
		vs.time_map[args.Me] = NodeState{0, true}
		vs.primary_acked = true
	} else{
		vs.time_map[args.Me] = NodeState{0, false}
	}
	if (args.Viewnum != uint(vs.view.state_number)){     // if kv-server's viewnum is not the same as vs's viewnum,
		reply.View.Viewnum = uint(vs.view.state_number)  // then update the kv's viewnum
	} else {
		reply.View.Viewnum = args.Viewnum 				 // else, keep the viewnum the same
	}
	reply.View = View{uint(vs.view.state_number), vs.view.backup, vs.view.primary}
	fmt.Println(reply.View)
	return nil
}

//
// server Get() RPC handler.	
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// TODO: Your code here.

	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func(vs *ViewServer) replace(primary string, backup string){
	for key, value := range vs.time_map{
		if key != primary && key != backup{
			if value.time <= DeadPings{
				if vs.view.primary == ""{
					vs.view.primary = key
					vs.primary_acked = false
				} else if vs.view.backup == ""{
					vs.view.backup = key
					vs.primary_acked = false
				}
			}
		} 
	}
}
func (vs *ViewServer) tick() {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	fmt.Println(vs.time_map)
	for key , value:= range vs.time_map{
		vs.time_map[key] = NodeState{value.time +1, value.Restarted}
	}
	fmt.Println(vs.time_map, vs.primary_acked)
	if vs.primary_acked {
		//if primary acked then we can change the view if needed
		//if we make changes to the view then we set primary acked as false i think
		//where do we do the assignment of the new primary if the primary crashes, i think in here
		primary := vs.view.primary
		backup := vs.view.backup
		if (vs.time_map[primary].time >= DeadPings && vs.time_map[backup].time>= DeadPings) || (primary == "" && backup == ""){
			//increment view, NOT TOO SURE
			vs.view.primary = ""
			vs.view.backup = ""
			vs.view.state_number += 1
			//call func to replace
			vs.replace(primary, backup)
		} else if vs.time_map[primary].Restarted || vs.time_map[backup].Restarted{
			vs.view.state_number += 1
			if vs.time_map[primary].Restarted && !vs.time_map[backup].Restarted{
				vs.view.primary = backup
				vs.view.backup = ""
				//call func to replace backup
				vs.replace(primary, backup)
			} else if vs.time_map[backup].Restarted{
				vs.view.backup = ""
				//call func to replace backup
				vs.replace(primary, backup)
			}
		} 
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	vs.view.state_number = 0
	vs.primary_acked = true
	vs.time_map = make(map[string]NodeState)
	// TODO: Your vs.* initializations here.

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
