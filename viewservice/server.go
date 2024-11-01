package viewservice

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // For testing
	rpccount int32 // For testing
	me       string

	time_map      map[string]time.Time // Ping states of nodes
	view          View                 // Current view state
	primary_acked bool                 // Track if the primary has acknowledged
}

func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	vs.time_map[args.Me] = time.Now()
	if args.Me == vs.view.Primary && args.Viewnum == vs.view.Viewnum {
		vs.primary_acked = true
	}
	if vs.view.Viewnum == 0 { //can update primary to be args.me
		vs.view.Primary = args.Me
		vs.view.Viewnum += 1
		vs.primary_acked = false
	} else if args.Viewnum == 0 {
		//primary, backup, or other nodes can send this, means this node has restarted
		if args.Me == vs.view.Primary && vs.primary_acked {
			if vs.view.Backup != "" {
				vs.view.Primary, vs.view.Backup = vs.view.Backup, ""
				vs.view.Viewnum++
				//check for external
				for key, value := range vs.time_map {
					if key != vs.view.Primary && key != args.Me {
						timeSinceLastPing := time.Now().Sub(value)
						intervals := timeSinceLastPing / PingInterval
						if intervals <= DeadPings {
							vs.view.Backup = key
							break
						}
					}
				}
				vs.primary_acked = false
			} else if vs.view.Backup == "" {
				//check for external to replace
				for key, value := range vs.time_map {
					if key != vs.view.Primary && key != args.Me {
						timeSinceLastPing := time.Now().Sub(value)
						intervals := timeSinceLastPing / PingInterval
						if intervals <= DeadPings {
							vs.view.Backup = key
							vs.view.Viewnum += 1
							vs.primary_acked = false
							break
						}
					}
				}
			}
		} else if args.Me == vs.view.Backup && vs.primary_acked { //backup failed
			vs.view.Backup = ""
			//check for external
			for key, value := range vs.time_map {
				if key != vs.view.Primary && key != args.Me {
					timeSinceLastPing := time.Now().Sub(value)
					intervals := timeSinceLastPing / PingInterval
					if intervals <= DeadPings {
						vs.view.Backup = key
						break
					}
				}
			}
			vs.view.Viewnum += 1
			vs.primary_acked = false
		}
	}
	reply.View = vs.view
	return nil
}
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	reply.View = vs.view
	return nil
}

func (vs *ViewServer) replace() {

}

func (vs *ViewServer) tick() {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// Check if the primary is alive
	primary := vs.view.Primary
	if primary != "" {
		lastPingTime, exists := vs.time_map[primary]
		if !exists || time.Since(lastPingTime) >= DeadPings*PingInterval {
			// Primary is dead
			if vs.primary_acked {
				// Promote backup to primary
				vs.view.Primary = vs.view.Backup
				vs.view.Backup = ""
				vs.view.Viewnum++
				vs.primary_acked = false
			}
			// Do not promote if primary hasn't acknowledged
		}
	}

	// Check if backup is alive
	backup := vs.view.Backup
	if backup != "" {
		lastPingTime, exists := vs.time_map[backup]
		if !exists || time.Since(lastPingTime) >= DeadPings*PingInterval {
			// Backup is dead
			vs.view.Backup = ""
			vs.view.Viewnum++
			vs.primary_acked = false
		}
	}

	// Assign new backup if needed and possible
	if vs.view.Backup == "" && vs.primary_acked {
		for server, lastPingTime := range vs.time_map {
			if server != vs.view.Primary && time.Since(lastPingTime) < DeadPings*PingInterval {
				vs.view.Backup = server
				vs.view.Viewnum++
				vs.primary_acked = false
				break
			}
		}
	}
}

func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	vs.view.Viewnum = 0
	vs.primary_acked = false
	vs.time_map = make(map[string]time.Time)

	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	os.Remove(vs.me) // Only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	go func() {
		for !vs.isdead() {
			conn, err := vs.l.Accept()
			if err == nil && !vs.isdead() {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			} else if !vs.isdead() {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	go func() {
		for !vs.isdead() {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
