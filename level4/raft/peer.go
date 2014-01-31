package raft

import (
	"encoding/json"
	"net/http"
	"net/http/httputil"
	"stripe-ctf.com/sqlcluster/sql"
	"stripe-ctf.com/sqlcluster/transport"
	"sync"
	"time"
    "log"
)

func delay() bool {
	time.Sleep(50 * time.Millisecond)
	return false
}

func (p *Peer) enterLoop(con *httputil.ClientConn) {
    c, rw := con.Hijack()
    reader := json.NewDecoder(rw)
    writer := json.NewEncoder(c)

    log.Printf("################################################################### Entering peer loop for peer: %s\n", p.Name)

    for {
/*        switch {
            
        case <-p.stopForward:
            log.Printf("Peer %s received a stop request\n", p.Name)
            o := make(chan bool)
            go func(){
                p.stopForward <- true
                o <- true
            }()
            <-o
            return
        default:
*/            
            execCmd := &ExecuteCommand{}
            execCmdReply := &ExecuteCommandReply{}
            err := reader.Decode(execCmd)
            if err != nil {
                log.Printf("##############  Peer %s Decoding incoming message failed: %v\n", p.Name, err)
                return
            } else {
                log.Printf("############ Peer %s Decoding incoming message success: %v\n", p.Name, execCmd)
                iface, err := p.server.Do(execCmd)
                log.Printf("############# Peer %s Executed command. Result: %v, Error: %v\n", p.Name, iface, err)
                execCmdReply.Error = err
                out, ok := iface.(*sql.Output)
                if ok {
                    execCmdReply.Output = *out
                }
            }

            err = writer.Encode(execCmdReply)
            if err != nil {
                return
            }
//        }
    }
}

func (p *Peer) doConnect() bool {
        netcon, err := transport.UnixDialer("unix", p.ConnectionString)
        if err != nil {
            log.Printf("################ - peer.connect: Connection failure - %s / %s - %s", p.Name, p.ConnectionString, err.Error())
            return delay()
        }
        log.Printf("################ - peer.connect: Connection established %s / %s", p.Name, p.ConnectionString)

        client := httputil.NewClientConn(netcon, nil)
        req, err := http.NewRequest("GET", "/forward", nil)
        if err != nil {
            log.Printf("################ - peer.connect: Creating new GET request failed %s / %s - %v", p.Name, p.ConnectionString, err)
            return delay()
        }

        req.Header.Set("Content-Type", "plain/text")
        req.Header.Set("Connection", "Keep-Alive")

        defer client.Close()
        defer netcon.Close()

        log.Printf("################ - peer.connect: Performing GET request %s / %s - %v", p.Name, p.ConnectionString, req)
        resp, err := client.Do(req)
        if err != nil {
            log.Printf("################ - peer.connect: GET /forward failed %s / %s - %s", p.Name, p.ConnectionString, err.Error())
            return delay()
        }
        log.Printf("################ - peer.connect: Get Result %s / %s", p.Name, p.ConnectionString)

        defer resp.Body.Close()

        p.enterLoop(client)
        return false
}

func (p *Peer) connect() {
    return
	log.Printf("################ - peer.connect: %s / %s", p.Name, p.ConnectionString)
	for {
		if p.doConnect() {
			return
		}
	}
}

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// A peer is a reference to another server involved in the consensus protocol.
type Peer struct {
	server            *server
	Name              string `json:"name"`
	ConnectionString  string `json:"connectionString"`
	prevLogIndex      uint64
	mutex             sync.RWMutex
	stopChan          chan bool
	heartbeatInterval time.Duration
	stopForward       chan bool
    proceed           chan bool
}

//------------------------------------------------------------------------------
//
// Constructor
//
//------------------------------------------------------------------------------

// Creates a new peer.
func newPeer(server *server, name string, connectionString string, heartbeatInterval time.Duration) *Peer {
	return &Peer{
		server:            server,
		Name:              name,
		ConnectionString:  connectionString,
		heartbeatInterval: heartbeatInterval,
        stopForward:       make(chan bool, 1),
        proceed:           make(chan bool, 1),
	}
}

//------------------------------------------------------------------------------
//
// Accessors
//
//------------------------------------------------------------------------------

// Sets the heartbeat timeout.
func (p *Peer) setHeartbeatInterval(duration time.Duration) {
	p.heartbeatInterval = duration
}

//--------------------------------------
// Prev log index
//--------------------------------------

// Retrieves the previous log index.
func (p *Peer) getPrevLogIndex() uint64 {
	return p.prevLogIndex
}

// Sets the previous log index.
func (p *Peer) setPrevLogIndex(value uint64) {
	p.prevLogIndex = value
}

//------------------------------------------------------------------------------
//
// Methods
//
//------------------------------------------------------------------------------

//--------------------------------------
// Copying
//--------------------------------------

func (p *Peer) clone() *Peer {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return &Peer{
		Name:             p.Name,
		ConnectionString: p.ConnectionString,
		prevLogIndex:     p.prevLogIndex,
	}
}

func (p *Peer) flush(entries []*LogEntry, prevTerm, term, prevIndex, commitIndex uint64) {
    start := time.Now()
	if entries != nil {
         p.sendAppendEntriesRequest(newAppendEntriesRequest(term, prevIndex, prevTerm, p.server.log.CommitIndex(), p.server.name, entries))
	} else {
		p.sendSnapshotRequest(newSnapshotRequest(p.server.name, p.server.lastSnapshot))
	}

    duration := time.Now().Sub(start)
    p.server.DispatchEvent(newEvent(HeartbeatEventType, duration, nil))
}

//--------------------------------------
// Append Entries
//--------------------------------------

// Sends an AppendEntries request to the peer through the transport.
func (p *Peer) sendAppendEntriesRequest(req *AppendEntriesRequest) {
	tracef("peer.append.send: %s->%s [prevLog:%v length: %v]\n", p.server.Name(), p.Name, req.PrevLogIndex, len(req.Entries))

    defer func() {
        p.proceed <- true
    }()

    resp := p.server.Transporter().SendAppendEntriesRequest(p.server, p, req)
	if resp == nil {
		p.server.DispatchEvent(newEvent(HeartbeatIntervalEventType, p, nil))
		debugln("peer.append.timeout: ", p.server.Name(), "->", p.Name)
		return
	}
	traceln("peer.append.resp: ", p.server.Name(), "<-", p.Name)

	// Attach the peer to resp, thus server can know where it comes from
	resp.peer = p.Name
	// Send response to server for processing.
	p.server.sendAsync(resp)
}

// Sends an Snapshot request to the peer through the transport.
func (p *Peer) sendSnapshotRequest(req *SnapshotRequest) {
	debugln("peer.snap.send: ", p.Name)

	resp := p.server.Transporter().SendSnapshotRequest(p.server, p, req)
	if resp == nil {
		debugln("peer.snap.timeout: ", p.Name)
		return
	}

	debugln("peer.snap.recv: ", p.Name)

	// If successful, the peer should have been to snapshot state
	// Send it the snapshot!
	if resp.Success {
		p.sendSnapshotRecoveryRequest()
	} else {
		debugln("peer.snap.failed: ", p.Name)
		return
	}

}

// Sends an Snapshot Recovery request to the peer through the transport.
func (p *Peer) sendSnapshotRecoveryRequest() {
	req := newSnapshotRecoveryRequest(p.server.name, p.server.lastSnapshot)
	debugln("peer.snap.recovery.send: ", p.Name)
	resp := p.server.Transporter().SendSnapshotRecoveryRequest(p.server, p, req)

	if resp == nil {
		debugln("peer.snap.recovery.timeout: ", p.Name)
		return
	}

	if resp.Success {
		p.prevLogIndex = req.LastIndex
	} else {
		debugln("peer.snap.recovery.failed: ", p.Name)
		return
	}

	p.server.sendAsync(resp)
}

//--------------------------------------
// Vote Requests
//--------------------------------------

// send VoteRequest Request
func (p *Peer) sendVoteRequest(req *RequestVoteRequest, c chan *RequestVoteResponse) {
	debugln("peer.vote: ", p.server.Name(), "->", p.Name)
	req.peer = p
	if resp := p.server.Transporter().SendVoteRequest(p.server, p, req); resp != nil {
		debugln("peer.vote.recv: ", p.server.Name(), "<-", p.Name)
		resp.peer = p
		c <- resp
	} else {
		debugln("peer.vote.failed: ", p.server.Name(), "<-", p.Name)
	}
}
