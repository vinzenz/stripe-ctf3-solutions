package server

import (
    "bytes"
    "fmt"
    "github.com/gorilla/mux"
    "io/ioutil"
    "encoding/json"
    "net/http"
    "path/filepath"
    "stripe-ctf.com/sqlcluster/log"
    "stripe-ctf.com/sqlcluster/raft"
    "stripe-ctf.com/sqlcluster/sql"
    "stripe-ctf.com/sqlcluster/transport"
    "stripe-ctf.com/sqlcluster/util"
    "time"
)

type Server struct {
    name             string
    path             string
    listen           string
    connectionString string
    router           *mux.Router
    httpServer       *http.Server
    httpServer2      *http.Server
    sql              *sql.SQL
    client           *transport.Client
    raftServer       raft.Server
    execCommand      chan *ExecCommand
}

// This is a hack around Gorilla mux not providing the correct net/http
// HandleFunc() interface.
func (s *Server) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
    s.router.HandleFunc(pattern, handler)
}

type Join struct {
    Self ServerAddress `json:"self"`
}

type JoinResponse struct {
    Self    ServerAddress   `json:"self"`
    Members []ServerAddress `json:"members"`
}

type Replicate struct {
    Self  ServerAddress `json:"self"`
    Query []byte        `json:"query"`
}

type ReplicateResponse struct {
    Self ServerAddress `json:"self"`
}

// Creates a new server.
func New(path, listen, join string) (*Server, error) {
    cs, err := transport.Encode(listen)
    if err != nil {
        return nil, err
    }

    sqlPath := filepath.Join(path, "storage.sql")
    util.EnsureAbsent(sqlPath)

    s := &Server{
        path:             path,
        listen:           listen,
        name:             path,
        connectionString: cs,
        sql:              sql.NewSQL(sqlPath),
        router:           mux.NewRouter(),
        client:           transport.NewClient(),
        execCommand:      make(chan *ExecCommand),
    }

    return s, nil
}

// Starts the server.
func (s *Server) ListenAndServe(primary string) error {
    var err error
    // Initialize and start HTTP server.
    s.httpServer = &http.Server{
        Handler: s.router,
    }
    s.httpServer2 = &http.Server{
        Handler: s.router,
    }

    transporter := raft.NewHTTPTransporter("/raft")
    s.raftServer, err = raft.NewServer(s.name, s.path, transporter, nil, s.sql, "")
    if err != nil {
        log.Fatal(err)
    }
    transporter.Install(s.raftServer, s)
    s.raftServer.Start()

    if primary != "" {
        log.Println("Attempting to join primary:", primary)
        if !s.raftServer.IsLogEmpty() {
            log.Fatal("Cannot join with an existing log")
        }
        if err := s.Join(primary); err != nil {
            log.Fatal(err)
        }
    } else if s.raftServer.IsLogEmpty() {
        // Initialize the server by joining itself.

        log.Println("Initializing new cluster")
        _, err := s.raftServer.Do(&raft.DefaultJoinCommand{
            Name:             s.name,
            ConnectionString: s.connectionString,
        })
        if err != nil {
            log.Fatal(err)
        }

    } else {
        log.Println("Recovered from log")
    }

    log.Println("Initializing HTTP server")

    s.router.HandleFunc("/sql", transport.MakeGzipHandler(s.sqlHandler)).Methods("POST")
    s.router.HandleFunc("/forward", s.forwardHandler).Methods("GET")
    s.router.HandleFunc("/healthcheck", transport.MakeGzipHandler(s.healthcheckHandler)).Methods("GET")
    s.router.HandleFunc("/join", transport.MakeGzipHandler(s.joinHandler)).Methods("POST")

    log.Println("Listening at:", s.connectionString)

    // Start Unix transport
    l, err := transport.Listen(s.listen)
    for retry := 0; retry < 5 && err != nil; retry += 1 {
        time.Sleep(100 * time.Millisecond)
        l, err = transport.Listen(s.listen)
    }
    if err != nil {
        log.Fatal(err)
    }
    return s.httpServer.Serve(l)
}

// Client operations
/*
func (s *Server) healthcheckPrimary() bool {
    _, err := s.client.SafeGet(s.cluster.primary.ConnectionString, "/healthcheck")

    if err != nil {
        log.Printf("The primary appears to be down: %s", err)
        return false
    } else {
        return true
    }
}
*/
// Join an existing cluster
func (s *Server) Join(primary string) error {
    command := &raft.DefaultJoinCommand{
        Name:             s.raftServer.Name(),
        ConnectionString: s.connectionString,
    }

    b := util.JSONEncode(command)
    log.Printf("Joining Cluster: %s", primary)
    cs, err := transport.Encode(primary)
    if err != nil {
        return err
    }

    for i:=0; i < 5; i+=1 {
        log.Printf("Join -> %s Command: %s", cs, command)
        _, err = s.client.SafePost(cs, "/join", b)
        if err != nil {
            log.Printf("Unable to join cluster: %s", err)
        } else {
            break
        }
    }
    return err
}

// Server handlers
func (s *Server) joinHandler(w http.ResponseWriter, req *http.Request) {
    log.Printf("handling /join %v", req);
    j := &raft.DefaultJoinCommand{}

    if err := util.JSONDecode(req.Body, j); err != nil {
        log.Printf("Invalid join request: %s", err)
        return
    }

    go func() {
        log.Printf("Handling join request: %#v", j)

        // Add node to the cluster
        if _, err := s.raftServer.Do(j); err != nil {
            return
        }
    }()
}


func (s *Server) PingServer(cs string) bool {
    _, err := s.client.SafeGet(cs, "/healthcheck");
    return err == nil;
}

// This is the only user-facing function, and accordingly the body is
// a raw string rather than JSON.
func (s *Server) sqlHandler(w http.ResponseWriter, req *http.Request) {
    // Read the value from the POST body.
    b, err := ioutil.ReadAll(req.Body)
    if err != nil {
        w.WriteHeader(http.StatusBadRequest)
        return
    }

    value := string(b)
    var output *sql.Output


    for {
        if s.raftServer.State() == raft.Candidate {
            time.Sleep(time.Millisecond * 100)
        } else {
            break;
        }
    }
/*


    if s.raftServer.State() != raft.Leader {
        cmd := &ExecCommand{
            Data: *raft.NewExecuteCommand("", value),
            Reply: make(chan *raft.ExecuteCommandReply),
        }
        s.execCommand <- cmd
        reply := <-cmd.Reply
        output = &reply.Output
        err = reply.Error
    } else {
*/
    if s.raftServer.State() == raft.Leader {
        resp, err := s.raftServer.Do(raft.NewExecuteCommand(s.raftServer.Leader(), value))
        if err == nil {
            output = resp.(*sql.Output)
        }
        formatted := ""
        // Execute the command against the Raft server.
        if err != nil {
            http.Error(w, err.Error(), http.StatusBadRequest)
            formatted = err.Error()
        } else {
            formatted = fmt.Sprintf("SequenceNumber: %d\n%s", output.SequenceNumber, output.Stdout)
        }
        log.Debugf("[%s] Returning response to %#v: %#v", s.raftServer.Leader(), value, formatted)
        w.Write([]byte(formatted))
    } else {
/*
        cmd := &ExecCommand{
            Data: *raft.NewExecuteCommand("", value),
            Reply: make(chan *raft.ExecuteCommandReply),
        }
        s.execCommand <- cmd
        reply := <-cmd.Reply
        output = &reply.Output
        err = reply.Error
        formatted := ""
        // Execute the command against the Raft server.
        if err != nil {
            http.Error(w, err.Error(), http.StatusBadRequest)
            formatted = err.Error()
        } else {
            formatted = fmt.Sprintf("SequenceNumber: %d\n%s", output.SequenceNumber, output.Stdout)
        }
        log.Debugf("[%s] Returning response to %#v: %#v", s.raftServer.Leader(), value, formatted)
        w.Write([]byte(formatted))
*/
        for {
            p, ok := s.raftServer.Peers()[s.raftServer.Leader()]
            if ok {
                r, e := s.client.SafePost(p.ConnectionString, "/sql", bytes.NewBuffer(b))
                if e == nil {
                    rb, e := ioutil.ReadAll(r)
                    if e == nil {
                        w.Write(rb)
                        return
                    }
                }
            }
            time.Sleep(100 * time.Millisecond)
        }
    }
}

func (s *Server) healthcheckHandler(w http.ResponseWriter, req *http.Request) {
    w.WriteHeader(http.StatusOK)
}

type ExecCommand struct {
    Data raft.ExecuteCommand
    Reply chan *raft.ExecuteCommandReply
}

func (s *Server) forwardHandler(w http.ResponseWriter, req *http.Request) {
    log.Printf("############################# forward: Initiating forward loop")
    h, ok := w.(http.Hijacker)
    if !ok {
        log.Printf("########################## forward: Failed to hijack connection for forwarding. Aborting")
        return
    }
    netConn, rw, err := h.Hijack()
    if err != nil {
        log.Printf("######################### forward: Failed to hijack connection for forwarding: %v Aborting", err)
        return
    }
    defer netConn.Close()
    reader := json.NewDecoder(rw)
    writer := json.NewEncoder(rw)
    log.Printf("############################ forward: Forward loop initiated")
    for {
        cmd := <-s.execCommand
        log.Printf("############ Received command to forward: %v", cmd.Data)
        err := writer.Encode(&cmd.Data)
        if err != nil {
            log.Printf("######################### forward: Writer cannot encode command: %v Stopping loop", err)
            return
        }
        reply := &raft.ExecuteCommandReply{}
        err = reader.Decode(reply)
        if err != nil {
            log.Printf("############################ forward: Reader cannot decode command reply: %v Stopping loop", err)
            return
        }
        log.Printf("############# Received reply via forward: %v", reply)
        cmd.Reply <- reply
    }
}
