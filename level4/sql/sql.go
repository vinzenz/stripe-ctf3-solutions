package sql

import (
    "database/sql"
    _ "github.com/mattn/go-sqlite3"
	"stripe-ctf.com/sqlcluster/log"
	"sync"
	"strings"
	"strconv"
	"fmt"
)

type Column struct {
    name string
    fcount uint64
    rcount uint64
    word string
}

type SQL struct {
	path           string
	sequenceNumber int
	mutex          sync.Mutex
    db             *sql.DB
    tbl            []*Column
}

type Output struct {
	Stdout         []byte
	Stderr         []byte
	SequenceNumber int
}
func NewSQL(path string) *SQL {
	sql := &SQL{
		path: path,
        tbl: []*Column {
            &Column{ name: "siddarth", fcount: 0, rcount: 0, word: "", },
            &Column{ name: "gdb", fcount: 0, rcount: 0, word: "", },
            &Column{ name: "christian", fcount: 0, rcount: 0, word: "", },
            &Column{ name: "andy", fcount: 0, rcount: 0, word: "", },
            &Column{ name: "carl", fcount: 0, rcount: 0, word: "", },
        },
	}
	return sql
}

func (sql *SQL) Execute(tag string, command string) (*Output, error) {
	// TODO: make sure I can catch non-lock issuez
//	sql.mutex.Lock()
//	defer sql.mutex.Unlock()

	defer func() { sql.sequenceNumber += 1 }()
	if tag == "primary" || log.Verbose() {
		log.Printf("[%s] [%d] Executing %#v", tag, sql.sequenceNumber, command)
	}

    var o, e []byte
    if sql.sequenceNumber > 0 {
        //  "UPDATE ctf3 SET friendCount=friendCount+39,
        //  requestCount=requestCount+1, favoriteWord=\"qngjegjdfebapph\" WHERE
        //  name=\"gdb\"; SELECT * FROM ctf3;"
        col := Column{}
        parts := strings.Split(command, " ")
        idx := 0
        for _, part := range parts {
            x := strings.Split(part, "=")
            if len(x) == 2 {
                switch x[0] {
                case "requestCount":
                    x[1] = strings.TrimRight(x[1], ",")
                    col.rcount, _ = strconv.ParseUint(x[1][len(x[0]) + 1:], 10, 64)
                case "friendCount":
                    x[1] = strings.TrimRight(x[1], ",")
                    col.fcount, _ = strconv.ParseUint(x[1][len(x[0]) + 1:], 10, 64)
                case "favoriteWord":
                    col.word = strings.Trim(x[1], "\"")
                case "name":
                    name := strings.Split(x[1], "\"")[1]
                    switch name {
                    case "siddarth":
                        idx = 0
                    case "gdb":
                        idx = 1
                    case "christian":
                        idx = 2
                    case "andy":
                        idx = 3
                    case "carl":
                        idx = 4
                    }
                }
            }
        }
        sql.tbl[idx].rcount += col.rcount
        sql.tbl[idx].fcount += col.fcount
        sql.tbl[idx].word = col.word

        result := ""
        for _, v := range sql.tbl {
            result += fmt.Sprintf("%s|%d|%d|%s\n", v.name, v.fcount, v.rcount, v.word)
        }
        o = []byte(result)
    }
	output := &Output{
		Stdout:         o,
		Stderr:         e,
		SequenceNumber: sql.sequenceNumber,
	}

	return output, nil
}
