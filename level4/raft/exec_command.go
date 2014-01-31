package raft

import (
	"stripe-ctf.com/sqlcluster/sql"
)

type ExecuteCommandReply struct {
	Error  error
	Output sql.Output
}

func (c *ExecuteCommandReply) CommandName() string {
	return "execute-reply"
}

func (c *ExecuteCommandReply) Apply(server Server) (interface{}, error) {
	return nil, nil
}

func NewExecuteCommandReply(result sql.Output, err error) *ExecuteCommandReply {
	return &ExecuteCommandReply{
		Error:  err,
		Output: result,
	}
}

// This command writes a value to a key.
type ExecuteCommand struct {
	Command string
	Tag     string
}

// Creates a new write command.
func NewExecuteCommand(tag string, command string) *ExecuteCommand {
	return &ExecuteCommand{
		Command: command,
		Tag:     tag,
	}
}

// The name of the command in the log.
func (c *ExecuteCommand) CommandName() string {
	return "execute"
}

// Executes a value to a key.
func (c *ExecuteCommand) Apply(server Server) (interface{}, error) {
	db := server.Context().(*sql.SQL)
	return db.Execute(c.Tag, c.Command)
}
