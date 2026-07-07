package epaxos

import "sync"

var messagePool = sync.Pool{New: func() any { return new(Message) }}
var commandPool = sync.Pool{New: func() any { return new(Command) }}

// GetMessage returns a zeroed message from the package pool.
func GetMessage() *Message {
	m := messagePool.Get().(*Message)
	m.Reset()
	return m
}

// PutMessage returns a message to the package pool after clearing references.
func PutMessage(m *Message) {
	if m == nil {
		return
	}
	m.Reset()
	messagePool.Put(m)
}

// GetCommand returns a zeroed command from the package pool.
func GetCommand() *Command {
	c := commandPool.Get().(*Command)
	c.Reset()
	return c
}

// PutCommand returns a command to the package pool after clearing references.
func PutCommand(c *Command) {
	if c == nil {
		return
	}
	c.Reset()
	commandPool.Put(c)
}
