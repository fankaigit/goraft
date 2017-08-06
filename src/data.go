package main

import (
	"fmt"
	"time"
	"bytes"
)

type Command int

const (
	WAKE_UP        Command = iota
	VOTE_REQUEST
	VOTE_REPLY
	RECORD_REQUEST
	RECORD_REPLY
	WORK
)

type Phase int

const (
	MASTER    Phase = iota
	SLAVE
	CANDIDATE
)

type Message struct {
	from      int
	to        int
	cmd       Command
	payload   interface{}
	timestamp time.Time
}

type WrappedMessage struct {
	Message
	deliverTime time.Time
}

func (m Message) String() string {
	//js, _ := json.Marshal(m.payload)
	//js := string(m.payload)
	return fmt.Sprintf("[%d->%d] %d %v %v", m.from, m.to, m.cmd, m.payload,
		m.timestamp.Format("15:04:05.000"))
}

type VoteRequest struct {
	term        int
	candidate   int
	logSize     int
	lastLogTerm int
}

func (r VoteRequest) String() string {
	return fmt.Sprintf("{Vote: T%d C%d LS=%d LT=%d}", r.term, r.candidate, r.logSize, r.lastLogTerm)
}

type VoteReply struct {
	request  VoteRequest
	accepted bool
}

func (r VoteReply) String() string {
	return fmt.Sprintf("{Reply: %v %v}", r.request, r.accepted)
}

type RecordRequest struct {
	term         int
	master       int
	logs         Logs
	prevLogIndex int
	prevLogTerm  int
	commit       int
}

func (r RecordRequest) String() string {
	if len(r.logs) == 0 {
		return fmt.Sprintf("{Heartbeat: T%d M%d}", r.term, r.master)
	} else {
		return fmt.Sprintf("{Record: T%d M%d C%d LI%d LT%d L=%v}", r.term, r.master, r.commit,
			r.prevLogIndex, r.prevLogTerm, r.logs)
	}
}

func (r RecordRequest) isHeartbeat() bool {
	return len(r.logs) == 0
}

type RecordReply struct {
	request  RecordRequest
	term     int
	accepted bool
	matched  int
}

func (r RecordReply) String() string {
	return fmt.Sprintf("{Reply: %v T%d %v}", r.request, r.term, r.accepted)
}

type Node struct {
	id     int
	term   int
	phase  Phase
	logs   Logs
	commit int
	voted  map[int]int

	// for slave
	lastHeartbeat time.Time

	// for candidate
	lastElectionTime time.Time
	voteReplies      map[int]VoteReply

	// for master
	pushedIndex       [nodeSize]int
	matchedIndex      [nodeSize]int
	lastHeartbeatTime time.Time
}

func (n Node) String() string {
	if n.phase == MASTER {
		return fmt.Sprintf("[%d master@T%d:C%d]", n.id, n.term, n.commit)
	} else if n.phase == SLAVE {
		return fmt.Sprintf("[%d slave@T%d:C%d]", n.id, n.term, n.commit)
	} else {
		return fmt.Sprintf("[%d candidate@T%d:C%d]", n.id, n.term, n.commit)
	}
}

const NO_OP_LOG = "-"

type Log struct {
	term  int
	value interface{}
}

type Logs []Log

func (logs Logs) String() string {
	var buffer bytes.Buffer
	buffer.WriteString("[")
	lastTerm := -1
	for i := 0; i < len(logs); i++ {
		if logs[i].term > lastTerm {
			if i > 0 {
				buffer.WriteString(" ")
			}
			buffer.WriteString(fmt.Sprintf("%d:", logs[i].term))
			lastTerm = logs[i].term
		}
		buffer.WriteString(fmt.Sprintf("%v", logs[i].value))
	}
	buffer.WriteString("]")

	return buffer.String()
}

func (logs Logs) lastTerm() int {
	var lastLogTerm int
	if len(logs) == 0 {
		lastLogTerm = 0
	} else {
		lastLogTerm = logs[len(logs)-1].term
	}
	return lastLogTerm
}