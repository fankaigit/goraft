package main

import (
	"time"
	"fmt"
)

func slaveHandleRecordRequest(me *Node, msg Message) {
	debug(me, "handle", msg)
	request := msg.payload.(RecordRequest)
	reply := new(RecordReply)
	if request.term < me.term || !checkPrev(me, request) {
		info(me, "reject", request)
		reply.accepted = false
	} else {
		reply.accepted = true
		me.lastHeartbeat = time.Now()
		if len(request.logs) > 0 {
			appendLogs(me, request)
			reply.matched = len(me.logs)
		}
	}
	reply.request = request
	reply.term = me.term
	router <- Message{me.id, request.master, RECORD_REPLY, *reply, time.Now()}
}

func appendLogs(me *Node, request RecordRequest) {
	me.logs = me.logs[:request.prevLogIndex+1]
	for _, log := range request.logs {
		me.logs = append(me.logs, log)
		me.term = max(me.term, log.term)
	}
	me.commit = min(len(me.logs), request.commit)
	debug(me, "accept", request)
	logSlaveStatus(me)
}

func checkPrev(me *Node, request RecordRequest) bool {
	debug(me, "logs:", me.logs)
	index := request.prevLogIndex
	if len(request.logs) == 0 {
		// heartbeat
		return true
	}
	if len(me.logs) <= index || me.logs[index].term != request.prevLogTerm {
		return false
	}
	return true
}

func logSlaveStatus(me *Node) {
	debug(me, fmt.Sprintf("status L=%v", me.logs))
}
