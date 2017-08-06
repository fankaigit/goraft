package main

import (
	"sort"
	"fmt"
	"time"
)

func handleWork(me *Node, msg Message) {
	if me.phase != MASTER {
		return
	}
	v := msg.payload
	work(me, Log{me.term, v})
}

func startMaster(me *Node) {
	me.phase = MASTER
	for i := 0; i < nodeSize; i++ {
		me.pushedIndex[i] = len(me.logs) - 1
		me.matchedIndex[i] = me.commit
	}
	info(me, "start master")
	heartbeat(me)
}

func heartbeat(me *Node) {
	if time.Since(me.lastHeartbeat) < WAKE_UP_INTERVAL {
		return
	}
	debug(me, "heartbeat")
	var logs []Log
	for node := 0; node < nodeSize; node++ {
		if node == me.id {
			continue
		}
		push(me, logs, node)
	}
	me.lastHeartbeat = time.Now()
}

func work(me *Node, log Log) {
	me.logs = append(me.logs, log)
	me.pushedIndex[me.id] = len(me.logs) - 1
	me.matchedIndex[me.id] = len(me.logs) - 1
	logMasterStatus(me)
	sync(me)
}

func sync(me *Node) {
	for node := 0; node < nodeSize; node++ {
		if node == me.id {
			continue
		}
		syncTo(me, node)
	}
}

func syncTo(me *Node, to int) {
	if me.pushedIndex[to]+1 >= len(me.logs) {
		return
	}
	logs := me.logs[me.pushedIndex[to]+1:]
	push(me, logs, to)
}

func push(me *Node, logs []Log, to int) {
	//debug(me, "pushed index", me.pushedIndex)
	prev := me.pushedIndex[to]
	req := RecordRequest{me.term, me.id, logs, prev, me.logs[prev].term, me.commit}
	msg := Message{me.id, to, RECORD_REQUEST, req, time.Now()}
	router <- msg
}

func masterHandleRecordReply(me *Node, msg Message) {
	debug(me, "handle", msg)
	reply := msg.payload.(RecordReply)
	node := msg.from
	if reply.request.isHeartbeat() {
		return
	}
	if reply.matched <= me.matchedIndex[node] {
		info(me, "ignore deprecated reply")
	}
	if reply.accepted {
		index := reply.request.prevLogIndex + len(reply.request.logs)
		me.matchedIndex[node] = index
		me.pushedIndex[node] = index
		updateCommit(me)
	} else {
		me.pushedIndex[node] = max(me.pushedIndex[node] - 1, 0)
		info(me, "retry from", me.pushedIndex[node])
		syncTo(me, node)
	}
}

func updateCommit(me *Node) {
	matched := me.matchedIndex
	sort.Ints(matched[:])
	commit := matched[nodeSize/2]
	if me.commit < commit {
		debug(me, "update commit from", me.commit, "to", commit)
		me.commit = commit
		logMasterStatus(me)
	}
}

func logMasterStatus(me *Node) {
	info(me, fmt.Sprintf("status P=%v M=%v C=%d L=%v", me.pushedIndex,
		me.matchedIndex, me.commit, me.logs))
}
