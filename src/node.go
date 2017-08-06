package main

import (
	"time"
)

func runNode(me *Node) {
	for {
		msg := <-mailbox[me.id]
		if time.Since(msg.timestamp) > WAKE_UP_INTERVAL {
			debug("drop outdated", msg)
			continue
		}
		switch msg.cmd {
		case WAKE_UP:
			wakeUp(me)
		case VOTE_REQUEST:
			handleVoteRequest(me, msg)
		case VOTE_REPLY:
			handleVoteReply(me, msg)
		case RECORD_REQUEST:
			handleRecordRequest(me, msg)
		case RECORD_REPLY:
			handleRecordReply(me, msg)
		case WORK:
			handleWork(me, msg)
		}
	}
}

func handleVoteReply(me *Node, msg Message) {
	if me.phase != CANDIDATE {
		info(me, "not candidate, ignore", msg)
		return
	}
	candidateHandleVoteReply(me, msg)
}

func handleRecordRequest(me *Node, msg Message) {
	request := msg.payload.(RecordRequest)
	if request.term > me.term {
		info(me, "update term:", me.term, "->", request.term)
		me.term = request.term
		me.phase = SLAVE
	} else if request.term == me.term && me.phase == CANDIDATE {
		me.phase = SLAVE
	}
	if me.phase == SLAVE {
		slaveHandleRecordRequest(me, msg)
	} else {
		info(me, "not slave, ignore", msg)
	}
}

func handleRecordReply(me *Node, msg Message) {
	if me.phase != MASTER {
		info(me, "not master, ignore", msg)
		return
	}
	masterHandleRecordReply(me, msg)
}

func wakeUp(me *Node) {
	if downNodeByChance(me) {
		return
	}
	switch me.phase {
	case MASTER:
		heartbeat(me)
	case SLAVE:
		if time.Since(me.lastHeartbeat) > randDuration(HEARTBEAT_TIMEOUT) {
			info(me, "heartbeat timeout")
			candidate(me)
		}
	case CANDIDATE:
		candidate(me)
	}
}

func downNodeByChance(me *Node) bool {
	if nextDownTime[me.id].Second() == 0 {
		nextDownTime[me.id] = time.Now().Add(randDuration(MTBF))
	}

	if time.Now().After(nextDownTime[me.id]) {
		downNode(me)
		return true
	}
	return false
}

func downNode(me *Node) {
	info(me, ">>>>>>>>>>>>>>>>>>>> node down >>>>>>>>>>>>>>>>>>>>")
	isDown[me.id] = true
	wait(MTTR)
	isDown[me.id] = false
	info(me, "<<<<<<<<<<<<<<<<<<<<< node up <<<<<<<<<<<<<<<<<<<<<")
	nextDownTime[me.id] = time.Now().Add(randDuration(MTBF))
	me.lastHeartbeat = time.Now()	// do not start election too soon
}

// applies to master/slave/candidate
func handleVoteRequest(me *Node, msg Message) {
	debug(me, "handle", msg)
	request := msg.payload.(VoteRequest)
	reply := new(VoteReply)
	reply.request = request
	vote, ok := me.voted[request.term]
	if request.term < me.term {
		info(me, "reject for smaller term", request)
	} else if ok && vote != request.candidate {
		info(me, "reject for vote conflict", request, "voted=", me.voted)
		reply.accepted = false
	} else if request.logSize < len(me.logs) {
		info(me, "reject for less logs", request, "my log size=", len(me.logs))
		reply.accepted = false
	} else if request.lastLogTerm < me.logs.lastTerm() {
		info(me, "reject for smaller log term", request, "last log term=", me.logs.lastTerm())
		reply.accepted = false
	} else {
		info(me, "accept", request)
		reply.accepted = true
		me.voted[request.term] = request.candidate
	}

	router <- Message{me.id, request.candidate, VOTE_REPLY, *reply, time.Now()}
}
