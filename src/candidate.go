package main

import "time"

func candidateHandleVoteReply(me *Node, msg Message) {
	debug(me, "handle", msg)
	if me.phase != CANDIDATE {
		return
	}
	reply := msg.payload.(VoteReply)
	if reply.request.term != me.term {
		info("ignore outdated", msg)
		return
	}
	me.voteReplies[msg.from] = reply
	cnt := 0
	for _, r := range me.voteReplies {
		if r.accepted {
			cnt += 1
		}
	}
	info(me, "count voted, accepted=", cnt)
	if cnt >= nodeSize/2 {
		startMaster(me)
	}
}


func startElection(me *Node) {
	me.phase = CANDIDATE
	updateTerm(me)
	me.voted[me.term] = me.id
	me.voteReplies = make(map[int]VoteReply, 0)
	me.lastElectionTime = time.Now()
	request := VoteRequest{me.term, me.id, len(me.logs), me.logs.lastTerm()}
	msg := Message{me.id, -1, VOTE_REQUEST, request, time.Now()}
	router <- msg
	info(me, "run for master, my voted=", me.voted)
}

func updateTerm(me *Node) {
	maxVote := 0
	for k := range me.voted {
		if k > maxVote {
			maxVote = k
		}
	}
	me.term = maxVote + 1
}

func candidate(me *Node) {
	//info(me, "is candidate")
	//info(me, "duration since last election", time.Since(me.lastElectionTime))
	if time.Since(me.lastElectionTime) > randDuration(ELECTION_TIMEOUT) {
		startElection(me)
	} else {
		//info(me, "continue waiting for vote")
	}
}