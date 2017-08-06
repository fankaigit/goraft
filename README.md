# goraft
This is a playground implementation of the raft consensus algorithm, as described in the raft paper, for the purpose of learning raft and golang.

Goroutines are used to simulate a distributed system, with each goroutine working like a node, communicating with each other with channel only.

Goroutines will stop once in a while, simulating MTBF and MTTR of nodes. And new elections will be held when master is down.

The workout is a stream of letters. The only form of this steam will be agreed by all nodes, guaranteed by raft algorithm.
