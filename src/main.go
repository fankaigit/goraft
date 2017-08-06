package main

import (
	"math/rand"
	"time"
	"log"
	"fmt"
	"container/heap"
)

const nodeSize = 9
const debugMode = true

const ANY_NODE = -1
const NETWORK_DELAY = time.Millisecond * 5
const ELECTION_TIMEOUT = time.Millisecond * 1000
const HEARTBEAT_TIMEOUT = time.Millisecond * 1000
const WAKE_UP_INTERVAL = time.Millisecond * 100
const WORK_INTERVAL = time.Millisecond * 2000
const MTBF = time.Second * 30
const MTTR = time.Second * 5

var mailbox [nodeSize]chan Message
var router chan Message
var nodes [nodeSize]*Node
var isDown = [nodeSize]bool{}
var appStartTime = time.Now()
var nextDownTime = [nodeSize]time.Time{}
var messageHeap *MessageHeap

type logWriter struct {
}

func (writer logWriter) Write(bytes []byte) (int, error) {
	duration := time.Since(appStartTime).Seconds()
	ts := time.Now().Format("15:04:05.000")
	return fmt.Printf("%s\t%.03f\t%s", ts, duration, string(bytes))
}

func info(v ...interface{}) {
	log.Println(v...)
}

func debug(v ...interface{}) {
	if debugMode {
		log.Println(v...)
	}
}

func main() {
	setUpEnv()

	initializeNodes()

	go startMessageCollecting()
	go startMessageDelivering()
	go wakeUpNodes()
	startClient()
}

func initializeNodes() {
	router = make(chan Message, 10000)
	for i := 0; i < nodeSize; i++ {
		nodes[i] = new(Node)
		nodes[i].id = i
		nodes[i].term = 0
		nodes[i].phase = SLAVE
		nodes[i].commit = 0
		nodes[i].voted = make(map[int]int)
		nodes[i].logs = []Log{{0, NO_OP_LOG}}
		nodes[i].lastHeartbeat = time.Now()

		mailbox[i] = make(chan Message, 1000)
		go runNode(nodes[i])
	}
}

func setUpEnv() {
	log.SetFlags(0)
	log.SetOutput(new(logWriter))
	rand.Seed(time.Now().UnixNano())
	messageHeap = new(MessageHeap)
}

func startMessageCollecting() {
	info("[main] start router")
	for {
		msg := <-router
		if msg.from != ANY_NODE {
			debug("[main] route:", msg)
		}
		if msg.to == ANY_NODE {
			for i := 0; i < nodeSize; i++ {
				if i == msg.from {
					continue
				}
				msg.to = i
				bufferMessage(msg)	// will copy
			}
		} else {
			bufferMessage(msg)
		}
	}
}

func bufferMessage(msg Message) Message {
	wm := WrappedMessage{}
	wm.Message = msg
	wm.deliverTime = time.Now().Add(randDuration(NETWORK_DELAY))
	heap.Push(messageHeap, wm)
	return msg
}

func startMessageDelivering() {
	for {
		if messageHeap.size == 0 {
			time.Sleep(time.Millisecond)
			continue
		}
		wm := heap.Pop(messageHeap).(WrappedMessage)
		if time.Now().After(wm.deliverTime) {
			debug("delivering", wm)
			mailbox[wm.to] <- wm.Message
		} else {
			heap.Push(messageHeap, wm)
			time.Sleep(time.Millisecond)
		}
	}
}

func wakeUpNodes() {
	for {
		debug("[main] wake up nodes")
		wakeUp := Message{ANY_NODE, ANY_NODE, WAKE_UP, nil, time.Now()}
		router <- wakeUp
		time.Sleep(WAKE_UP_INTERVAL)
	}
}

func startClient() {
	info("[main] start client")
	for {
		wait(WORK_INTERVAL)
		letters := "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
		r := rand.Int() % len(letters)
		value := letters[r:r+1]
		info("[main] work", value)
		wakeUp := Message{ANY_NODE, ANY_NODE, WORK, value, time.Now()}
		router <- wakeUp
	}
}