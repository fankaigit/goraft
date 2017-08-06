package main

import (
	"time"
	"math/rand"
)

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func sum(values []int, convert func(int) int) int {
	result := 0
	for i := 0; i < len(values); i++ {
		result += convert(values[i])
	}
	return result
}

func any(values []bool) bool {
	for i := 0; i < len(values); i++ {
		if values[i] {
			return true
		}
	}
	return false
}

func all(values []bool) bool {
	for i := 0; i < len(values); i++ {
		if !values[i] {
			return false
		}
	}
	return true
}

func none(values []bool) bool {
	for i := 0; i < len(values); i++ {
		if values[i] {
			return false
		}
	}
	return true
}

func wait(base time.Duration) {
	duration := randDuration(base)
	time.Sleep(duration)
}

func randDuration(base time.Duration) time.Duration {
	rand.Seed(time.Now().UnixNano())
	factor := rand.NormFloat64() / 2 + 1
	if factor < 0.5 {
		factor = 0.5
	}
	if factor > 1.5 {
		factor = 1.5
	}
	return time.Nanosecond * time.Duration(float64(base.Nanoseconds())*factor)
}

type MessageHeap struct {
	messages []WrappedMessage
	size int
}

func (h *MessageHeap) Len() int {
	return h.size
}

func (h *MessageHeap) Less(i, j int) bool {
	return h.messages[i].deliverTime.Before(h.messages[j].deliverTime)
}

func (h *MessageHeap) Swap(i, j int) {
	m := h.messages[i]
	h.messages[i] = h.messages[j]
	h.messages[j] = m
}

func (h *MessageHeap) Push(x interface{}) {
	if h.size == len(h.messages) {
		h.messages = append(h.messages, x.(WrappedMessage))
	} else {
		h.messages[h.size] = x.(WrappedMessage)
	}
	h.size ++
}

func (h *MessageHeap) Pop() interface{} {
	h.size -= 1
	return h.messages[h.size]
}
