package workerpool

import (
	"math/rand"
)

type ring[T any] struct {
	buffer []T
	start  int
	end    int
	Count  int
}

func newRing[T any](capacity int) ring[T] {
	return ring[T]{buffer: make([]T, capacity)}
}

func ringPush[T any](r *ring[T], v T) {
	if r.Count >= len(r.buffer) {
		panic("cannot push to full ring buffer")
	}
	r.buffer[r.end] = v
	r.end++
	if r.end == len(r.buffer) {
		r.end = 0
	}
	r.Count++
}

func ringPop[T any](r *ring[T]) (T, bool) {
	var zero T
	if r.Count <= 0 {
		return zero, false
	}
	v := r.buffer[r.start]
	r.buffer[r.start] = zero
	r.start++
	if r.start == len(r.buffer) {
		r.start = 0
	}
	r.Count--
	return v, true
}

func (r *ring[T]) Shuffle() {
	rand.Shuffle(len(r.buffer), func(i, j int) {
		r.buffer[i], r.buffer[j] = r.buffer[j], r.buffer[i]
	})
}
