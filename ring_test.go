package workerpool

import (
	"testing"
)

func assertEqual[T comparable](t *testing.T, expected, actual T) {
	if expected != actual {
		t.Helper()
		t.Errorf("expected=%v actual=%v", expected, actual)
	}
}

func TestRing(t *testing.T) {
	const n = 10
	r := newRing[int](n)
	for i := 0; i < n/2; i++ {
		ringPush(&r, i)
		assertEqual(t, 0, r.start)
		assertEqual(t, i+1, r.end)
		assertEqual(t, i+1, r.Count)
	}
	for i := 0; i < n/2; i++ {
		v, ok := ringPop(&r)
		if !ok {
			t.Error("ring is empty")
		}
		assertEqual(t, i, v)
		assertEqual(t, i+1, r.start)
		assertEqual(t, n/2, r.end)
		assertEqual(t, n/2-(i+1), r.Count)
	}
	if _, ok := ringPop(&r); ok {
		t.Error("ring is not empty")
	}
	for i := 0; i < n; i++ {
		ringPush(&r, i)
		assertEqual(t, n/2, r.start)
		assertEqual(t, (n/2+i+1)%n, r.end)
		assertEqual(t, i+1, r.Count)
	}
	for i := 0; i < n; i++ {
		v, ok := ringPop(&r)
		if !ok {
			t.Error("ring is empty")
		}
		assertEqual(t, i, v)
		assertEqual(t, (n/2+i+1)%n, r.start)
		assertEqual(t, n/2, r.end)
		assertEqual(t, n-(i+1), r.Count)
	}
	if _, ok := ringPop(&r); ok {
		t.Error("ring is not empty")
	}
}
