package events

import "testing"

func TestSinceReturnsNewerEventsInOrder(t *testing.T) {
	l := New(10)
	for range 3 {
		l.Record("t", Info, "m", "", "")
	}
	got, last := l.Since(1)
	if last != 3 {
		t.Fatalf("last seq = %d, want 3", last)
	}
	if len(got) != 2 || got[0].Seq != 2 || got[1].Seq != 3 {
		t.Fatalf("Since(1) = %+v, want seqs 2,3", got)
	}
}

func TestRingKeepsOnlyNewest(t *testing.T) {
	l := New(3)
	for range 5 {
		l.Record("t", Info, "m", "", "")
	}
	got, last := l.Since(0)
	if last != 5 {
		t.Fatalf("last seq = %d, want 5", last)
	}
	if len(got) != 3 || got[0].Seq != 3 || got[2].Seq != 5 {
		t.Fatalf("Since(0) = %+v, want seqs 3,4,5", got)
	}
}

func TestNilLogIsUsable(t *testing.T) {
	var l *Log
	l.Record("t", Info, "m", "", "")
	got, last := l.Since(0)
	if len(got) != 0 || last != 0 {
		t.Fatalf("nil log returned %v, %d", got, last)
	}
}
