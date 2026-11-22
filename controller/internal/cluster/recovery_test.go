package cluster

import (
	"testing"
	"time"
)

func TestMarkDeadMarksActiveCopiesLost(t *testing.T) {
	r := NewRegistry(LeastLoaded{})
	res, _ := r.Announce("w0", "http://a:1")
	r.CompleteSync("w0", res.SyncGen, []Replica{
		{ChunkID: "active", ObjectID: "o", SeqNo: 0, Status: ReplicaActive},
		{ChunkID: "bad", ObjectID: "o", SeqNo: 1, Status: ReplicaCorrupted},
	})
	res, _ = r.Announce("w1", "http://b:1")
	r.CompleteSync("w1", res.SyncGen, nil)

	// Nobody is older than a cutoff in the past.
	if dead := r.MarkDead(time.Now().Add(-time.Hour)); len(dead) != 0 {
		t.Fatalf("MarkDead in the past = %+v", dead)
	}
	time.Sleep(5 * time.Millisecond)
	r.Announce("w1", "http://b:1") // w1 keeps announcing
	dead := r.MarkDead(time.Now().Add(-2 * time.Millisecond))
	if len(dead) != 1 || dead[0].ID != "w0" || dead[0].Lost != 1 {
		t.Fatalf("MarkDead = %+v; want w0 with 1 lost copy", dead)
	}
	if w, _ := r.Worker("w0"); w.Status != WorkerDead {
		t.Fatalf("w0 status = %s", w.Status)
	}
	if w, _ := r.Worker("w1"); w.Status != WorkerReady {
		t.Fatalf("w1 status = %s", w.Status)
	}
	got := map[string]ReplicaStatus{}
	for _, l := range r.ObjectReplicas("o") {
		got[l.ChunkID] = l.Status
		if l.WorkerStatus != WorkerDead {
			t.Fatalf("location worker status = %s, want DEAD", l.WorkerStatus)
		}
	}
	if got["active"] != ReplicaLost || got["bad"] != ReplicaCorrupted {
		t.Fatalf("statuses = %v", got)
	}
	// Already dead: not reported again.
	if again := r.MarkDead(time.Now()); len(again) != 1 || again[0].ID != "w1" {
		t.Fatalf("second MarkDead = %+v; want only w1", again)
	}
}

func TestDeadWorkerAnnouncingAgainIsResynced(t *testing.T) {
	r := NewRegistry(LeastLoaded{})
	res, _ := r.Announce("w", "http://a:1")
	gen := res.SyncGen
	// Dies while its first sync is still running: that sync is discarded.
	r.MarkDead(time.Now().Add(time.Second))
	if ok, _ := r.CompleteSync("w", gen, nil); ok {
		t.Fatal("sync started before death was applied")
	}
	res, err := r.Announce("w", "http://a:1")
	if err != nil || !res.Revived || res.Moved || res.SyncGen == 0 || res.Worker.Status != WorkerAvailable {
		t.Fatalf("announce after death = %+v, %v", res, err)
	}
	if ok, _ := r.CompleteSync("w", res.SyncGen, []Replica{{ChunkID: "c", ObjectID: "o", Status: ReplicaActive}}); !ok {
		t.Fatal("resync rejected")
	}
	if n := len(r.Readable("o", 0)); n != 1 {
		t.Fatalf("readable after revive = %d", n)
	}
}

func TestPlaceReplicaExcludesHolders(t *testing.T) {
	r := readyRegistry(t, 3)
	for range 20 {
		loc, err := r.PlaceReplica("o", 0, map[string]bool{"w0": true, "w2": true}, seqIDs())
		if err != nil {
			t.Fatal(err)
		}
		if loc.WorkerID != "w1" || loc.Status != ReplicaPending || loc.Addr == "" {
			t.Fatalf("placed %+v; want PENDING on w1", loc)
		}
		r.Remove(loc.ChunkID)
	}
	if _, err := r.PlaceReplica("o", 0, map[string]bool{"w0": true, "w1": true, "w2": true}, seqIDs()); err == nil {
		t.Fatal("placed with every worker excluded")
	}
}

func TestViewAndPruneDead(t *testing.T) {
	r := NewRegistry(LeastLoaded{})
	res, _ := r.Announce("w0", "http://a:1")
	r.CompleteSync("w0", res.SyncGen, []Replica{{ChunkID: "c", ObjectID: "o", SeqNo: 2, Status: ReplicaActive}})
	r.Announce("w1", "http://b:1") // AVAILABLE

	v := r.View()
	if !v.Syncing {
		t.Fatal("View.Syncing = false with an AVAILABLE worker")
	}
	locs := v.Replicas[ChunkKey{ObjectID: "o", SeqNo: 2}]
	if len(locs) != 1 || locs[0].WorkerStatus != WorkerReady || locs[0].Addr != "http://a:1" {
		t.Fatalf("view = %+v", v.Replicas)
	}

	r.MarkDead(time.Now().Add(time.Second))
	// w0 still holds a copy; w1 holds none, but was seen too recently.
	if pruned := r.PruneDead(time.Now().Add(-time.Hour)); len(pruned) != 0 {
		t.Fatalf("pruned %v too early", pruned)
	}
	if pruned := r.PruneDead(time.Now().Add(time.Second)); len(pruned) != 1 || pruned[0] != "w1" {
		t.Fatalf("pruned %v; want [w1]", pruned)
	}
	r.Remove("c")
	if pruned := r.PruneDead(time.Now().Add(time.Second)); len(pruned) != 1 || pruned[0] != "w0" {
		t.Fatalf("pruned %v; want [w0]", pruned)
	}
}

func TestSetStatusIf(t *testing.T) {
	r := readyRegistry(t, 2)
	locs, _ := r.Place("o", 0, 2, seqIDs())
	id := locs[0].ChunkID
	if r.SetStatusIf(id, ReplicaActive, ReplicaCorrupted) {
		t.Fatal("changed a PENDING replica expecting ACTIVE")
	}
	if !r.SetStatusIf(id, ReplicaPending, ReplicaActive) {
		t.Fatal("PENDING -> ACTIVE refused")
	}
	if r.SetStatusIf("missing", ReplicaActive, ReplicaLost) {
		t.Fatal("changed a missing replica")
	}
}
