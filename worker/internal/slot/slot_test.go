package slot

import (
	"path/filepath"
	"testing"
)

func TestClaimTakesLowestFreeSlot(t *testing.T) {
	root := t.TempDir()

	a, err := Claim(root)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Release()
	b, err := Claim(root)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Release()

	if a.Index != 0 || b.Index != 1 {
		t.Fatalf("indexes = %d, %d; want 0, 1", a.Index, b.Index)
	}
	if a.Dir != filepath.Join(root, "slot-0") || b.Dir != filepath.Join(root, "slot-1") {
		t.Fatalf("dirs = %s, %s", a.Dir, b.Dir)
	}

	// Releasing slot 0 makes it the next one claimed.
	if err := a.Release(); err != nil {
		t.Fatal(err)
	}
	c, err := Claim(root)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Release()
	if c.Index != 0 {
		t.Fatalf("after release, claimed slot %d; want 0", c.Index)
	}
	if err := a.Release(); err != nil {
		t.Fatalf("second Release: %v", err)
	}
}
