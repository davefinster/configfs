package sqlite

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	types "github.com/davefinster/configfs/internal/proto"
)

// TestConcurrentUpsertsDoNotLock is a regression test for concurrent SetConfig
// calls failing with "database is locked". NewStore caps the SQLite pool at a
// single connection so concurrent write transactions queue and wait for one
// another instead of racing for the write lock; before that, the loser of the
// lock-upgrade race would intermittently return SQLITE_BUSY ("database is
// locked"). Every Upsert here must succeed.
func TestConcurrentUpsertsDoNotLock(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	// Seed a config that half the workers repeatedly update, giving maximum
	// write-write contention on a single row.
	seeded, err := s.Upsert(ctx, &types.Config{
		Name:        "shared",
		Path:        "/shared",
		Acls:        []*types.ConfigAcl{{Acl: types.Acl_WRITE, Tag: "tag:test"}},
		ContentSize: 1,
		Content:     []byte("0"),
	})
	if err != nil {
		t.Fatalf("seed Upsert: %v", err)
	}

	const (
		workers      = 16
		opsPerWorker = 8
	)

	var wg sync.WaitGroup
	errs := make(chan error, workers*opsPerWorker)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for op := 0; op < opsPerWorker; op++ {
				var err error
				if w%2 == 0 {
					// Update the shared config: pure write-write contention.
					content := []byte(fmt.Sprintf("w%d-op%d", w, op))
					_, err = s.Upsert(ctx, &types.Config{
						Id:          seeded.GetId(),
						Name:        seeded.GetName(),
						Path:        seeded.GetPath(),
						Acls:        []*types.ConfigAcl{{Acl: types.Acl_WRITE, Tag: "tag:test"}},
						ContentSize: uint64(len(content)),
						Content:     content,
					})
				} else {
					// Insert a distinct new config, exercising ensurePath + insert
					// concurrently with the updates above.
					content := []byte("x")
					_, err = s.Upsert(ctx, &types.Config{
						Name:        fmt.Sprintf("cfg-%d-%d", w, op),
						Path:        fmt.Sprintf("/workers/%d", w),
						Acls:        []*types.ConfigAcl{{Acl: types.Acl_READ, Tag: "tag:test"}},
						ContentSize: uint64(len(content)),
						Content:     content,
					})
				}
				if err != nil {
					errs <- err
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		if strings.Contains(err.Error(), "database is locked") {
			t.Fatalf("concurrent Upsert hit lock contention: %v", err)
		}
		t.Fatalf("concurrent Upsert failed: %v", err)
	}
}
