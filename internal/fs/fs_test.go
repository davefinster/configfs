package fs

import (
	"context"
	"sync"
	"testing"
	"time"

	"bazil.org/fuse"
	types "github.com/davefinster/configfs/internal/proto"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func snapshotWithConfig(c *types.Config) *FilesystemSnapshot {
	return &FilesystemSnapshot{
		idConfig: map[string]*types.Config{c.GetId(): c},
	}
}

func TestConfigAttrSurfacesTimestamps(t *testing.T) {
	created := time.Date(2026, 6, 1, 9, 0, 0, 123456789, time.UTC)
	updated := created.Add(time.Hour)
	f := Config{id: "7", snapshot: snapshotWithConfig(&types.Config{
		Id:          "7",
		Name:        "cfg",
		ContentSize: 3,
		CreatedAt:   timestamppb.New(created),
		UpdatedAt:   timestamppb.New(updated),
	})}

	a := fuse.Attr{}
	if err := f.Attr(context.Background(), &a); err != nil {
		t.Fatalf("Attr: %v", err)
	}
	for name, got := range map[string]time.Time{"Mtime": a.Mtime, "Ctime": a.Ctime, "Atime": a.Atime} {
		if !got.Equal(updated) {
			t.Errorf("%s = %v, want %v", name, got, updated)
		}
	}
	if a.Inode != 7 || a.Size != 3 {
		t.Errorf("Inode/Size = %d/%d, want 7/3", a.Inode, a.Size)
	}
}

func TestConfigAttrLeavesTimesZeroWhenTimestampsUnset(t *testing.T) {
	f := Config{id: "7", snapshot: snapshotWithConfig(&types.Config{
		Id:          "7",
		Name:        "legacy",
		ContentSize: 3,
	})}

	a := fuse.Attr{}
	if err := f.Attr(context.Background(), &a); err != nil {
		t.Fatalf("Attr: %v", err)
	}
	for name, got := range map[string]time.Time{"Mtime": a.Mtime, "Ctime": a.Ctime, "Atime": a.Atime} {
		if !got.IsZero() {
			t.Errorf("%s = %v, want zero for a config without timestamps", name, got)
		}
	}
}

func TestAccessTimes(t *testing.T) {
	// Nil tracker is a safe, empty no-op (nodes built without one, e.g. in tests).
	var nilAT *accessTimes
	if _, ok := nilAT.get("x"); ok {
		t.Errorf("nil accessTimes.get reported a value")
	}
	nilAT.touch("x") // must not panic

	at := newAccessTimes()
	if _, ok := at.get("7"); ok {
		t.Errorf("get returned a value before any touch")
	}

	t1 := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	at.now = func() time.Time { return t1 }
	at.touch("7")
	if got, ok := at.get("7"); !ok || !got.Equal(t1) {
		t.Errorf("after first touch get = (%v, %v), want (%v, true)", got, ok, t1)
	}

	// A later access overwrites the recorded time.
	t2 := t1.Add(time.Hour)
	at.now = func() time.Time { return t2 }
	at.touch("7")
	if got, _ := at.get("7"); !got.Equal(t2) {
		t.Errorf("after second touch get = %v, want %v", got, t2)
	}

	// Concurrent touch/get must be data-race free (run with -race).
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			at.touch("concurrent")
			at.get("concurrent")
		})
	}
	wg.Wait()
}

func TestConfigAttrUsesRecordedAccessTime(t *testing.T) {
	updated := time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC)
	accessed := updated.Add(2 * time.Hour)

	at := newAccessTimes()
	at.now = func() time.Time { return accessed }
	f := Config{id: "7", access: at, snapshot: snapshotWithConfig(&types.Config{
		Id:          "7",
		Name:        "cfg",
		ContentSize: 3,
		UpdatedAt:   timestamppb.New(updated),
	})}

	// Before any access, atime falls back to updated_at.
	a := fuse.Attr{}
	if err := f.Attr(context.Background(), &a); err != nil {
		t.Fatalf("Attr: %v", err)
	}
	if !a.Atime.Equal(updated) {
		t.Errorf("Atime before access = %v, want updated_at %v", a.Atime, updated)
	}

	// After an access, atime reflects the recorded access time while mtime/ctime
	// stay anchored to updated_at.
	at.touch("7")
	a = fuse.Attr{}
	if err := f.Attr(context.Background(), &a); err != nil {
		t.Fatalf("Attr: %v", err)
	}
	if !a.Atime.Equal(accessed) {
		t.Errorf("Atime after access = %v, want %v", a.Atime, accessed)
	}
	if !a.Mtime.Equal(updated) || !a.Ctime.Equal(updated) {
		t.Errorf("Mtime/Ctime = %v/%v, want updated_at %v", a.Mtime, a.Ctime, updated)
	}
}

// fakeConfigClient satisfies types.ConfigFSServerClient; only GetConfig is wired
// (the rest are nil and would panic if called, which these tests never do).
type fakeConfigClient struct {
	types.ConfigFSServerClient
	getConfig func(context.Context, *types.GetConfigRequest) (*types.GetConfigResponse, error)
}

func (f fakeConfigClient) GetConfig(ctx context.Context, in *types.GetConfigRequest, _ ...grpc.CallOption) (*types.GetConfigResponse, error) {
	return f.getConfig(ctx, in)
}

func TestConfigReadAllRecordsAccess(t *testing.T) {
	accessed := time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC)
	cfg := &types.Config{Id: "7", Name: "cfg", ContentSize: 3}
	snap := &FilesystemSnapshot{
		idConfig: map[string]*types.Config{"7": cfg},
		client: fakeConfigClient{
			getConfig: func(_ context.Context, in *types.GetConfigRequest) (*types.GetConfigResponse, error) {
				if in.GetId() != "7" {
					t.Errorf("GetConfig id = %q, want 7", in.GetId())
				}
				return &types.GetConfigResponse{Config: &types.Config{Id: "7", Content: []byte("abc")}}, nil
			},
		},
	}
	at := newAccessTimes()
	at.now = func() time.Time { return accessed }
	f := Config{id: "7", access: at, snapshot: snap}

	if _, ok := at.get("7"); ok {
		t.Fatalf("access recorded before any read")
	}
	content, err := f.ReadAll(context.Background())
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(content) != "abc" {
		t.Errorf("ReadAll content = %q, want abc", content)
	}
	if got, ok := at.get("7"); !ok || !got.Equal(accessed) {
		t.Errorf("after ReadAll get = (%v, %v), want (%v, true)", got, ok, accessed)
	}
}
