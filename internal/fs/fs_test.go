package fs

import (
	"context"
	"testing"
	"time"

	"bazil.org/fuse"
	types "github.com/davefinster/configfs/internal/proto"
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
