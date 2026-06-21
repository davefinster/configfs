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

// fakeDirClient satisfies types.ConfigFSServerClient and wires the directory
// mutation RPCs (plus List, which Refresh calls) so the mkdir/rmdir paths can be
// exercised without a real server.
type fakeDirClient struct {
	types.ConfigFSServerClient
	createDirectory func(context.Context, *types.CreateDirectoryRequest) (*types.CreateDirectoryResponse, error)
	deleteDirectory func(context.Context, *types.DeleteDirectoryRequest) (*types.DeleteDirectoryResponse, error)
	deleteConfig    func(context.Context, *types.DeleteConfigRequest) (*types.DeleteConfigResponse, error)
}

func (f fakeDirClient) CreateDirectory(ctx context.Context, in *types.CreateDirectoryRequest, _ ...grpc.CallOption) (*types.CreateDirectoryResponse, error) {
	return f.createDirectory(ctx, in)
}

func (f fakeDirClient) DeleteDirectory(ctx context.Context, in *types.DeleteDirectoryRequest, _ ...grpc.CallOption) (*types.DeleteDirectoryResponse, error) {
	return f.deleteDirectory(ctx, in)
}

func (f fakeDirClient) DeleteConfig(ctx context.Context, in *types.DeleteConfigRequest, _ ...grpc.CallOption) (*types.DeleteConfigResponse, error) {
	return f.deleteConfig(ctx, in)
}

func (f fakeDirClient) List(_ context.Context, _ *types.ListRequest, _ ...grpc.CallOption) (*types.ListResponse, error) {
	// Refresh after a mutation; an empty tree is enough for these tests.
	return &types.ListResponse{Top: &types.Directory{Id: "0", Name: "/", Path: ""}}, nil
}

// TestDirMkdirInheritsParentACLs confirms mkdir creates the child under the
// parent's path carrying the parent directory's ACLs.
func TestDirMkdirInheritsParentACLs(t *testing.T) {
	parentACLs := []*types.ConfigAcl{
		{Acl: types.Acl_READ, Tag: "tag:x"},
		{Acl: types.Acl_WRITE, Tag: "tag:y"},
	}
	var got *types.CreateDirectoryRequest
	snap := &FilesystemSnapshot{
		pathDirectory: map[string]*types.Directory{
			"/team": {Id: "5", Name: "team", Path: "/", Acls: parentACLs},
		},
		client: fakeDirClient{
			createDirectory: func(_ context.Context, in *types.CreateDirectoryRequest) (*types.CreateDirectoryResponse, error) {
				got = in
				d := in.GetDirectory()
				return &types.CreateDirectoryResponse{Directory: &types.Directory{
					Id: "9", Name: d.GetName(), Path: d.GetPath(), Acls: d.GetAcls(),
				}}, nil
			},
		},
	}
	d := &Dir{path: "/team", snapshot: snap}

	node, err := d.Mkdir(context.Background(), &fuse.MkdirRequest{Name: "sub"})
	if err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	if got == nil {
		t.Fatal("CreateDirectory was not called")
	}
	if got.GetDirectory().GetName() != "sub" || got.GetDirectory().GetPath() != "/team" {
		t.Errorf("CreateDirectory got name=%q path=%q, want sub /team", got.GetDirectory().GetName(), got.GetDirectory().GetPath())
	}
	gotACLs := got.GetDirectory().GetAcls()
	if len(gotACLs) != len(parentACLs) {
		t.Fatalf("inherited %d acls, want %d", len(gotACLs), len(parentACLs))
	}
	for i, want := range parentACLs {
		if gotACLs[i].GetAcl() != want.GetAcl() || gotACLs[i].GetTag() != want.GetTag() || gotACLs[i].GetEveryone() != want.GetEveryone() {
			t.Errorf("inherited acl[%d] = %+v, want %+v", i, gotACLs[i], want)
		}
	}
	dir, ok := node.(*Dir)
	if !ok {
		t.Fatalf("Mkdir returned %T, want *Dir", node)
	}
	if dir.path != "/team/sub" {
		t.Errorf("new dir path = %q, want /team/sub", dir.path)
	}
}

// TestDirRemoveDispatch confirms Remove routes rmdir (req.Dir) to DeleteDirectory
// by id and unlink to DeleteConfig. The two cases use independent snapshots
// because each Remove triggers a Refresh that rebuilds the snapshot's maps.
func TestDirRemoveDispatch(t *testing.T) {
	t.Run("rmdir deletes the directory by id", func(t *testing.T) {
		var deletedDir, deletedConfig string
		snap := &FilesystemSnapshot{
			pathDirectory: map[string]*types.Directory{
				"/team": {Id: "5", Name: "team", Path: "/", Directories: []*types.Directory{
					{Id: "9", Name: "sub", Path: "/team"},
				}},
			},
			client: fakeDirClient{
				deleteDirectory: func(_ context.Context, in *types.DeleteDirectoryRequest) (*types.DeleteDirectoryResponse, error) {
					deletedDir = in.GetId()
					return &types.DeleteDirectoryResponse{}, nil
				},
				deleteConfig: func(_ context.Context, in *types.DeleteConfigRequest) (*types.DeleteConfigResponse, error) {
					deletedConfig = in.GetId()
					return &types.DeleteConfigResponse{}, nil
				},
			},
		}
		d := &Dir{path: "/team", snapshot: snap}
		if err := d.Remove(context.Background(), &fuse.RemoveRequest{Name: "sub", Dir: true}); err != nil {
			t.Fatalf("rmdir: %v", err)
		}
		if deletedDir != "9" {
			t.Errorf("rmdir deleted directory id %q, want 9", deletedDir)
		}
		if deletedConfig != "" {
			t.Errorf("rmdir should not delete a config, but deleted %q", deletedConfig)
		}
	})

	t.Run("unlink deletes the config by id", func(t *testing.T) {
		var deletedConfig string
		snap := &FilesystemSnapshot{
			fullPathConfig: map[string]*types.Config{
				"/team/cfg": {Id: "42", Name: "cfg", Path: "/team"},
			},
			client: fakeDirClient{
				deleteConfig: func(_ context.Context, in *types.DeleteConfigRequest) (*types.DeleteConfigResponse, error) {
					deletedConfig = in.GetId()
					return &types.DeleteConfigResponse{}, nil
				},
			},
		}
		d := &Dir{path: "/team", snapshot: snap}
		if err := d.Remove(context.Background(), &fuse.RemoveRequest{Name: "cfg", Dir: false}); err != nil {
			t.Fatalf("unlink: %v", err)
		}
		if deletedConfig != "42" {
			t.Errorf("unlink deleted config id %q, want 42", deletedConfig)
		}
	})
}
