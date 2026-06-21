package sqlite

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	types "github.com/davefinster/configfs/internal/proto"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

type testDirectoryRow struct {
	ID       int    `db:"id"`
	Name     string `db:"name"`
	Path     string `db:"path"`
	ParentID *int   `db:"parent_id"`
}

func (r testDirectoryRow) fullPath() string {
	if r.Path == "/" {
		return "/" + r.Name
	}
	return r.Path + "/" + r.Name
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	db, err := sqlx.Connect("sqlite3", filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("connect to sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s, err := NewStore(db)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	return s
}

func loadDirectories(t *testing.T, s *Store) map[string]testDirectoryRow {
	t.Helper()
	rows := []testDirectoryRow{}
	if err := s.db.Select(&rows, "SELECT id, name, path, parent_id FROM directory"); err != nil {
		t.Fatalf("load directories: %v", err)
	}
	out := make(map[string]testDirectoryRow, len(rows))
	for _, r := range rows {
		out[r.fullPath()] = r
	}
	return out
}

// mkdirAll ensures every directory along fullPath exists (transparent, no ACL) so
// a config can be written into it. Directories are no longer auto-created on the
// config write path, so tests must materialise them first; this is the test-side
// stand-in for the mkdir -p that Upsert used to perform implicitly.
func mkdirAll(t *testing.T, s *Store, fullPath string) {
	t.Helper()
	if _, err := s.ensureDirectories(context.Background(), fullPath, nil); err != nil {
		t.Fatalf("ensureDirectories(%q): %v", fullPath, err)
	}
}

func TestCreateDirectoryCreatesThreeDeepHierarchy(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	acls := []*types.ConfigAcl{{Acl: types.Acl_READ, Tag: "tag:test"}}
	// Creating /a/b/c auto-creates the missing ancestors /a and /a/b too, each
	// carrying the supplied acls (option: apply the ACL to every created level).
	leaf, err := s.CreateDirectory(ctx, &types.Directory{
		Name: "c",
		Path: "/a/b",
		Acls: acls,
	})
	if err != nil {
		t.Fatalf("CreateDirectory: %v", err)
	}
	if leaf.FullPath() != "/a/b/c" {
		t.Errorf("created leaf full path = %q, want /a/b/c", leaf.FullPath())
	}

	dirs := loadDirectories(t, s)
	if len(dirs) != 3 {
		t.Fatalf("expected 3 directory rows, got %d: %+v", len(dirs), dirs)
	}
	for _, want := range []string{"/a", "/a/b", "/a/b/c"} {
		if _, ok := dirs[want]; !ok {
			t.Errorf("missing directory %q (have %+v)", want, dirs)
		}
	}

	a, b, c := dirs["/a"], dirs["/a/b"], dirs["/a/b/c"]
	if a.Path != "/" || a.Name != "a" {
		t.Errorf("/a row path/name mismatch: %+v", a)
	}
	if b.Path != "/a" || b.Name != "b" {
		t.Errorf("/a/b row path/name mismatch: %+v", b)
	}
	if c.Path != "/a/b" || c.Name != "c" {
		t.Errorf("/a/b/c row path/name mismatch: %+v", c)
	}
	if a.ParentID != nil {
		t.Errorf("/a parent_id should be nil, got %d", *a.ParentID)
	}
	if b.ParentID == nil || *b.ParentID != a.ID {
		t.Errorf("/a/b parent_id should be %d, got %v", a.ID, b.ParentID)
	}
	if c.ParentID == nil || *c.ParentID != b.ID {
		t.Errorf("/a/b/c parent_id should be %d, got %v", b.ID, c.ParentID)
	}

	// Every created level carries the supplied ACL.
	for _, full := range []string{"/a", "/a/b", "/a/b/c"} {
		got, err := s.GetDirectoryByID(ctx, fmt.Sprintf("%d", dirs[full].ID))
		if err != nil {
			t.Fatalf("GetDirectoryByID(%s): %v", full, err)
		}
		if len(got.GetAcls()) != 1 || got.GetAcls()[0].GetTag() != "tag:test" {
			t.Errorf("directory %q acls = %+v, want a single tag:test READ entry", full, got.GetAcls())
		}
	}
}

func TestCreateDirectoryReusesExistingDirectoriesAndCreatesNew(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	if _, err := s.CreateDirectory(ctx, &types.Directory{
		Name: "c",
		Path: "/a/b",
		Acls: []*types.ConfigAcl{{Acl: types.Acl_READ, Tag: "tag:test"}},
	}); err != nil {
		t.Fatalf("first CreateDirectory: %v", err)
	}
	initial := loadDirectories(t, s)

	if _, err := s.CreateDirectory(ctx, &types.Directory{
		Name: "y",
		Path: "/a/x",
		Acls: []*types.ConfigAcl{{Acl: types.Acl_READ, Tag: "tag:test"}},
	}); err != nil {
		t.Fatalf("second CreateDirectory: %v", err)
	}
	final := loadDirectories(t, s)

	if final["/a"].ID != initial["/a"].ID {
		t.Errorf("/a was recreated: id %d -> %d", initial["/a"].ID, final["/a"].ID)
	}
	for _, want := range []string{"/a/b", "/a/b/c"} {
		if final[want].ID != initial[want].ID {
			t.Errorf("%s was recreated: id %d -> %d", want, initial[want].ID, final[want].ID)
		}
	}
	for _, want := range []string{"/a/x", "/a/x/y"} {
		if _, ok := final[want]; !ok {
			t.Errorf("missing directory %q (have %+v)", want, final)
		}
	}
	if len(final) != 5 {
		t.Errorf("expected 5 directory rows, got %d: %+v", len(final), final)
	}

	a, x, y := final["/a"], final["/a/x"], final["/a/x/y"]
	if x.ParentID == nil || *x.ParentID != a.ID {
		t.Errorf("/a/x parent_id should be %d (id of pre-existing /a), got %v", a.ID, x.ParentID)
	}
	if y.ParentID == nil || *y.ParentID != x.ID {
		t.Errorf("/a/x/y parent_id should be %d, got %v", x.ID, y.ParentID)
	}
}
