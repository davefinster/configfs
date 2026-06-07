package sqlite

import (
	"context"
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

func TestUpsertCreatesThreeDeepDirectoryHierarchy(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	if _, err := s.Upsert(ctx, &types.Config{
		Name:        "cfg",
		Path:        "/a/b/c",
		Acls:        []*types.ConfigAcl{{Acl: types.Acl_READ, Tag: "tag:test"}},
		ContentSize: 3,
		Content:     []byte("xyz"),
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
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
}

func TestUpsertReusesExistingDirectoriesAndCreatesNew(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	if _, err := s.Upsert(ctx, &types.Config{
		Name:        "first",
		Path:        "/a/b/c",
		Acls:        []*types.ConfigAcl{{Acl: types.Acl_READ, Tag: "tag:test"}},
		ContentSize: 3,
		Content:     []byte("xyz"),
	}); err != nil {
		t.Fatalf("first Upsert: %v", err)
	}
	initial := loadDirectories(t, s)

	if _, err := s.Upsert(ctx, &types.Config{
		Name:        "second",
		Path:        "/a/x/y",
		Acls:        []*types.ConfigAcl{{Acl: types.Acl_READ, Tag: "tag:test"}},
		ContentSize: 3,
		Content:     []byte("xyz"),
	}); err != nil {
		t.Fatalf("second Upsert: %v", err)
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
