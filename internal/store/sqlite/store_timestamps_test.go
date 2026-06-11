package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	types "github.com/davefinster/configfs/internal/proto"
	"github.com/jmoiron/sqlx"
)

type timestampRow struct {
	CreatedAt int64 `db:"created_at"`
	UpdatedAt int64 `db:"updated_at"`
}

func contentTimestamps(t *testing.T, s *Store, id string) timestampRow {
	t.Helper()
	row := timestampRow{}
	if err := s.db.Get(&row, "SELECT created_at, updated_at FROM config_content WHERE id = ?", id); err != nil {
		t.Fatalf("load config_content timestamps: %v", err)
	}
	return row
}

func TestUpsertStampsCreatedAtAndUpdatedAt(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	t0 := time.Date(2026, 6, 11, 10, 0, 0, 123456789, time.UTC)
	current := t0
	s.now = func() time.Time { return current }

	created, err := s.Upsert(ctx, &types.Config{
		Name:        "cfg",
		Path:        "/a",
		Acls:        []*types.ConfigAcl{{Acl: types.Acl_WRITE, Everyone: true}},
		ContentSize: 3,
		Content:     []byte("xyz"),
	})
	if err != nil {
		t.Fatalf("create Upsert: %v", err)
	}
	if !created.GetCreatedAt().AsTime().Equal(t0) {
		t.Errorf("created_at after create = %v, want %v", created.GetCreatedAt().AsTime(), t0)
	}
	if !created.GetUpdatedAt().AsTime().Equal(t0) {
		t.Errorf("updated_at after create = %v, want %v", created.GetUpdatedAt().AsTime(), t0)
	}
	if row := contentTimestamps(t, s, created.GetId()); row.CreatedAt != t0.UnixNano() || row.UpdatedAt != t0.UnixNano() {
		t.Errorf("config_content timestamps after create = %+v, want both %d", row, t0.UnixNano())
	}

	// A content update keeps created_at and bumps updated_at on both tables.
	t1 := t0.Add(time.Minute)
	current = t1
	updated, err := s.Upsert(ctx, &types.Config{
		Id:          created.GetId(),
		Name:        "cfg",
		Path:        "/a",
		Acls:        []*types.ConfigAcl{{Acl: types.Acl_WRITE, Everyone: true}},
		ContentSize: 4,
		Content:     []byte("wxyz"),
	})
	if err != nil {
		t.Fatalf("content Upsert: %v", err)
	}
	if !updated.GetCreatedAt().AsTime().Equal(t0) {
		t.Errorf("created_at after content update = %v, want %v", updated.GetCreatedAt().AsTime(), t0)
	}
	if !updated.GetUpdatedAt().AsTime().Equal(t1) {
		t.Errorf("updated_at after content update = %v, want %v", updated.GetUpdatedAt().AsTime(), t1)
	}
	if row := contentTimestamps(t, s, created.GetId()); row.CreatedAt != t0.UnixNano() || row.UpdatedAt != t1.UnixNano() {
		t.Errorf("config_content timestamps after content update = %+v, want created %d updated %d", row, t0.UnixNano(), t1.UnixNano())
	}

	// An ACL-only update (nil content) still bumps config.updated_at but leaves
	// the untouched content blob's updated_at alone.
	t2 := t0.Add(2 * time.Minute)
	current = t2
	aclOnly, err := s.Upsert(ctx, &types.Config{
		Id:   created.GetId(),
		Name: "cfg",
		Path: "/a",
		Acls: []*types.ConfigAcl{{Acl: types.Acl_READ, Everyone: true}},
	})
	if err != nil {
		t.Fatalf("acl-only Upsert: %v", err)
	}
	if !aclOnly.GetCreatedAt().AsTime().Equal(t0) {
		t.Errorf("created_at after acl-only update = %v, want %v", aclOnly.GetCreatedAt().AsTime(), t0)
	}
	if !aclOnly.GetUpdatedAt().AsTime().Equal(t2) {
		t.Errorf("updated_at after acl-only update = %v, want %v", aclOnly.GetUpdatedAt().AsTime(), t2)
	}
	if row := contentTimestamps(t, s, created.GetId()); row.UpdatedAt != t1.UnixNano() {
		t.Errorf("config_content updated_at after acl-only update = %d, want unchanged %d", row.UpdatedAt, t1.UnixNano())
	}

	// The tree (List path) surfaces the same timestamps.
	tree, err := s.TreeForACLTags(ctx, nil, "")
	if err != nil {
		t.Fatalf("TreeForACLTags: %v", err)
	}
	if len(tree.GetDirectories()) != 1 || len(tree.GetDirectories()[0].GetConfigs()) != 1 {
		t.Fatalf("unexpected tree shape: %+v", tree)
	}
	treeConfig := tree.GetDirectories()[0].GetConfigs()[0]
	if !treeConfig.GetCreatedAt().AsTime().Equal(t0) || !treeConfig.GetUpdatedAt().AsTime().Equal(t2) {
		t.Errorf("tree config timestamps = created %v updated %v, want %v and %v",
			treeConfig.GetCreatedAt().AsTime(), treeConfig.GetUpdatedAt().AsTime(), t0, t2)
	}
}

// preTimestampSchema mirrors the schema as it was before the created_at /
// updated_at columns existed, so NewStore's ALTER TABLE migrations are
// exercised against a genuinely old database.
const preTimestampSchema = `
CREATE TABLE inode (id INTEGER PRIMARY KEY);
CREATE TABLE config (
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL,
	content_size INTEGER NOT NULL,
	path STRING NOT NULL
);
CREATE TABLE directory (
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL,
	path TEXT NOT NULL,
	parent_id INTEGER,
	FOREIGN KEY (parent_id) REFERENCES directory (id) ON DELETE CASCADE ON UPDATE NO ACTION
);
CREATE TABLE config_content (
	id INTEGER PRIMARY KEY,
	content BLOB NOT NULL,
	FOREIGN KEY (id) REFERENCES config (id) ON DELETE CASCADE ON UPDATE NO ACTION
);
CREATE TABLE config_acl (
	id INTEGER PRIMARY KEY,
	config_id INTEGER NOT NULL,
	acl INTEGER NOT NULL,
	tag TEXT NOT NULL DEFAULT '',
	everyone INTEGER NOT NULL DEFAULT 0,
	FOREIGN KEY (config_id) REFERENCES config (id) ON DELETE CASCADE ON UPDATE NO ACTION
);
`

func TestNewStoreMigratesPreTimestampDatabase(t *testing.T) {
	ctx := context.Background()
	db, err := sqlx.Connect("sqlite3", filepath.Join(t.TempDir(), "legacy.db"))
	if err != nil {
		t.Fatalf("connect to sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.MustExec(preTimestampSchema)
	db.MustExec(`INSERT INTO inode (id) VALUES (1), (2)`)
	db.MustExec(`INSERT INTO directory (id, name, path, parent_id) VALUES (1, 'a', '/', NULL)`)
	db.MustExec(`INSERT INTO config (id, name, content_size, path) VALUES (2, 'legacy', 3, '/a')`)
	db.MustExec(`INSERT INTO config_content (id, content) VALUES (2, 'xyz')`)
	db.MustExec(`INSERT INTO config_acl (config_id, acl, tag, everyone) VALUES (2, 2, '', 1)`)

	s, err := NewStore(db)
	if err != nil {
		t.Fatalf("NewStore on legacy database: %v", err)
	}
	// Running the migrations again must remain a no-op.
	if _, err := NewStore(db); err != nil {
		t.Fatalf("NewStore second run on migrated database: %v", err)
	}

	// Rows that predate the columns report unknown (unset) timestamps.
	legacy, err := s.GetConfigByID(ctx, "2", true)
	if err != nil {
		t.Fatalf("GetConfigByID: %v", err)
	}
	if legacy == nil {
		t.Fatal("legacy config not found after migration")
	}
	if legacy.GetCreatedAt() != nil || legacy.GetUpdatedAt() != nil {
		t.Errorf("legacy config timestamps should be unset, got created %v updated %v", legacy.GetCreatedAt(), legacy.GetUpdatedAt())
	}
	if string(legacy.GetContent()) != "xyz" {
		t.Errorf("legacy config content = %q, want %q", legacy.GetContent(), "xyz")
	}

	// Updating a legacy config stamps updated_at but leaves created_at unknown.
	t1 := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return t1 }
	updated, err := s.Upsert(ctx, &types.Config{
		Id:   "2",
		Name: "legacy",
		Path: "/a",
		Acls: []*types.ConfigAcl{{Acl: types.Acl_WRITE, Everyone: true}},
	})
	if err != nil {
		t.Fatalf("Upsert on legacy config: %v", err)
	}
	if updated.GetCreatedAt() != nil {
		t.Errorf("legacy created_at should stay unset after update, got %v", updated.GetCreatedAt())
	}
	if !updated.GetUpdatedAt().AsTime().Equal(t1) {
		t.Errorf("legacy updated_at = %v, want %v", updated.GetUpdatedAt().AsTime(), t1)
	}
}
