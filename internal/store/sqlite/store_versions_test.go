package sqlite

import (
	"context"
	"testing"

	types "github.com/davefinster/configfs/internal/proto"
)

// contentRowCount reports how many config_content rows exist for a config.
func contentRowCount(t *testing.T, s *Store, configID string) int {
	t.Helper()
	var n int
	if err := s.db.Get(&n, "SELECT COUNT(*) FROM config_content WHERE config_id = ?", configID); err != nil {
		t.Fatalf("counting config_content rows: %v", err)
	}
	return n
}

// TestContentUpdateSpawnsNewVersion is the core of content versioning: each
// content-changing update writes a new config_content row and repoints
// config.current_content_id at it, while prior versions remain fetchable by id.
func TestContentUpdateSpawnsNewVersion(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	mkdirAll(t, s, "/a")
	everyone := []*types.ConfigAcl{{Acl: types.Acl_WRITE, Everyone: true}}
	created, err := s.Upsert(ctx, &types.Config{
		Name: "cfg", Path: "/a", Acls: everyone, ContentSize: 2, Content: []byte("v1"),
	})
	if err != nil {
		t.Fatalf("create Upsert: %v", err)
	}
	if created.GetCurrentContentId() == "" {
		t.Fatal("created config should carry a current_content_id")
	}
	// content_size is reported even on the metadata-only path (SetConfig response),
	// sourced from the new version row.
	if created.GetContentSize() != 2 {
		t.Errorf("created content_size = %d, want 2", created.GetContentSize())
	}
	if n := contentRowCount(t, s, created.GetId()); n != 1 {
		t.Fatalf("config_content rows after create = %d, want 1", n)
	}
	v1 := created.GetCurrentContentId()

	// Update with content of a different length: a fresh version row is written,
	// current repoints, and the new size belongs to the new version.
	updated, err := s.Upsert(ctx, &types.Config{
		Id: created.GetId(), Name: "cfg", Path: "/a", Acls: everyone, ContentSize: 9, Content: []byte("version-2"),
	})
	if err != nil {
		t.Fatalf("content Upsert: %v", err)
	}
	v2 := updated.GetCurrentContentId()
	if v2 == "" || v2 == v1 {
		t.Fatalf("current_content_id should advance to a new version: v1=%q v2=%q", v1, v2)
	}
	if updated.GetContentSize() != 9 {
		t.Errorf("updated content_size = %d, want 9", updated.GetContentSize())
	}
	if n := contentRowCount(t, s, created.GetId()); n != 2 {
		t.Fatalf("config_content rows after content update = %d, want 2 (history retained)", n)
	}

	// Default fetch (no version) returns the latest content and its size.
	latest, err := s.GetConfigByID(ctx, created.GetId(), true, "")
	if err != nil {
		t.Fatalf("GetConfigByID latest: %v", err)
	}
	if string(latest.GetContent()) != "version-2" {
		t.Errorf("latest content = %q, want %q", latest.GetContent(), "version-2")
	}
	if latest.GetContentSize() != 9 {
		t.Errorf("latest content_size = %d, want 9", latest.GetContentSize())
	}
	if latest.GetCurrentContentId() != v2 {
		t.Errorf("latest current_content_id = %q, want %q", latest.GetCurrentContentId(), v2)
	}

	// Explicitly fetching the old version returns its own content and its own size,
	// not the current version's.
	old, err := s.GetConfigByID(ctx, created.GetId(), true, v1)
	if err != nil {
		t.Fatalf("GetConfigByID v1: %v", err)
	}
	if string(old.GetContent()) != "v1" {
		t.Errorf("v1 content = %q, want %q", old.GetContent(), "v1")
	}
	if old.GetContentSize() != 2 {
		t.Errorf("v1 content_size = %d, want 2 (the old version's size, not the current)", old.GetContentSize())
	}
	// current_content_id always reflects the live version, regardless of which
	// version's content was loaded.
	if old.GetCurrentContentId() != v2 {
		t.Errorf("current_content_id when loading v1 = %q, want %q", old.GetCurrentContentId(), v2)
	}
}

// TestACLOnlyUpdateDoesNotVersionContent confirms an update that omits content
// (nil) neither writes a new version nor moves current_content_id.
func TestACLOnlyUpdateDoesNotVersionContent(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	mkdirAll(t, s, "/a")
	created, err := s.Upsert(ctx, &types.Config{
		Name: "cfg", Path: "/a",
		Acls: []*types.ConfigAcl{{Acl: types.Acl_WRITE, Everyone: true}}, ContentSize: 2, Content: []byte("v1"),
	})
	if err != nil {
		t.Fatalf("create Upsert: %v", err)
	}
	v1 := created.GetCurrentContentId()

	updated, err := s.Upsert(ctx, &types.Config{
		Id: created.GetId(), Name: "cfg", Path: "/a",
		Acls: []*types.ConfigAcl{{Acl: types.Acl_READ, Everyone: true}}, // content omitted
	})
	if err != nil {
		t.Fatalf("acl-only Upsert: %v", err)
	}
	if updated.GetCurrentContentId() != v1 {
		t.Errorf("current_content_id moved on acl-only update: %q -> %q", v1, updated.GetCurrentContentId())
	}
	if n := contentRowCount(t, s, created.GetId()); n != 1 {
		t.Errorf("config_content rows after acl-only update = %d, want 1 (no new version)", n)
	}
}

// TestGetConfigVersionIsolatedByConfig confirms a version id belonging to one
// config cannot be read through a different config's id, and that an unknown
// version reports not-found (nil) rather than leaking another config's content.
func TestGetConfigVersionIsolatedByConfig(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	mkdirAll(t, s, "/d")
	everyone := []*types.ConfigAcl{{Acl: types.Acl_WRITE, Everyone: true}}
	a, err := s.Upsert(ctx, &types.Config{Name: "a", Path: "/d", Acls: everyone, ContentSize: 2, Content: []byte("aa")})
	if err != nil {
		t.Fatalf("Upsert a: %v", err)
	}
	b, err := s.Upsert(ctx, &types.Config{Name: "b", Path: "/d", Acls: everyone, ContentSize: 2, Content: []byte("bb")})
	if err != nil {
		t.Fatalf("Upsert b: %v", err)
	}

	// b's current version id, requested through a's config id, must not resolve.
	got, err := s.GetConfigByID(ctx, a.GetId(), true, b.GetCurrentContentId())
	if err != nil {
		t.Fatalf("GetConfigByID cross-config: %v", err)
	}
	if got != nil {
		t.Errorf("reading another config's version returned content %q, want nil", got.GetContent())
	}
}

// TestDeleteRemovesAllVersions confirms deleting a config removes every one of
// its content versions, not just the current row.
func TestDeleteRemovesAllVersions(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	mkdirAll(t, s, "/a")
	everyone := []*types.ConfigAcl{{Acl: types.Acl_WRITE, Everyone: true}}
	created, err := s.Upsert(ctx, &types.Config{Name: "cfg", Path: "/a", Acls: everyone, ContentSize: 2, Content: []byte("v1")})
	if err != nil {
		t.Fatalf("create Upsert: %v", err)
	}
	if _, err := s.Upsert(ctx, &types.Config{
		Id: created.GetId(), Name: "cfg", Path: "/a", Acls: everyone, ContentSize: 2, Content: []byte("v2"),
	}); err != nil {
		t.Fatalf("content Upsert: %v", err)
	}
	if n := contentRowCount(t, s, created.GetId()); n != 2 {
		t.Fatalf("precondition: want 2 versions, got %d", n)
	}

	if err := s.Delete(ctx, created.GetId()); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if n := contentRowCount(t, s, created.GetId()); n != 0 {
		t.Errorf("config_content rows after delete = %d, want 0 (all versions removed)", n)
	}
	gone, err := s.GetConfigByID(ctx, created.GetId(), true, "")
	if err != nil {
		t.Fatalf("GetConfigByID after delete: %v", err)
	}
	if gone != nil {
		t.Errorf("config still present after delete: %+v", gone)
	}
}
