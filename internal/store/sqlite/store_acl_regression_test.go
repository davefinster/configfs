package sqlite

import (
	"context"
	"testing"

	types "github.com/davefinster/configfs/internal/proto"
)

// TestUpsertACLOnlyUpdatePreservesContent covers an update that changes only the
// ACLs and omits content (nil). Content must be preserved and the new ACLs
// applied — previously the nil content forced an UPDATE ... SET content = NULL
// that violated the NOT NULL constraint and rolled back the ACL change.
func TestUpsertACLOnlyUpdatePreservesContent(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	saved, err := s.Upsert(ctx, &types.Config{
		Name:        "cfg",
		Path:        "/a",
		Acls:        []*types.ConfigAcl{{Acl: types.Acl_READ, Tag: "tag:old"}},
		ContentSize: 3,
		Content:     []byte("xyz"),
	})
	if err != nil {
		t.Fatalf("create Upsert: %v", err)
	}

	if _, err := s.Upsert(ctx, &types.Config{
		Id:   saved.GetId(),
		Name: "cfg",
		Path: "/a",
		Acls: []*types.ConfigAcl{{Acl: types.Acl_WRITE, Tag: "tag:new"}},
		// Content omitted (nil): manage access without resending the blob.
	}); err != nil {
		t.Fatalf("acl-only Upsert: %v", err)
	}

	got, err := s.GetConfigByID(ctx, saved.GetId(), true)
	if err != nil {
		t.Fatalf("GetConfigByID: %v", err)
	}
	if string(got.GetContent()) != "xyz" {
		t.Errorf("content = %q, want preserved %q", got.GetContent(), "xyz")
	}
	if got.GetContentSize() != 3 {
		t.Errorf("content_size = %d, want preserved 3", got.GetContentSize())
	}
	want := aclKey(&types.ConfigAcl{Acl: types.Acl_WRITE, Tag: "tag:new"})
	if keys := aclKeys(got.GetAcls()); len(keys) != 1 || keys[0] != want {
		t.Errorf("acls = %v, want [%s]", keys, want)
	}
}

// TestGetConfigByIDDenyByDefault pins deny-by-default on the single-config load
// path that GetConfig relies on (independent of the tree filter).
func TestGetConfigByIDDenyByDefault(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	saved, err := s.Upsert(ctx, &types.Config{
		Name: "cfg", Path: "/a", Acls: nil, ContentSize: 1, Content: []byte("z"),
	})
	if err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	got, err := s.GetConfigByID(ctx, saved.GetId(), false)
	if err != nil {
		t.Fatalf("GetConfigByID: %v", err)
	}
	if got == nil {
		t.Fatalf("config should exist")
	}
	if len(got.GetAcls()) != 0 {
		t.Errorf("expected 0 acls, got %d", len(got.GetAcls()))
	}
	if got.Allows([]string{"tag:any"}, types.Acl_READ) {
		t.Errorf("config with no acls must not be readable")
	}
	if got.Allows(nil, types.Acl_READ) {
		t.Errorf("config with no acls must not be readable with nil tags")
	}
	if got.Allows([]string{"tag:any"}, types.Acl_WRITE) {
		t.Errorf("config with no acls must not be writable")
	}
}

// TestInsertConfigACLsSkipsUnknown confirms UNKNOWN_ACL entries are dropped on
// insert while valid entries alongside them are kept.
func TestInsertConfigACLsSkipsUnknown(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	saved, err := s.Upsert(ctx, &types.Config{
		Name: "cfg", Path: "/a",
		Acls: []*types.ConfigAcl{
			{Acl: types.Acl_UNKNOWN_ACL, Everyone: true},
			{Acl: types.Acl_READ, Tag: "tag:r"},
		},
		ContentSize: 1, Content: []byte("z"),
	})
	if err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if n := aclRowCount(t, s, saved.GetId()); n != 1 {
		t.Errorf("config_acl row count = %d, want 1 (UNKNOWN_ACL skipped)", n)
	}
	got, err := s.GetConfigByID(ctx, saved.GetId(), false)
	if err != nil {
		t.Fatalf("GetConfigByID: %v", err)
	}
	want := aclKey(&types.ConfigAcl{Acl: types.Acl_READ, Tag: "tag:r"})
	if keys := aclKeys(got.GetAcls()); len(keys) != 1 || keys[0] != want {
		t.Errorf("acls = %v, want [%s]", keys, want)
	}
}

// TestUnknownOnlyACLNotReadable confirms a config whose only ACL is UNKNOWN_ACL
// persists zero ACL rows and is therefore unreadable (deny by default).
func TestUnknownOnlyACLNotReadable(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	saved, err := s.Upsert(ctx, &types.Config{
		Name: "cfg", Path: "/a",
		Acls:        []*types.ConfigAcl{{Acl: types.Acl_UNKNOWN_ACL, Everyone: true}},
		ContentSize: 1, Content: []byte("z"),
	})
	if err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if n := aclRowCount(t, s, saved.GetId()); n != 0 {
		t.Errorf("config_acl row count = %d, want 0", n)
	}
	got, err := s.GetConfigByID(ctx, saved.GetId(), false)
	if err != nil {
		t.Fatalf("GetConfigByID: %v", err)
	}
	if got.Allows(nil, types.Acl_READ) {
		t.Errorf("config whose only acl is UNKNOWN must not be readable")
	}
}

// TestTreeForACLTagsRootLevelConfig guards against a config stored at the root
// path ("/" or "") breaking TreeForACLTags for every caller (the root has no
// directory row; it is synthesised).
func TestTreeForACLTagsRootLevelConfig(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	everyoneRead := []*types.ConfigAcl{{Acl: types.Acl_READ, Everyone: true}}
	mk := func(name, path string) {
		t.Helper()
		if _, err := s.Upsert(ctx, &types.Config{
			Name: name, Path: path, Acls: everyoneRead, ContentSize: 1, Content: []byte("z"),
		}); err != nil {
			t.Fatalf("Upsert(%s @ %q): %v", name, path, err)
		}
	}
	mk("slashroot", "/")
	mk("emptyroot", "")
	mk("nested", "/x")

	tree, err := s.TreeForACLTags(ctx, []string{"tag:any"}, "")
	if err != nil {
		t.Fatalf("TreeForACLTags must not fail with a root-level config: %v", err)
	}

	rootNames := map[string]bool{}
	for _, c := range tree.GetConfigs() {
		rootNames[c.GetName()] = true
	}
	for _, want := range []string{"slashroot", "emptyroot"} {
		if !rootNames[want] {
			t.Errorf("root-level config %q not attached to tree root; got %v", want, rootNames)
		}
	}

	nestedFound := false
	for _, d := range tree.GetDirectories() {
		if d.GetName() != "x" {
			continue
		}
		for _, c := range d.GetConfigs() {
			if c.GetName() == "nested" {
				nestedFound = true
			}
		}
	}
	if !nestedFound {
		t.Errorf("nested config /x/nested missing from tree")
	}
}
