package sqlite

import (
	"context"
	"fmt"
	"strings"
	"testing"

	types "github.com/davefinster/configfs/internal/proto"
)

func dirACLRowCount(t *testing.T, s *Store, directoryID string) int {
	t.Helper()
	var n int
	if err := s.db.Get(&n, "SELECT COUNT(*) FROM directory_acl WHERE directory_id = ?", directoryID); err != nil {
		t.Fatalf("counting directory_acl rows: %v", err)
	}
	return n
}

// TestCreateDirectoryPersistsACLsAndRejectsDuplicate covers the happy path
// (directory created with its ACLs, reloadable) and that re-creating the same
// full path is an error.
func TestCreateDirectoryPersistsACLsAndRejectsDuplicate(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	acls := []*types.ConfigAcl{
		{Acl: types.Acl_READ, Tag: "tag:reader"},
		{Acl: types.Acl_WRITE, Everyone: true},
	}
	created, err := s.CreateDirectory(ctx, &types.Directory{Name: "team", Path: "/org", Acls: acls})
	if err != nil {
		t.Fatalf("CreateDirectory: %v", err)
	}
	if created.FullPath() != "/org/team" {
		t.Errorf("created full path = %q, want /org/team", created.FullPath())
	}
	want := aclKeys(acls)
	if got := aclKeys(created.GetAcls()); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("created acls = %v, want %v", got, want)
	}

	reloaded, err := s.GetDirectoryByID(ctx, created.GetId())
	if err != nil {
		t.Fatalf("GetDirectoryByID: %v", err)
	}
	if got := aclKeys(reloaded.GetAcls()); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("reloaded acls = %v, want %v", got, want)
	}

	if _, err := s.CreateDirectory(ctx, &types.Directory{Name: "team", Path: "/org", Acls: acls}); err == nil {
		t.Errorf("expected an error creating a directory that already exists")
	}
}

// TestCreateDirectoryTopLevel confirms a top-level directory can be created with
// either an empty or "/" parent path, and lands at the root.
func TestCreateDirectoryTopLevel(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	for i, parent := range []string{"", "/"} {
		name := fmt.Sprintf("top%d", i)
		created, err := s.CreateDirectory(ctx, &types.Directory{Name: name, Path: parent})
		if err != nil {
			t.Fatalf("CreateDirectory(parent=%q): %v", parent, err)
		}
		if created.FullPath() != "/"+name {
			t.Errorf("created full path = %q, want %q", created.FullPath(), "/"+name)
		}
		if created.GetPath() != "/" {
			t.Errorf("top-level directory parent path = %q, want /", created.GetPath())
		}
	}
}

// TestUpdateDirectoryReplacesACLs confirms UpdateDirectory swaps the full ACL set
// and rejects an attempt to change the immutable name/path.
func TestUpdateDirectoryReplacesACLs(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	created, err := s.CreateDirectory(ctx, &types.Directory{
		Name: "d", Path: "/", Acls: []*types.ConfigAcl{{Acl: types.Acl_READ, Tag: "tag:old"}},
	})
	if err != nil {
		t.Fatalf("CreateDirectory: %v", err)
	}

	updated, err := s.UpdateDirectory(ctx, &types.Directory{
		Id: created.GetId(), Acls: []*types.ConfigAcl{{Acl: types.Acl_WRITE, Tag: "tag:new"}},
	})
	if err != nil {
		t.Fatalf("UpdateDirectory: %v", err)
	}
	want := []string{aclKey(&types.ConfigAcl{Acl: types.Acl_WRITE, Tag: "tag:new"})}
	if got := aclKeys(updated.GetAcls()); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("updated acls = %v, want %v", got, want)
	}
	if n := dirACLRowCount(t, s, created.GetId()); n != 1 {
		t.Errorf("directory_acl row count after update = %d, want 1", n)
	}

	if _, err := s.UpdateDirectory(ctx, &types.Directory{Id: created.GetId(), Name: "wrong"}); err == nil {
		t.Errorf("expected an error updating a directory with a mismatched name")
	}
	if _, err := s.UpdateDirectory(ctx, &types.Directory{Id: "99999"}); err == nil {
		t.Errorf("expected an error updating a non-existent directory")
	}
}

// TestDeleteDirectoryEmptyVsNonEmpty confirms an empty directory deletes (and its
// ACL rows go with it) while a directory holding a config or a subdirectory is
// refused.
func TestDeleteDirectoryEmptyVsNonEmpty(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	// A directory holding a config cannot be deleted.
	withConfig, err := s.CreateDirectory(ctx, &types.Directory{Name: "hasconfig", Path: "/"})
	if err != nil {
		t.Fatalf("CreateDirectory hasconfig: %v", err)
	}
	if _, err := s.Upsert(ctx, &types.Config{
		Name: "cfg", Path: "/hasconfig", Acls: []*types.ConfigAcl{{Acl: types.Acl_WRITE, Everyone: true}},
		ContentSize: 1, Content: []byte("z"),
	}); err != nil {
		t.Fatalf("Upsert config into /hasconfig: %v", err)
	}
	if err := s.DeleteDirectory(ctx, withConfig.GetId()); err == nil {
		t.Errorf("expected an error deleting a directory that still contains a config")
	}

	// A directory holding a subdirectory cannot be deleted.
	parent, err := s.CreateDirectory(ctx, &types.Directory{Name: "parent", Path: "/"})
	if err != nil {
		t.Fatalf("CreateDirectory parent: %v", err)
	}
	if _, err := s.CreateDirectory(ctx, &types.Directory{Name: "child", Path: "/parent"}); err != nil {
		t.Fatalf("CreateDirectory child: %v", err)
	}
	if err := s.DeleteDirectory(ctx, parent.GetId()); err == nil {
		t.Errorf("expected an error deleting a directory that still contains a subdirectory")
	}

	// An empty directory (with ACLs) deletes, taking its ACL rows with it.
	empty, err := s.CreateDirectory(ctx, &types.Directory{
		Name: "empty", Path: "/", Acls: []*types.ConfigAcl{{Acl: types.Acl_WRITE, Everyone: true}},
	})
	if err != nil {
		t.Fatalf("CreateDirectory empty: %v", err)
	}
	if n := dirACLRowCount(t, s, empty.GetId()); n != 1 {
		t.Fatalf("precondition: empty directory should have 1 acl row, got %d", n)
	}
	if err := s.DeleteDirectory(ctx, empty.GetId()); err != nil {
		t.Fatalf("DeleteDirectory empty: %v", err)
	}
	gone, err := s.GetDirectoryByID(ctx, empty.GetId())
	if err != nil {
		t.Fatalf("GetDirectoryByID after delete: %v", err)
	}
	if gone != nil {
		t.Errorf("directory still present after delete: %+v", gone)
	}
	if n := dirACLRowCount(t, s, empty.GetId()); n != 0 {
		t.Errorf("directory_acl rows after delete = %d, want 0", n)
	}
}

// TestDirectoryChainResolvesAncestorsWithACLs confirms DirectoryChain returns the
// full ancestor chain (each with its ACLs) and reports a missing leaf.
func TestDirectoryChainResolvesAncestorsWithACLs(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	if _, err := s.CreateDirectory(ctx, &types.Directory{
		Name: "c", Path: "/a/b", Acls: []*types.ConfigAcl{{Acl: types.Acl_READ, Tag: "tag:x"}},
	}); err != nil {
		t.Fatalf("CreateDirectory: %v", err)
	}

	chain, missing, err := s.DirectoryChain(ctx, "/a/b/c")
	if err != nil {
		t.Fatalf("DirectoryChain: %v", err)
	}
	if len(missing) != 0 {
		t.Errorf("missing = %v, want none", missing)
	}
	byPath := map[string]*types.Directory{}
	for _, d := range chain {
		byPath[d.FullPath()] = d
	}
	for _, want := range []string{"/a", "/a/b", "/a/b/c"} {
		d, ok := byPath[want]
		if !ok {
			t.Errorf("chain missing %q (have %v)", want, byPath)
			continue
		}
		if len(d.GetAcls()) != 1 || d.GetAcls()[0].GetTag() != "tag:x" {
			t.Errorf("directory %q acls = %+v, want a single tag:x entry", want, d.GetAcls())
		}
	}

	// The root resolves to an empty chain with nothing missing.
	rootChain, rootMissing, err := s.DirectoryChain(ctx, "/")
	if err != nil {
		t.Fatalf("DirectoryChain(root): %v", err)
	}
	if len(rootChain) != 0 || len(rootMissing) != 0 {
		t.Errorf("root chain = %v missing = %v, want both empty", rootChain, rootMissing)
	}

	// A non-existent leaf is reported missing.
	_, missing, err = s.DirectoryChain(ctx, "/a/b/nope")
	if err != nil {
		t.Fatalf("DirectoryChain(missing): %v", err)
	}
	if len(missing) != 1 || missing[0] != "/a/b/nope" {
		t.Errorf("missing = %v, want [/a/b/nope]", missing)
	}
}

// TestUpsertRequiresExistingDirectory pins the SetConfig-side behaviour at the
// store layer: a config written into a directory that does not exist is an error,
// and succeeds once the directory has been created.
func TestUpsertRequiresExistingDirectory(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	cfg := &types.Config{
		Name: "cfg", Path: "/missing", Acls: []*types.ConfigAcl{{Acl: types.Acl_WRITE, Everyone: true}},
		ContentSize: 1, Content: []byte("z"),
	}
	if _, err := s.Upsert(ctx, cfg); err == nil {
		t.Fatalf("expected an error writing a config into a non-existent directory")
	} else if !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("error = %q, want it to mention the directory does not exist", err.Error())
	}

	if _, err := s.CreateDirectory(ctx, &types.Directory{Name: "missing", Path: "/"}); err != nil {
		t.Fatalf("CreateDirectory: %v", err)
	}
	if _, err := s.Upsert(ctx, cfg); err != nil {
		t.Errorf("Upsert after creating the directory should succeed, got %v", err)
	}

	// Root-level configs need no directory and are always accepted.
	if _, err := s.Upsert(ctx, &types.Config{
		Name: "rootcfg", Path: "/", Acls: []*types.ConfigAcl{{Acl: types.Acl_WRITE, Everyone: true}},
		ContentSize: 1, Content: []byte("z"),
	}); err != nil {
		t.Errorf("root-level Upsert should succeed, got %v", err)
	}
}

// TestTreeForACLTagsHierarchicalFiltering confirms a restrictive ancestor
// directory hides configs beneath it even when the config's own ACL would permit
// the caller, while a transparent (no-ACL) directory does not.
func TestTreeForACLTagsHierarchicalFiltering(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	everyoneRead := []*types.ConfigAcl{{Acl: types.Acl_READ, Everyone: true}}

	// /open is transparent (no ACL); /locked grants READ only to tag:insider.
	if _, err := s.CreateDirectory(ctx, &types.Directory{Name: "open", Path: "/"}); err != nil {
		t.Fatalf("CreateDirectory /open: %v", err)
	}
	if _, err := s.CreateDirectory(ctx, &types.Directory{
		Name: "locked", Path: "/", Acls: []*types.ConfigAcl{{Acl: types.Acl_READ, Tag: "tag:insider"}},
	}); err != nil {
		t.Fatalf("CreateDirectory /locked: %v", err)
	}

	// Both configs are readable by everyone per their OWN ACL.
	if _, err := s.Upsert(ctx, &types.Config{
		Name: "a", Path: "/open", Acls: everyoneRead, ContentSize: 1, Content: []byte("z"),
	}); err != nil {
		t.Fatalf("Upsert /open/a: %v", err)
	}
	if _, err := s.Upsert(ctx, &types.Config{
		Name: "b", Path: "/locked", Acls: everyoneRead, ContentSize: 1, Content: []byte("z"),
	}); err != nil {
		t.Fatalf("Upsert /locked/b: %v", err)
	}

	// An outsider sees only the config under the transparent directory; the locked
	// ancestor hides /locked/b despite its everyone-READ config ACL.
	outsider, err := s.TreeForACLTags(ctx, []string{"tag:outsider"}, "")
	if err != nil {
		t.Fatalf("TreeForACLTags(outsider): %v", err)
	}
	got := collectConfigs(outsider)
	if _, ok := got["/open/a"]; !ok {
		t.Errorf("outsider should see /open/a; got %v", keysOf(got))
	}
	if _, ok := got["/locked/b"]; ok {
		t.Errorf("outsider must not see /locked/b (locked ancestor); got %v", keysOf(got))
	}
	// The locked directory must not be exposed in the tree either.
	if directoryPresent(outsider, "/locked") {
		t.Errorf("outsider tree should not contain the /locked directory")
	}

	// An insider (carrying tag:insider) sees both.
	insider, err := s.TreeForACLTags(ctx, []string{"tag:insider"}, "")
	if err != nil {
		t.Fatalf("TreeForACLTags(insider): %v", err)
	}
	gotIn := collectConfigs(insider)
	for _, want := range []string{"/open/a", "/locked/b"} {
		if _, ok := gotIn[want]; !ok {
			t.Errorf("insider should see %q; got %v", want, keysOf(gotIn))
		}
	}
}

// TestTreeForACLTagsSurfacesEmptyReadableDirectories confirms List now shows
// directories the caller can read even when they hold no configs (so a freshly
// mkdir'd directory appears), while still hiding directories whose ACL — or an
// ancestor's — withholds READ.
func TestTreeForACLTagsSurfacesEmptyReadableDirectories(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	if _, err := s.CreateDirectory(ctx, &types.Directory{Name: "empty", Path: "/"}); err != nil {
		t.Fatalf("CreateDirectory /empty: %v", err)
	}
	if _, err := s.CreateDirectory(ctx, &types.Directory{
		Name: "private", Path: "/", Acls: []*types.ConfigAcl{{Acl: types.Acl_READ, Tag: "tag:insider"}},
	}); err != nil {
		t.Fatalf("CreateDirectory /private: %v", err)
	}
	// A transparent child under the restricted parent: visible only to those who
	// can read the parent (hierarchical), even though the child itself is open.
	if _, err := s.CreateDirectory(ctx, &types.Directory{Name: "child", Path: "/private"}); err != nil {
		t.Fatalf("CreateDirectory /private/child: %v", err)
	}

	outsider, err := s.TreeForACLTags(ctx, []string{"tag:outsider"}, "")
	if err != nil {
		t.Fatalf("TreeForACLTags(outsider): %v", err)
	}
	if !directoryPresent(outsider, "/empty") {
		t.Errorf("outsider should see the transparent empty directory /empty")
	}
	if directoryPresent(outsider, "/private") {
		t.Errorf("outsider must not see /private (READ restricted)")
	}
	if directoryPresent(outsider, "/private/child") {
		t.Errorf("outsider must not see /private/child (ancestor READ restricted)")
	}

	insider, err := s.TreeForACLTags(ctx, []string{"tag:insider"}, "")
	if err != nil {
		t.Fatalf("TreeForACLTags(insider): %v", err)
	}
	for _, want := range []string{"/empty", "/private", "/private/child"} {
		if !directoryPresent(insider, want) {
			t.Errorf("insider should see %q", want)
		}
	}
}

func keysOf(m map[string]*types.Config) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func directoryPresent(d *types.Directory, fullPath string) bool {
	if d.FullPath() == fullPath {
		return true
	}
	for _, sub := range d.GetDirectories() {
		if directoryPresent(sub, fullPath) {
			return true
		}
	}
	return false
}
