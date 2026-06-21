package sqlite

import (
	"context"
	"fmt"
	"sort"
	"testing"

	types "github.com/davefinster/configfs/internal/proto"
)

func aclKey(a *types.ConfigAcl) string {
	return fmt.Sprintf("%s|%s|%t", a.GetAcl(), a.GetTag(), a.GetEveryone())
}

func aclKeys(acls []*types.ConfigAcl) []string {
	out := make([]string, 0, len(acls))
	for _, a := range acls {
		out = append(out, aclKey(a))
	}
	sort.Strings(out)
	return out
}

func aclRowCount(t *testing.T, s *Store, configID string) int {
	t.Helper()
	var n int
	if err := s.db.Get(&n, "SELECT COUNT(*) FROM config_acl WHERE config_id = ?", configID); err != nil {
		t.Fatalf("counting config_acl rows: %v", err)
	}
	return n
}

func collectConfigs(d *types.Directory) map[string]*types.Config {
	out := map[string]*types.Config{}
	for _, c := range d.GetConfigs() {
		out[c.FullPath()] = c
	}
	for _, sub := range d.GetDirectories() {
		for k, v := range collectConfigs(sub) {
			out[k] = v
		}
	}
	return out
}

func TestUpsertPersistsAndLoadsACLs(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	mkdirAll(t, s, "/a/b")

	saved, err := s.Upsert(ctx, &types.Config{
		Name: "cfg",
		Path: "/a/b",
		Acls: []*types.ConfigAcl{
			{Acl: types.Acl_READ, Tag: "tag:reader"},
			{Acl: types.Acl_WRITE, Everyone: true},
		},
		ContentSize: 3,
		Content:     []byte("xyz"),
	})
	if err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	got, err := s.GetConfigByID(ctx, saved.GetId(), false, "")
	if err != nil {
		t.Fatalf("GetConfigByID: %v", err)
	}
	want := []string{
		aclKey(&types.ConfigAcl{Acl: types.Acl_READ, Tag: "tag:reader"}),
		aclKey(&types.ConfigAcl{Acl: types.Acl_WRITE, Everyone: true}),
	}
	sort.Strings(want)
	if gotKeys := aclKeys(got.GetAcls()); fmt.Sprint(gotKeys) != fmt.Sprint(want) {
		t.Errorf("loaded acls = %v, want %v", gotKeys, want)
	}
	if n := aclRowCount(t, s, saved.GetId()); n != 2 {
		t.Errorf("config_acl row count = %d, want 2", n)
	}
}

func TestTreeForACLTagsFiltersByReadAccess(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	mkdirAll(t, s, "/x")

	mustUpsert := func(name string, acls []*types.ConfigAcl) {
		t.Helper()
		if _, err := s.Upsert(ctx, &types.Config{
			Name:        name,
			Path:        "/x",
			Acls:        acls,
			ContentSize: 1,
			Content:     []byte("z"),
		}); err != nil {
			t.Fatalf("Upsert(%s): %v", name, err)
		}
	}

	mustUpsert("open", []*types.ConfigAcl{{Acl: types.Acl_READ, Everyone: true}})
	mustUpsert("team", []*types.ConfigAcl{{Acl: types.Acl_READ, Tag: "tag:team"}})
	mustUpsert("writer", []*types.ConfigAcl{{Acl: types.Acl_WRITE, Tag: "tag:ops"}})
	mustUpsert("locked", nil)

	cases := []struct {
		name string
		tags []string
		want []string
	}{
		{"no tags sees only everyone", nil, []string{"/x/open"}},
		{"team tag", []string{"tag:team"}, []string{"/x/open", "/x/team"}},
		{"ops tag sees write-implies-read", []string{"tag:ops"}, []string{"/x/open", "/x/writer"}},
		{"unrelated tag", []string{"tag:nobody"}, []string{"/x/open"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tree, err := s.TreeForACLTags(ctx, tc.tags, "")
			if err != nil {
				t.Fatalf("TreeForACLTags: %v", err)
			}
			got := []string{}
			for path := range collectConfigs(tree) {
				got = append(got, path)
			}
			sort.Strings(got)
			sort.Strings(tc.want)
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Errorf("visible configs = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestUpsertReplacesACLs(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	mkdirAll(t, s, "/a")

	saved, err := s.Upsert(ctx, &types.Config{
		Name:        "cfg",
		Path:        "/a",
		Acls:        []*types.ConfigAcl{{Acl: types.Acl_READ, Tag: "tag:a"}},
		ContentSize: 1,
		Content:     []byte("1"),
	})
	if err != nil {
		t.Fatalf("create Upsert: %v", err)
	}

	if _, err := s.Upsert(ctx, &types.Config{
		Id:          saved.GetId(),
		Name:        "cfg",
		Path:        "/a",
		Acls:        []*types.ConfigAcl{{Acl: types.Acl_WRITE, Everyone: true}},
		ContentSize: 1,
		Content:     []byte("2"),
	}); err != nil {
		t.Fatalf("update Upsert: %v", err)
	}

	got, err := s.GetConfigByID(ctx, saved.GetId(), false, "")
	if err != nil {
		t.Fatalf("GetConfigByID: %v", err)
	}
	want := []string{aclKey(&types.ConfigAcl{Acl: types.Acl_WRITE, Everyone: true})}
	if gotKeys := aclKeys(got.GetAcls()); fmt.Sprint(gotKeys) != fmt.Sprint(want) {
		t.Errorf("acls after update = %v, want %v", gotKeys, want)
	}
	if n := aclRowCount(t, s, saved.GetId()); n != 1 {
		t.Errorf("config_acl row count after update = %d, want 1 (stale rows not cleaned)", n)
	}
}

func TestDeleteRemovesACLs(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	mkdirAll(t, s, "/a")

	saved, err := s.Upsert(ctx, &types.Config{
		Name: "cfg",
		Path: "/a",
		Acls: []*types.ConfigAcl{
			{Acl: types.Acl_READ, Tag: "tag:a"},
			{Acl: types.Acl_WRITE, Everyone: true},
		},
		ContentSize: 1,
		Content:     []byte("1"),
	})
	if err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	if err := s.Delete(ctx, saved.GetId()); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if n := aclRowCount(t, s, saved.GetId()); n != 0 {
		t.Errorf("config_acl rows after delete = %d, want 0 (orphaned acl rows)", n)
	}
	got, err := s.GetConfigByID(ctx, saved.GetId(), false, "")
	if err != nil {
		t.Fatalf("GetConfigByID after delete: %v", err)
	}
	if got != nil {
		t.Errorf("config still present after delete: %+v", got)
	}
}
