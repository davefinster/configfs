package api

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	types "github.com/davefinster/configfs/internal/proto"
	"github.com/davefinster/configfs/internal/store/sqlite"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"tailscale.com/client/tailscale/apitype"
	"tailscale.com/tailcfg"
)

func newTestServer(t *testing.T, createAllowed []string) (*Server, *sqlite.Store) {
	t.Helper()
	db, err := sqlx.Connect("sqlite3", filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("connect sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store, err := sqlite.NewStore(db)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	return NewServer(store, createAllowed), store
}

// ctxWithTags builds an incoming-metadata context as the auth interceptor would
// leave it, carrying the caller's resolved identities under acl_tags.
func ctxWithTags(identities ...string) context.Context {
	md := metadata.MD{}
	md["acl_tags"] = identities
	return metadata.NewIncomingContext(context.Background(), md)
}

// createReq builds a SetConfig request for a root-level config. The root needs no
// directory (it is synthesised and transparent), keeping these create/update
// allow-list tests independent of the directory existence and hierarchy gates,
// which are covered separately below.
func createReq(id, name string, acls ...*types.ConfigAcl) *types.SetConfigRequest {
	return &types.SetConfigRequest{Config: &types.Config{
		Id:          id,
		Name:        name,
		Path:        "",
		Acls:        acls,
		ContentSize: 1,
		Content:     []byte("z"),
	}}
}

func TestIdentitiesFromWhoIs(t *testing.T) {
	cases := []struct {
		name string
		resp *apitype.WhoIsResponse
		want []string
	}{
		{"nil response", nil, nil},
		{
			"tagged node uses tags, not the tagged-devices user",
			&apitype.WhoIsResponse{
				Node:        &tailcfg.Node{Tags: []string{"tag:a", "tag:b"}},
				UserProfile: &tailcfg.UserProfile{LoginName: "tagged-devices"},
			},
			[]string{"tag:a", "tag:b"},
		},
		{
			"untagged node uses user:<login>",
			&apitype.WhoIsResponse{
				Node:        &tailcfg.Node{},
				UserProfile: &tailcfg.UserProfile{LoginName: "alice@example.com"},
			},
			[]string{"user:alice@example.com"},
		},
		{"no node and no user yields nothing", &apitype.WhoIsResponse{}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := identitiesFromWhoIs(tc.resp); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("identitiesFromWhoIs = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestSetConfigCreateRequiresAllowedIdentity(t *testing.T) {
	s, _ := newTestServer(t, []string{"tag:creator", "user:alice@example.com"})
	everyoneWrite := &types.ConfigAcl{Acl: types.Acl_WRITE, Everyone: true}

	if _, err := s.SetConfig(ctxWithTags("tag:creator"), createReq("", "by-tag", everyoneWrite)); err != nil {
		t.Errorf("create by allowed tag should succeed: %v", err)
	}
	if _, err := s.SetConfig(ctxWithTags("user:alice@example.com"), createReq("", "by-user", everyoneWrite)); err != nil {
		t.Errorf("create by allowed user should succeed: %v", err)
	}

	if _, err := s.SetConfig(ctxWithTags("tag:other"), createReq("", "nope", everyoneWrite)); status.Code(err) != codes.PermissionDenied {
		t.Errorf("create by disallowed tag: want PermissionDenied, got %v", err)
	}
	if _, err := s.SetConfig(ctxWithTags("user:mallory@example.com"), createReq("", "nope2", everyoneWrite)); status.Code(err) != codes.PermissionDenied {
		t.Errorf("create by disallowed user: want PermissionDenied, got %v", err)
	}
	if _, err := s.SetConfig(ctxWithTags(), createReq("", "nope3", everyoneWrite)); status.Code(err) != codes.PermissionDenied {
		t.Errorf("create with no identity: want PermissionDenied, got %v", err)
	}
}

func TestSetConfigCreateOpenWhenUnconfigured(t *testing.T) {
	s, _ := newTestServer(t, nil)
	everyoneWrite := &types.ConfigAcl{Acl: types.Acl_WRITE, Everyone: true}
	if _, err := s.SetConfig(ctxWithTags(), createReq("", "anon", everyoneWrite)); err != nil {
		t.Errorf("create with no restriction configured should succeed: %v", err)
	}
}

// TestSetConfigUpdateIgnoresCreateList is the crux of the feature: updates must
// be governed by the per-config ACLs, never the server-wide create allow-list.
func TestSetConfigUpdateIgnoresCreateList(t *testing.T) {
	s, _ := newTestServer(t, []string{"tag:creator"})

	// Created by an allowed creator; only tag:editor gets WRITE on the config.
	created, err := s.SetConfig(ctxWithTags("tag:creator"),
		createReq("", "cfg", &types.ConfigAcl{Acl: types.Acl_WRITE, Tag: "tag:editor"}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	id := created.GetConfig().GetId()

	// tag:editor is NOT in the create list but holds WRITE on the config: allowed.
	if _, err := s.SetConfig(ctxWithTags("tag:editor"),
		createReq(id, "cfg", &types.ConfigAcl{Acl: types.Acl_WRITE, Tag: "tag:editor"})); err != nil {
		t.Errorf("update by per-config WRITE holder should succeed despite not being a creator: %v", err)
	}

	// tag:creator IS in the create list but has NO WRITE on the config: denied.
	if _, err := s.SetConfig(ctxWithTags("tag:creator"),
		createReq(id, "cfg", &types.ConfigAcl{Acl: types.Acl_WRITE, Tag: "tag:editor"})); status.Code(err) != codes.PermissionDenied {
		t.Errorf("update by creator lacking per-config WRITE: want PermissionDenied, got %v", err)
	}
}

// TestSetConfigMissingDirectory is the headline behaviour change: writing a config
// into a directory that does not exist is rejected (FailedPrecondition) rather
// than silently materialising the directory.
func TestSetConfigMissingDirectory(t *testing.T) {
	s, _ := newTestServer(t, nil)
	everyoneWrite := &types.ConfigAcl{Acl: types.Acl_WRITE, Everyone: true}

	req := &types.SetConfigRequest{Config: &types.Config{
		Name: "cfg", Path: "/nope", Acls: []*types.ConfigAcl{everyoneWrite}, ContentSize: 1, Content: []byte("z"),
	}}
	if _, err := s.SetConfig(ctxWithTags(), req); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("SetConfig into a missing directory: got code %v (err %v), want FailedPrecondition", status.Code(err), err)
	}

	// After the directory is created the same write succeeds.
	if _, err := s.CreateDirectory(ctxWithTags(), &types.CreateDirectoryRequest{Directory: &types.Directory{Name: "nope", Path: "/"}}); err != nil {
		t.Fatalf("CreateDirectory: %v", err)
	}
	if _, err := s.SetConfig(ctxWithTags(), req); err != nil {
		t.Fatalf("SetConfig after creating the directory: %v", err)
	}
}

// TestSetConfigRequiresDirectoryWrite confirms the hierarchical write gate: even a
// permitted creator cannot write a config into a directory whose ACL withholds
// WRITE from them.
func TestSetConfigRequiresDirectoryWrite(t *testing.T) {
	s, store := newTestServer(t, nil)
	if _, err := store.CreateDirectory(context.Background(), &types.Directory{
		Name: "team", Path: "/", Acls: []*types.ConfigAcl{{Acl: types.Acl_WRITE, Tag: "tag:owner"}},
	}); err != nil {
		t.Fatalf("CreateDirectory: %v", err)
	}
	cfg := &types.SetConfigRequest{Config: &types.Config{
		Name: "cfg", Path: "/team",
		Acls: []*types.ConfigAcl{{Acl: types.Acl_WRITE, Everyone: true}}, ContentSize: 1, Content: []byte("x"),
	}}

	if _, err := s.SetConfig(ctxWithTags("tag:other"), cfg); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("outsider SetConfig: got %v, want PermissionDenied", status.Code(err))
	}
	if _, err := s.SetConfig(ctxWithTags("tag:owner"), cfg); err != nil {
		t.Fatalf("owner SetConfig: %v", err)
	}
}

// TestGetConfigRequiresAncestorRead confirms the hierarchical read gate: a config
// whose own ACL grants everyone READ is still hidden behind a directory that
// withholds READ.
func TestGetConfigRequiresAncestorRead(t *testing.T) {
	s, store := newTestServer(t, nil)
	bg := context.Background()
	if _, err := store.CreateDirectory(bg, &types.Directory{
		Name: "locked", Path: "/", Acls: []*types.ConfigAcl{{Acl: types.Acl_READ, Tag: "tag:insider"}},
	}); err != nil {
		t.Fatalf("CreateDirectory: %v", err)
	}
	saved, err := store.Upsert(bg, &types.Config{
		Name: "secret", Path: "/locked",
		Acls: []*types.ConfigAcl{{Acl: types.Acl_READ, Everyone: true}}, ContentSize: 1, Content: []byte("s"),
	})
	if err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	if _, err := s.GetConfig(ctxWithTags("tag:outsider"), &types.GetConfigRequest{Id: saved.GetId()}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("outsider GetConfig: got %v, want PermissionDenied", status.Code(err))
	}
	resp, err := s.GetConfig(ctxWithTags("tag:insider"), &types.GetConfigRequest{Id: saved.GetId()})
	if err != nil {
		t.Fatalf("insider GetConfig: %v", err)
	}
	if string(resp.GetConfig().GetContent()) != "s" {
		t.Errorf("content = %q, want s", resp.GetConfig().GetContent())
	}
}

// TestDirectoryCRUDHandlers exercises the create/get/update/delete handlers and
// their ACL gates end to end.
func TestDirectoryCRUDHandlers(t *testing.T) {
	s, _ := newTestServer(t, nil)
	admin := ctxWithTags("tag:admin")

	created, err := s.CreateDirectory(admin, &types.CreateDirectoryRequest{Directory: &types.Directory{
		Name: "proj", Path: "/", Acls: []*types.ConfigAcl{{Acl: types.Acl_WRITE, Tag: "tag:admin"}},
	}})
	if err != nil {
		t.Fatalf("CreateDirectory: %v", err)
	}
	id := created.GetDirectory().GetId()

	// admin holds WRITE (which implies READ) → Get succeeds.
	got, err := s.GetDirectory(admin, &types.GetDirectoryRequest{Id: id})
	if err != nil {
		t.Fatalf("GetDirectory: %v", err)
	}
	if got.GetDirectory().GetName() != "proj" {
		t.Errorf("name = %q, want proj", got.GetDirectory().GetName())
	}

	// An unrelated identity cannot read the directory (it carries an ACL).
	if _, err := s.GetDirectory(ctxWithTags("tag:nobody"), &types.GetDirectoryRequest{Id: id}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("outsider GetDirectory: got %v, want PermissionDenied", status.Code(err))
	}

	// admin can replace the ACLs, opening it up to everyone.
	updated, err := s.UpdateDirectory(admin, &types.UpdateDirectoryRequest{Directory: &types.Directory{
		Id: id, Acls: []*types.ConfigAcl{{Acl: types.Acl_WRITE, Everyone: true}},
	}})
	if err != nil {
		t.Fatalf("UpdateDirectory: %v", err)
	}
	if acls := updated.GetDirectory().GetAcls(); len(acls) != 1 || !acls[0].GetEveryone() {
		t.Errorf("updated acls = %+v, want a single everyone WRITE entry", acls)
	}

	// Now that it grants everyone WRITE, any caller can delete the (empty) dir.
	if _, err := s.DeleteDirectory(ctxWithTags("tag:whoever"), &types.DeleteDirectoryRequest{Id: id}); err != nil {
		t.Fatalf("DeleteDirectory: %v", err)
	}
	if _, err := s.GetDirectory(admin, &types.GetDirectoryRequest{Id: id}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetDirectory after delete: got %v, want NotFound", status.Code(err))
	}
}

// TestDeleteDirectoryNonEmpty confirms the handler surfaces the store's not-empty
// refusal as FailedPrecondition.
func TestDeleteDirectoryNonEmpty(t *testing.T) {
	s, store := newTestServer(t, nil)
	bg := context.Background()
	if _, err := store.CreateDirectory(bg, &types.Directory{
		Name: "full", Path: "/", Acls: []*types.ConfigAcl{{Acl: types.Acl_WRITE, Everyone: true}},
	}); err != nil {
		t.Fatalf("CreateDirectory: %v", err)
	}
	if _, err := store.Upsert(bg, &types.Config{
		Name: "c", Path: "/full",
		Acls: []*types.ConfigAcl{{Acl: types.Acl_WRITE, Everyone: true}}, ContentSize: 1, Content: []byte("z"),
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	chain, _, err := store.DirectoryChain(bg, "/full")
	if err != nil || len(chain) != 1 {
		t.Fatalf("DirectoryChain: %v (chain %+v)", err, chain)
	}

	if _, err := s.DeleteDirectory(ctxWithTags(), &types.DeleteDirectoryRequest{Id: chain[0].GetId()}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DeleteDirectory of a non-empty dir: got %v, want FailedPrecondition", status.Code(err))
	}
}

// TestCreateDirectoryCreateGate confirms directory creation honours the
// server-wide create allow-list, just like config creation.
func TestCreateDirectoryCreateGate(t *testing.T) {
	s, _ := newTestServer(t, []string{"tag:creator"})

	if _, err := s.CreateDirectory(ctxWithTags("tag:other"), &types.CreateDirectoryRequest{Directory: &types.Directory{Name: "x", Path: "/"}}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("non-creator CreateDirectory: got %v, want PermissionDenied", status.Code(err))
	}
	if _, err := s.CreateDirectory(ctxWithTags("tag:creator"), &types.CreateDirectoryRequest{Directory: &types.Directory{Name: "x", Path: "/"}}); err != nil {
		t.Fatalf("creator CreateDirectory: %v", err)
	}
}

// TestCreateDirectoryRequiresParentWrite confirms creating a subdirectory under an
// existing directory requires WRITE on that parent.
func TestCreateDirectoryRequiresParentWrite(t *testing.T) {
	s, store := newTestServer(t, nil)
	if _, err := store.CreateDirectory(context.Background(), &types.Directory{
		Name: "parent", Path: "/", Acls: []*types.ConfigAcl{{Acl: types.Acl_WRITE, Tag: "tag:owner"}},
	}); err != nil {
		t.Fatalf("CreateDirectory parent: %v", err)
	}

	if _, err := s.CreateDirectory(ctxWithTags("tag:other"), &types.CreateDirectoryRequest{Directory: &types.Directory{Name: "child", Path: "/parent"}}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("outsider sub-create: got %v, want PermissionDenied", status.Code(err))
	}
	if _, err := s.CreateDirectory(ctxWithTags("tag:owner"), &types.CreateDirectoryRequest{Directory: &types.Directory{Name: "child", Path: "/parent"}}); err != nil {
		t.Fatalf("owner sub-create: %v", err)
	}
}
