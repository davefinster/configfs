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

func newTestServer(t *testing.T, createAllowed []string) *Server {
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
	return NewServer(store, createAllowed)
}

// ctxWithTags builds an incoming-metadata context as the auth interceptor would
// leave it, carrying the caller's resolved identities under acl_tags.
func ctxWithTags(identities ...string) context.Context {
	md := metadata.MD{}
	md["acl_tags"] = identities
	return metadata.NewIncomingContext(context.Background(), md)
}

func createReq(id, name string, acls ...*types.ConfigAcl) *types.SetConfigRequest {
	return &types.SetConfigRequest{Config: &types.Config{
		Id:          id,
		Name:        name,
		Path:        "/x",
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
	s := newTestServer(t, []string{"tag:creator", "user:alice@example.com"})
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
	s := newTestServer(t, nil)
	everyoneWrite := &types.ConfigAcl{Acl: types.Acl_WRITE, Everyone: true}
	if _, err := s.SetConfig(ctxWithTags(), createReq("", "anon", everyoneWrite)); err != nil {
		t.Errorf("create with no restriction configured should succeed: %v", err)
	}
}

// TestSetConfigUpdateIgnoresCreateList is the crux of the feature: updates must
// be governed by the per-config ACLs, never the server-wide create allow-list.
func TestSetConfigUpdateIgnoresCreateList(t *testing.T) {
	s := newTestServer(t, []string{"tag:creator"})

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
