package api

import (
	"context"
	"strings"

	"github.com/davefinster/configfs/internal/log"
	types "github.com/davefinster/configfs/internal/proto"
	"github.com/davefinster/configfs/internal/store/sqlite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"tailscale.com/client/local"
	"tailscale.com/client/tailscale/apitype"
)

type Server struct {
	store *sqlite.Store
	// createAllowedTags is the optional server-wide allow-list of identities
	// (tags like "tag:foo" and users like "user:alice@example.com") permitted to
	// create new configs. Empty disables the restriction.
	createAllowedTags map[string]struct{}
	types.UnimplementedConfigFSServerServer
}

// NewServer constructs a Server. createAllowedTags gates creation of new configs
// to the listed identities (tags and "user:<login>" users); an empty list lets
// anyone create. It never affects updates or deletes, which are governed solely
// by per-config ACLs.
func NewServer(s *sqlite.Store, createAllowedTags []string) *Server {
	allowed := make(map[string]struct{}, len(createAllowedTags))
	for _, t := range createAllowedTags {
		if t = strings.TrimSpace(t); t != "" {
			allowed[t] = struct{}{}
		}
	}
	return &Server{store: s, createAllowedTags: allowed}
}

// canCreate reports whether a caller carrying the given identities may create
// new configs. An empty allow-list means the restriction is disabled.
func (s *Server) canCreate(identities []string) bool {
	if len(s.createAllowedTags) == 0 {
		return true
	}
	for _, id := range identities {
		if _, ok := s.createAllowedTags[id]; ok {
			return true
		}
	}
	return false
}

func TailscaleAuthenticationInterceptor(localClient *local.Client) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (_ any, err error) {
		p, ok := peer.FromContext(ctx)
		if !ok {
			return nil, status.Errorf(codes.Unauthenticated, "unable to obtain peer ip")
		}
		resp, err := localClient.WhoIs(ctx, p.Addr.String())
		if err != nil {
			return nil, status.Errorf(codes.Unauthenticated, "unable to resolve peer ip: %s", err.Error())
		}
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Errorf(codes.DataLoss, "unable to load request metadata")
		}
		idents := identitiesFromWhoIs(resp)
		// Overwrite any client-supplied acl_tags with the authenticated peer's
		// identities via direct map assignment rather than md.Set: Set is a no-op
		// when given zero values, so an untagged peer's forged acl_tags would
		// survive and defeat ACL enforcement entirely.
		md = md.Copy()
		md["acl_tags"] = idents
		log.InfofCtx(ctx, "Extracted identities for request: %s", strings.Join(idents, ", "))
		return handler(metadata.NewIncomingContext(ctx, md), req)
	}
}

// identitiesFromWhoIs derives the caller's identities from a WhoIs response. A
// Tailscale node is either tagged or owned by a user: a tagged node contributes
// its tags, an untagged node contributes its owner as the non-standard
// "user:<login>" pseudo-tag so users can be matched alongside tags.
func identitiesFromWhoIs(resp *apitype.WhoIsResponse) []string {
	if resp == nil {
		return nil
	}
	if resp.Node != nil && len(resp.Node.Tags) > 0 {
		return append([]string(nil), resp.Node.Tags...)
	}
	if resp.UserProfile != nil && resp.UserProfile.LoginName != "" {
		return []string{"user:" + resp.UserProfile.LoginName}
	}
	return nil
}

// StripACLTagsInterceptor removes any client-supplied acl_tags from incoming
// metadata. Install it when no trusted authenticator populates acl_tags (e.g.
// --kernel_networking) so callers cannot forge tags; only everyone-scoped
// configs remain accessible in that mode.
func StripACLTagsInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			md = md.Copy()
			delete(md, "acl_tags")
			ctx = metadata.NewIncomingContext(ctx, md)
		}
		return handler(ctx, req)
	}
}

func aclTags(ctx context.Context) []string {
	return metadata.ValueFromIncomingContext(ctx, "acl_tags")
}

func findDirectory(id string, tree *types.Directory) *types.Directory {
	for _, d := range tree.GetDirectories() {
		if d.GetId() == id {
			return d
		}
		if child := findDirectory(id, d); child != nil {
			return child
		}
	}
	return nil
}

func (s *Server) List(ctx context.Context, req *types.ListRequest) (*types.ListResponse, error) {
	prefix := ""
	var (
		directory *types.Directory
		err       error
	)
	if req.GetDirectoryId() != "" {
		directory, err = s.store.GetDirectoryByID(ctx, req.GetDirectoryId())
		if err != nil {
			return nil, status.Errorf(codes.Internal, "error fetching directory: %s", err.Error())
		}
		if directory == nil {
			return nil, status.Errorf(codes.NotFound, "unable to find directory with id %q", req.GetDirectoryId())
		}
		prefix = directory.FullPath()
	}
	log.InfofCtx(ctx, "Querying for tree with prefix %q", prefix)
	dir, err := s.store.TreeForACLTags(ctx, aclTags(ctx), prefix)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error fetching tree: %s", err.Error())
	}
	if directory != nil {
		dir = findDirectory(directory.GetId(), dir)
	}
	return &types.ListResponse{
		Top: dir,
	}, nil
}

func (s *Server) GetConfig(ctx context.Context, req *types.GetConfigRequest) (*types.GetConfigResponse, error) {
	if req.GetId() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "config id must be provided and can not be empty - got %q", req.GetId())
	}
	conf, err := s.store.GetConfigByID(ctx, req.GetId(), true)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error loading config: %s", err.Error())
	}
	if conf == nil {
		return nil, status.Errorf(codes.NotFound, "unable to find config with id %q", req.GetId())
	}
	if !conf.Allows(aclTags(ctx), types.Acl_READ) {
		return nil, status.Errorf(codes.PermissionDenied, "tags [%s] are not permitted to read configuration %q", strings.Join(aclTags(ctx), ","), req.GetId())
	}
	copy := proto.Clone(conf).(*types.Config)
	return &types.GetConfigResponse{
		Config: copy,
	}, nil
}

func (s *Server) SetConfig(ctx context.Context, req *types.SetConfigRequest) (*types.SetConfigResponse, error) {
	log.InfofCtx(ctx, "Got req: %+v", req)
	conf := req.GetConfig()
	if conf == nil {
		return nil, status.Errorf(codes.InvalidArgument, "a config must be provided")
	}
	if conf.GetId() != "" {
		// Updating an existing config is governed solely by its per-config ACLs;
		// the server-wide create allow-list never applies to updates.
		existing, err := s.store.GetConfigByID(ctx, conf.GetId(), false)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "error loading config: %s", err.Error())
		}
		if existing == nil {
			return nil, status.Errorf(codes.NotFound, "unable to find config with id %q", conf.GetId())
		}
		if !existing.Allows(aclTags(ctx), types.Acl_WRITE) {
			return nil, status.Errorf(codes.PermissionDenied, "tags [%s] are not permitted to write configuration %q", strings.Join(aclTags(ctx), ","), conf.GetId())
		}
	} else {
		// Creating a new config is gated by the optional server-wide allow-list.
		// The ACLs supplied on the new config govern who can read/write it after.
		if !s.canCreate(aclTags(ctx)) {
			return nil, status.Errorf(codes.PermissionDenied, "identities [%s] are not permitted to create new configs", strings.Join(aclTags(ctx), ","))
		}
	}
	storedConfig, err := s.store.Upsert(ctx, conf)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error persisting config: %s", err.Error())
	}
	return &types.SetConfigResponse{
		Config: storedConfig,
	}, nil
}

func (s *Server) DeleteConfig(ctx context.Context, req *types.DeleteConfigRequest) (*types.DeleteConfigResponse, error) {
	if req.GetId() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "config id must be provided and can not be empty")
	}
	existing, err := s.store.GetConfigByID(ctx, req.GetId(), false)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error loading config: %s", err.Error())
	}
	if existing == nil {
		return nil, status.Errorf(codes.NotFound, "unable to find config with id %q", req.GetId())
	}
	if !existing.Allows(aclTags(ctx), types.Acl_WRITE) {
		return nil, status.Errorf(codes.PermissionDenied, "tags [%s] are not permitted to delete configuration %q", strings.Join(aclTags(ctx), ","), req.GetId())
	}
	if err := s.store.Delete(ctx, req.GetId()); err != nil {
		return nil, status.Errorf(codes.Internal, "error deleting config: %s", err.Error())
	}
	return &types.DeleteConfigResponse{}, nil
}
