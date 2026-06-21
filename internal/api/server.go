package api

import (
	"context"
	"slices"
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

// pathReadable reports whether the caller carrying tags may traverse to path:
// every ancestor directory on the path must grant READ (a directory with no ACLs
// is transparent — see Directory.Allows). The root ("" or "/") is always
// readable. A path with a missing component is treated as not readable. This is
// the hierarchical READ gate applied on top of a config's own READ ACL.
func (s *Server) pathReadable(ctx context.Context, path string, tags []string) (bool, error) {
	if path == "" || path == "/" {
		return true, nil
	}
	chain, missing, err := s.store.DirectoryChain(ctx, path)
	if err != nil {
		return false, err
	}
	if len(missing) > 0 {
		return false, nil
	}
	for _, d := range chain {
		if !d.Allows(tags, types.Acl_READ) {
			return false, nil
		}
	}
	return true, nil
}

// containingDirectory returns the directory a config at path lives in, whether it
// exists, and any error. The root ("" or "/") has no row but always "exists" and
// is returned as a nil (transparent) directory. A nil directory grants every
// access level, so callers can pass the result straight to Directory.Allows.
func (s *Server) containingDirectory(ctx context.Context, path string) (*types.Directory, bool, error) {
	if path == "" || path == "/" {
		return nil, true, nil
	}
	chain, missing, err := s.store.DirectoryChain(ctx, path)
	if err != nil {
		return nil, false, err
	}
	if slices.Contains(missing, path) {
		return nil, false, nil
	}
	for _, d := range chain {
		if d.FullPath() == path {
			return d, true, nil
		}
	}
	return nil, false, nil
}

// directoryWritable reports whether the caller may modify the directory itself
// (update its ACLs, or delete it): every ancestor above it must grant READ (to
// traverse to it) and the directory itself must grant WRITE. dir.GetPath() is the
// parent path, so pathReadable on it checks exactly the ancestors.
func (s *Server) directoryWritable(ctx context.Context, dir *types.Directory, tags []string) (bool, error) {
	readable, err := s.pathReadable(ctx, dir.GetPath(), tags)
	if err != nil {
		return false, err
	}
	if !readable {
		return false, nil
	}
	return dir.Allows(tags, types.Acl_WRITE), nil
}

// deepestExistingAncestor returns the most specific (longest full path) directory
// in a DirectoryChain result, i.e. the deepest ancestor that already exists. It
// returns nil when the chain is empty (everything down to the root is being
// created), which Directory.Allows treats as transparent.
func deepestExistingAncestor(chain []*types.Directory) *types.Directory {
	var deepest *types.Directory
	for _, d := range chain {
		if deepest == nil || len(d.FullPath()) > len(deepest.FullPath()) {
			deepest = d
		}
	}
	return deepest
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
	conf, err := s.store.GetConfigByID(ctx, req.GetId(), true, req.GetVersion())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error loading config: %s", err.Error())
	}
	if conf == nil {
		if req.Version != nil {
			return nil, status.Errorf(codes.NotFound, "unable to find version %q of config %q", req.GetVersion(), req.GetId())
		}
		return nil, status.Errorf(codes.NotFound, "unable to find config with id %q", req.GetId())
	}
	if !conf.Allows(aclTags(ctx), types.Acl_READ) {
		return nil, status.Errorf(codes.PermissionDenied, "tags [%s] are not permitted to read configuration %q", strings.Join(aclTags(ctx), ","), req.GetId())
	}
	// Hierarchical gate: every ancestor directory must also grant READ.
	readable, err := s.pathReadable(ctx, conf.GetPath(), aclTags(ctx))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error checking directory access: %s", err.Error())
	}
	if !readable {
		return nil, status.Errorf(codes.PermissionDenied, "tags [%s] are not permitted to read the directory containing configuration %q", strings.Join(aclTags(ctx), ","), req.GetId())
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
		// Updating an existing config is governed by its per-config ACLs (plus the
		// containing directory's WRITE gate checked below); the server-wide create
		// allow-list never applies to updates.
		existing, err := s.store.GetConfigByID(ctx, conf.GetId(), false, "")
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
	// Hierarchical gate (create and update alike): the config's directory must
	// already exist — directories are no longer auto-created on the write path —
	// and must grant WRITE. The root is transparent (nil directory grants every
	// level), so root-level configs keep their pre-hierarchy behaviour.
	containing, exists, err := s.containingDirectory(ctx, conf.GetPath())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error checking directory access: %s", err.Error())
	}
	if !exists {
		return nil, status.Errorf(codes.FailedPrecondition, "directory %q does not exist; create it before writing a config into it", conf.GetPath())
	}
	if !containing.Allows(aclTags(ctx), types.Acl_WRITE) {
		return nil, status.Errorf(codes.PermissionDenied, "tags [%s] are not permitted to write into directory %q", strings.Join(aclTags(ctx), ","), conf.GetPath())
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
	existing, err := s.store.GetConfigByID(ctx, req.GetId(), false, "")
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

func (s *Server) CreateDirectory(ctx context.Context, req *types.CreateDirectoryRequest) (*types.CreateDirectoryResponse, error) {
	dir := req.GetDirectory()
	if dir == nil {
		return nil, status.Errorf(codes.InvalidArgument, "a directory must be provided")
	}
	if strings.TrimSpace(dir.GetName()) == "" {
		return nil, status.Errorf(codes.InvalidArgument, "a directory name must be provided")
	}
	// Creating a directory is gated by the same server-wide allow-list as creating
	// a config; the supplied acls govern the new directory thereafter.
	if !s.canCreate(aclTags(ctx)) {
		return nil, status.Errorf(codes.PermissionDenied, "identities [%s] are not permitted to create new directories", strings.Join(aclTags(ctx), ","))
	}
	// Hierarchical gate: require WRITE on the deepest already-existing ancestor —
	// the directory whose subtree we are extending. Missing ancestors above it are
	// auto-created. The root (empty chain) is transparent.
	if parent := dir.GetPath(); parent != "" && parent != "/" {
		chain, _, err := s.store.DirectoryChain(ctx, parent)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "error checking directory access: %s", err.Error())
		}
		if !deepestExistingAncestor(chain).Allows(aclTags(ctx), types.Acl_WRITE) {
			return nil, status.Errorf(codes.PermissionDenied, "tags [%s] are not permitted to create directories under %q", strings.Join(aclTags(ctx), ","), parent)
		}
	}
	created, err := s.store.CreateDirectory(ctx, dir)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return nil, status.Errorf(codes.AlreadyExists, "%s", err.Error())
		}
		return nil, status.Errorf(codes.Internal, "error creating directory: %s", err.Error())
	}
	return &types.CreateDirectoryResponse{Directory: created}, nil
}

func (s *Server) GetDirectory(ctx context.Context, req *types.GetDirectoryRequest) (*types.GetDirectoryResponse, error) {
	if req.GetId() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "directory id must be provided and can not be empty")
	}
	dir, err := s.store.GetDirectoryByID(ctx, req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error loading directory: %s", err.Error())
	}
	if dir == nil {
		return nil, status.Errorf(codes.NotFound, "unable to find directory with id %q", req.GetId())
	}
	// Reading a directory requires READ on it and every ancestor; FullPath() is
	// the directory's own path, whose chain includes the directory itself.
	readable, err := s.pathReadable(ctx, dir.FullPath(), aclTags(ctx))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error checking directory access: %s", err.Error())
	}
	if !readable {
		return nil, status.Errorf(codes.PermissionDenied, "tags [%s] are not permitted to read directory %q", strings.Join(aclTags(ctx), ","), req.GetId())
	}
	return &types.GetDirectoryResponse{Directory: dir}, nil
}

func (s *Server) UpdateDirectory(ctx context.Context, req *types.UpdateDirectoryRequest) (*types.UpdateDirectoryResponse, error) {
	dir := req.GetDirectory()
	if dir == nil || dir.GetId() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "a directory with an id must be provided")
	}
	existing, err := s.store.GetDirectoryByID(ctx, dir.GetId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error loading directory: %s", err.Error())
	}
	if existing == nil {
		return nil, status.Errorf(codes.NotFound, "unable to find directory with id %q", dir.GetId())
	}
	writable, err := s.directoryWritable(ctx, existing, aclTags(ctx))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error checking directory access: %s", err.Error())
	}
	if !writable {
		return nil, status.Errorf(codes.PermissionDenied, "tags [%s] are not permitted to modify directory %q", strings.Join(aclTags(ctx), ","), dir.GetId())
	}
	updated, err := s.store.UpdateDirectory(ctx, dir)
	if err != nil {
		if strings.Contains(err.Error(), "does not match") {
			return nil, status.Errorf(codes.InvalidArgument, "%s", err.Error())
		}
		return nil, status.Errorf(codes.Internal, "error updating directory: %s", err.Error())
	}
	return &types.UpdateDirectoryResponse{Directory: updated}, nil
}

func (s *Server) DeleteDirectory(ctx context.Context, req *types.DeleteDirectoryRequest) (*types.DeleteDirectoryResponse, error) {
	if req.GetId() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "directory id must be provided and can not be empty")
	}
	existing, err := s.store.GetDirectoryByID(ctx, req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error loading directory: %s", err.Error())
	}
	if existing == nil {
		return nil, status.Errorf(codes.NotFound, "unable to find directory with id %q", req.GetId())
	}
	writable, err := s.directoryWritable(ctx, existing, aclTags(ctx))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error checking directory access: %s", err.Error())
	}
	if !writable {
		return nil, status.Errorf(codes.PermissionDenied, "tags [%s] are not permitted to delete directory %q", strings.Join(aclTags(ctx), ","), req.GetId())
	}
	if err := s.store.DeleteDirectory(ctx, req.GetId()); err != nil {
		if strings.Contains(err.Error(), "not empty") {
			return nil, status.Errorf(codes.FailedPrecondition, "%s", err.Error())
		}
		return nil, status.Errorf(codes.Internal, "error deleting directory: %s", err.Error())
	}
	return &types.DeleteDirectoryResponse{}, nil
}
