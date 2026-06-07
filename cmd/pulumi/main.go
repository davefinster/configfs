package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	types "github.com/davefinster/configfs/internal/proto"
	p "github.com/pulumi/pulumi-go-provider"
	"github.com/pulumi/pulumi-go-provider/infer"
	"github.com/pulumi/pulumi/sdk/v3/go/property"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func main() {
	provider, err := infer.NewProviderBuilder().
		WithConfig(infer.Config(&Config{})).
		WithResources(
			infer.Resource(ConfigResource{}),
		).
		WithNamespace("davefinster").
		Build()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s", err.Error())
		os.Exit(1)
	}
	if err := provider.Run(context.Background(), "configfs", "0.1.0"); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s", err.Error())
		os.Exit(1)
	}
}

type Config struct {
	Endpoint string `pulumi:"endpoint,optional"`

	client types.ConfigFSServerClient
}

func (c *Config) Annotate(a infer.Annotator) {
	a.Describe(&c.Endpoint, "host:port of the ConfigFSServer gRPC endpoint. Defaults to localhost:9090.")
}

func (c *Config) Configure(ctx context.Context) error {
	endpoint := c.Endpoint
	if endpoint == "" {
		endpoint = "localhost:9090"
	}
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dialing ConfigFSServer at %q: %w", endpoint, err)
	}
	c.client = types.NewConfigFSServerClient(conn)
	return nil
}

type ConfigResource struct{}

func (c *ConfigResource) Annotate(a infer.Annotator) {
	a.Describe(&c, "A config blob stored in a ConfigFSServer instance and exposed through its FUSE tree.")
}

// ConfigAcl mirrors the proto ConfigAcl: it grants an access level (READ or
// WRITE) to either a specific Tailscale ACL tag or to everyone.
type ConfigAcl struct {
	Acl      string `pulumi:"acl"`
	Tag      string `pulumi:"tag,optional"`
	Everyone bool   `pulumi:"everyone,optional"`
}

func (a *ConfigAcl) Annotate(an infer.Annotator) {
	an.Describe(&a.Acl, "Access level granted: READ or WRITE (WRITE implies READ).")
	an.Describe(&a.Tag, "Tailscale ACL tag this entry grants access to. Ignored when everyone is true.")
	an.Describe(&a.Everyone, "Grant this access level to everyone, regardless of tags.")
}

type ConfigArgs struct {
	Name    string      `pulumi:"name,optional"`
	Path    string      `pulumi:"path"`
	Content string      `pulumi:"content"`
	Acls    []ConfigAcl `pulumi:"acls,optional"`
}

func (a *ConfigArgs) Annotate(an infer.Annotator) {
	an.Describe(&a.Name, "Filename of the config (the leaf node in the FUSE tree). Defaults to the Pulumi resource name.")
	an.Describe(&a.Path, "Directory the config sits under, e.g. /foo/bar.")
	an.Describe(&a.Content, "Raw content of the config.")
	an.Describe(&a.Acls, "Access control entries governing who may read or write this config. A config with no entries is accessible to no one.")
}

type ConfigState struct {
	Name        string      `pulumi:"name"`
	Path        string      `pulumi:"path"`
	Content     string      `pulumi:"content"`
	Acls        []ConfigAcl `pulumi:"acls"`
	ContentSize int         `pulumi:"contentSize"`
}

func (s *ConfigState) Annotate(a infer.Annotator) {
	a.Describe(&s.ContentSize, "Byte length of Content. Computed by the provider.")
}

func (ConfigResource) Check(ctx context.Context, req infer.CheckRequest) (infer.CheckResponse[ConfigArgs], error) {
	if _, ok := req.NewInputs.GetOk("name"); !ok {
		req.NewInputs = req.NewInputs.Set("name", property.New(req.Name))
	}
	args, failures, err := infer.DefaultCheck[ConfigArgs](ctx, req.NewInputs)
	return infer.CheckResponse[ConfigArgs]{Inputs: args, Failures: failures}, err
}

func (ConfigResource) Create(ctx context.Context, req infer.CreateRequest[ConfigArgs]) (infer.CreateResponse[ConfigState], error) {
	if err := validateACLs(req.Inputs.Acls); err != nil {
		return infer.CreateResponse[ConfigState]{}, err
	}
	state := stateFromArgs(req.Inputs)
	if req.DryRun {
		return infer.CreateResponse[ConfigState]{Output: state}, nil
	}
	client, err := configClient(ctx)
	if err != nil {
		return infer.CreateResponse[ConfigState]{}, err
	}
	resp, err := client.SetConfig(ctx, &types.SetConfigRequest{Config: configFromArgs("", req.Inputs)})
	if err != nil {
		return infer.CreateResponse[ConfigState]{}, fmt.Errorf("creating config: %w", err)
	}
	return infer.CreateResponse[ConfigState]{
		ID:     resp.GetConfig().GetId(),
		Output: state,
	}, nil
}

func (ConfigResource) Read(ctx context.Context, req infer.ReadRequest[ConfigArgs, ConfigState]) (infer.ReadResponse[ConfigArgs, ConfigState], error) {
	client, err := configClient(ctx)
	if err != nil {
		return infer.ReadResponse[ConfigArgs, ConfigState]{}, err
	}
	resp, err := client.GetConfig(ctx, &types.GetConfigRequest{Id: req.ID})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return infer.ReadResponse[ConfigArgs, ConfigState]{}, nil
		}
		return infer.ReadResponse[ConfigArgs, ConfigState]{}, fmt.Errorf("reading config %q: %w", req.ID, err)
	}
	cfg := resp.GetConfig()
	args := ConfigArgs{
		Name:    cfg.GetName(),
		Path:    cfg.GetPath(),
		Content: string(cfg.GetContent()),
		Acls:    aclsFromProto(cfg.GetAcls()),
	}
	return infer.ReadResponse[ConfigArgs, ConfigState]{
		ID:     req.ID,
		Inputs: args,
		State:  stateFromArgs(args),
	}, nil
}

func (ConfigResource) Update(ctx context.Context, req infer.UpdateRequest[ConfigArgs, ConfigState]) (infer.UpdateResponse[ConfigState], error) {
	if err := validateACLs(req.Inputs.Acls); err != nil {
		return infer.UpdateResponse[ConfigState]{}, err
	}
	state := stateFromArgs(req.Inputs)
	if req.DryRun {
		return infer.UpdateResponse[ConfigState]{Output: state}, nil
	}
	client, err := configClient(ctx)
	if err != nil {
		return infer.UpdateResponse[ConfigState]{}, err
	}
	if _, err := client.SetConfig(ctx, &types.SetConfigRequest{Config: configFromArgs(req.ID, req.Inputs)}); err != nil {
		return infer.UpdateResponse[ConfigState]{}, fmt.Errorf("updating config %q: %w", req.ID, err)
	}
	return infer.UpdateResponse[ConfigState]{Output: state}, nil
}

func (ConfigResource) Delete(ctx context.Context, req infer.DeleteRequest[ConfigState]) (infer.DeleteResponse, error) {
	client, err := configClient(ctx)
	if err != nil {
		return infer.DeleteResponse{}, err
	}
	if _, err := client.DeleteConfig(ctx, &types.DeleteConfigRequest{Id: req.ID}); err != nil {
		if status.Code(err) == codes.NotFound {
			p.GetLogger(ctx).Warningf("config %q already gone on the server", req.ID)
			return infer.DeleteResponse{}, nil
		}
		return infer.DeleteResponse{}, fmt.Errorf("deleting config %q: %w", req.ID, err)
	}
	return infer.DeleteResponse{}, nil
}

func (ConfigResource) Diff(ctx context.Context, req infer.DiffRequest[ConfigArgs, ConfigState]) (infer.DiffResponse, error) {
	diff := map[string]p.PropertyDiff{}
	if req.Inputs.Name != req.State.Name {
		diff["name"] = p.PropertyDiff{Kind: p.UpdateReplace}
	}
	if req.Inputs.Path != req.State.Path {
		diff["path"] = p.PropertyDiff{Kind: p.UpdateReplace}
	}
	if req.Inputs.Content != req.State.Content {
		diff["content"] = p.PropertyDiff{Kind: p.Update}
	}
	if !aclsEqual(req.Inputs.Acls, req.State.Acls) {
		diff["acls"] = p.PropertyDiff{Kind: p.Update}
	}
	return infer.DiffResponse{
		DeleteBeforeReplace: true,
		HasChanges:          len(diff) > 0,
		DetailedDiff:        diff,
	}, nil
}

func (ConfigResource) WireDependencies(f infer.FieldSelector, args *ConfigArgs, state *ConfigState) {
	f.OutputField(&state.Name).DependsOn(f.InputField(&args.Name))
	f.OutputField(&state.Path).DependsOn(f.InputField(&args.Path))
	f.OutputField(&state.Content).DependsOn(f.InputField(&args.Content))
	f.OutputField(&state.Acls).DependsOn(f.InputField(&args.Acls))
	f.OutputField(&state.ContentSize).DependsOn(f.InputField(&args.Content))
}

func configClient(ctx context.Context) (types.ConfigFSServerClient, error) {
	c := infer.GetConfig[Config](ctx)
	if c.client == nil {
		return nil, fmt.Errorf("provider not configured: no ConfigFSServer client available")
	}
	return c.client, nil
}

func configFromArgs(id string, a ConfigArgs) *types.Config {
	return &types.Config{
		Id:          id,
		Name:        a.Name,
		Path:        a.Path,
		Content:     []byte(a.Content),
		ContentSize: uint64(len(a.Content)),
		Acls:        aclsToProto(a.Acls),
	}
}

func stateFromArgs(a ConfigArgs) ConfigState {
	return ConfigState{
		Name:        a.Name,
		Path:        a.Path,
		Content:     a.Content,
		Acls:        a.Acls,
		ContentSize: len(a.Content),
	}
}

func aclLevel(s string) types.Acl {
	if v, ok := types.Acl_value[strings.ToUpper(strings.TrimSpace(s))]; ok {
		return types.Acl(v)
	}
	return types.Acl_UNKNOWN_ACL
}

func aclsToProto(acls []ConfigAcl) []*types.ConfigAcl {
	if len(acls) == 0 {
		return nil
	}
	out := make([]*types.ConfigAcl, 0, len(acls))
	for _, a := range acls {
		out = append(out, &types.ConfigAcl{
			Acl:      aclLevel(a.Acl),
			Tag:      a.Tag,
			Everyone: a.Everyone,
		})
	}
	return out
}

func aclsFromProto(acls []*types.ConfigAcl) []ConfigAcl {
	if len(acls) == 0 {
		return nil
	}
	out := make([]ConfigAcl, 0, len(acls))
	for _, a := range acls {
		out = append(out, ConfigAcl{
			Acl:      types.Acl_name[int32(a.GetAcl())],
			Tag:      a.GetTag(),
			Everyone: a.GetEveryone(),
		})
	}
	return out
}

// aclsEqual compares ACL lists by canonical access level (case-insensitive) so
// that, e.g., "read" in a program and "READ" read back from the server do not
// register as a perpetual diff.
func aclsEqual(a, b []ConfigAcl) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if aclLevel(a[i].Acl) != aclLevel(b[i].Acl) || a[i].Tag != b[i].Tag || a[i].Everyone != b[i].Everyone {
			return false
		}
	}
	return true
}

// validateACLs rejects ACL entries whose access level is not READ or WRITE.
// Without this an unrecognised level silently degrades to UNKNOWN_ACL, which the
// store drops, leaving a config that nobody can read (deny by default) while
// Pulumi state claims access was granted.
func validateACLs(acls []ConfigAcl) error {
	for _, a := range acls {
		if aclLevel(a.Acl) == types.Acl_UNKNOWN_ACL {
			return fmt.Errorf("invalid acl level %q: must be READ or WRITE", a.Acl)
		}
	}
	return nil
}
