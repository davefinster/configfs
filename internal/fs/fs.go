package fs

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/davefinster/configfs/internal/log"
	types "github.com/davefinster/configfs/internal/proto"
	"go.uber.org/multierr"
)

type RemoteConfigFS struct {
	client     types.ConfigFSServerClient
	mountPoint string
	opts       *RemoteConfigFSOptions
	access     *accessTimes

	mu                       sync.Mutex
	fuseMount                *fuse.Conn
	snapshot                 *FilesystemSnapshot
	root                     *Dir
	backgroundRefreshStarted bool
}

type RemoteConfigFSOptions struct {
	Owner                 *uint32
	Group                 *uint32
	FileMode              *os.FileMode
	Writable              bool
	FSName                *string
	FSSubtype             *string
	RefreshInterval       time.Duration
	AdditionalACLOnCreate []*types.ConfigAcl
}

func NewRemoteConfigFS(client types.ConfigFSServerClient, mountPoint string, opts *RemoteConfigFSOptions) *RemoteConfigFS {
	// Set some defaults
	root := uint32(0)
	fileMode := os.FileMode(0o444)
	o := &RemoteConfigFSOptions{
		Owner:           &root,
		Group:           &root,
		Writable:        true,
		FileMode:        &fileMode,
		RefreshInterval: 10 * time.Second,
	}
	if opts != nil {
		o = opts
	}
	return &RemoteConfigFS{
		client:     client,
		mountPoint: mountPoint,
		opts:       o,
		access:     newAccessTimes(),
	}
}

func (s *RemoteConfigFS) mountExists(ctx context.Context) (bool, error) {
	args := []string{"findmnt", "--mountpoint", s.mountPoint}
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	log.InfofCtx(ctx, "attempting to determine whether a mount already exists by running %s", strings.Join(args, " "))
	if out, err := cmd.CombinedOutput(); err != nil {
		outString := string(out)
		if exiterr, ok := err.(*exec.ExitError); ok {
			if exiterr.ExitCode() == 1 {
				// findmnt will return nothing on stdout/stderr if it's a simple case of the mount not existing
				if outString == strings.TrimSpace(outString) {
					return false, nil
				}
				return false, fmt.Errorf("unexpected content in stdout/stderr when calling findmnt which exited with code 1: %q", outString)
			} else {
				return false, fmt.Errorf("unexpected exit code when calling findmnt - got %d and expect either 0 or 1: %w", exiterr.ExitCode(), err)
			}
		} else {
			return false, fmt.Errorf("unexpected error type when calling findmnt: %w", err)
		}
	}
	return true, nil
}

func (s *RemoteConfigFS) AttemptUnmountIfNecessary(ctx context.Context) error {
	exists, err := s.mountExists(ctx)
	if err != nil {
		log.ErrorfCtx(ctx, err, "error determining whether an existing fuse mount exists at %q", s.mountPoint)
		return err
	}
	if !exists {
		log.InfofCtx(ctx, "no existing mount detected at %q", s.mountPoint)
		return nil
	}
	log.InfofCtx(ctx, "attempting unmount using fuse library unmount")
	errors := []error{}
	if err := fuse.Unmount(s.mountPoint); err != nil {
		log.ErrorfCtx(ctx, err, "error attempting fuse library unmount")
		errors = append(errors, err)
	}
	commandsToAttempt := [][]string{
		{"fusermount", "-u", s.mountPoint},
		{"fusermount", "-uz", s.mountPoint},
	}
	for _, attempt := range commandsToAttempt {
		log.InfofCtx(ctx, "attempting to unmount existing mount using %s", strings.Join(attempt, " "))
		if out, err := exec.CommandContext(ctx, attempt[0], attempt[1:]...).CombinedOutput(); err != nil {
			combined := fmt.Errorf("unexpected error encountered when executing %s - output %q - error: %w", strings.Join(attempt, " "), out, err)
			if exiterr, ok := err.(*exec.ExitError); ok {
				combined = fmt.Errorf("unexpected exit code %d when executing %s", exiterr.ExitCode(), string(out))
			}
			log.ErrorfCtx(ctx, err, "error executing command")
			errors = append(errors, combined)
		}
	}
	if len(errors) != len(commandsToAttempt) {
		// At least something was successful, return a nil error
		return nil
	}
	return fmt.Errorf("no commands were successful in unmounting the existing fuse: %w", multierr.Combine(errors...))
}

func (s *RemoteConfigFS) Mount() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fuseMount != nil {
		return fmt.Errorf("already mounted")
	}
	fsName := "configfs"
	if s.opts.FSName != nil {
		fsName = *s.opts.FSName
	}
	subType := "configfs"
	if s.opts.FSSubtype != nil {
		subType = *s.opts.FSSubtype
	}
	opts := []fuse.MountOption{
		fuse.FSName(fsName),
		fuse.Subtype(subType),
	}
	if !s.opts.Writable {
		opts = append(opts, fuse.ReadOnly())
	}
	c, err := fuse.Mount(
		s.mountPoint,
		opts...,
	)
	if err != nil {
		return err
	}
	s.fuseMount = c
	return nil
}

func (s *RemoteConfigFS) LoadSnapshot(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshot = &FilesystemSnapshot{
		client: s.client,
		opts:   s.opts,
	}
	s.snapshot.Refresh(ctx)
	return nil
}

func (s *RemoteConfigFS) backgroundLoop(ctx context.Context) {
	s.mu.Lock()
	if s.backgroundRefreshStarted {
		s.mu.Unlock()
		return
	}
	s.backgroundRefreshStarted = true
	s.mu.Unlock()
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.opts.RefreshInterval):
			if s.snapshot != nil {
				s.snapshot.Refresh(ctx)
			}
		}
	}
}

func (s *RemoteConfigFS) Serve(ctx context.Context) error {
	go s.backgroundLoop(ctx)
	return fs.Serve(s.fuseMount, s)
}

func (s *RemoteConfigFS) Root() (fs.Node, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.root == nil {
		s.root = &Dir{
			path:     "/",
			snapshot: s.snapshot,
			opts:     s.opts,
			access:   s.access,
		}
	}
	return s.root, nil
}

func (s *RemoteConfigFS) Close() error {
	fuse.Unmount(s.mountPoint)
	return s.fuseMount.Close()
}

type FilesystemSnapshot struct {
	client types.ConfigFSServerClient
	opts   *RemoteConfigFSOptions

	mu             sync.RWMutex
	directory      *types.Directory
	pathDirectory  map[string]*types.Directory
	idConfig       map[string]*types.Config
	fullPathConfig map[string]*types.Config
}

func directoryMap(top *types.Directory, m map[string]*types.Directory, c map[string]*types.Config, cp map[string]*types.Config) {
	mapToPopulate := m
	if mapToPopulate == nil {
		mapToPopulate = map[string]*types.Directory{}
	}
	configMapToPopulate := c
	if configMapToPopulate == nil {
		configMapToPopulate = map[string]*types.Config{}
	}
	configPathMapToPopulate := cp
	if configMapToPopulate == nil {
		configPathMapToPopulate = map[string]*types.Config{}
	}
	if _, ok := mapToPopulate[top.FullPath()]; !ok {
		mapToPopulate[top.FullPath()] = top
	}
	for _, config := range top.GetConfigs() {
		configMapToPopulate[config.GetId()] = config
		configPathMapToPopulate[config.FullPath()] = config
	}
	for _, d := range top.GetDirectories() {
		directoryMap(d, mapToPopulate, configMapToPopulate, configPathMapToPopulate)
	}
}

func (s *FilesystemSnapshot) Refresh(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	timeoutCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	res, err := s.client.List(timeoutCtx, &types.ListRequest{})
	if err != nil {
		return err
	}
	s.directory = res.GetTop()
	dMap := map[string]*types.Directory{}
	cMap := map[string]*types.Config{}
	cpMap := map[string]*types.Config{}
	directoryMap(s.directory, dMap, cMap, cpMap)
	s.pathDirectory = dMap
	s.idConfig = cMap
	s.fullPathConfig = cpMap
	return nil
}

func (s *FilesystemSnapshot) Directory(path string) *types.Directory {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.pathDirectory[path]
}

func (s *FilesystemSnapshot) Config(id string) *types.Config {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.idConfig[id]
}

func (s *FilesystemSnapshot) ConfigByPath(path string) *types.Config {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.fullPathConfig[path]
}

func (s *FilesystemSnapshot) FullConfig(ctx context.Context, c *types.Config) (*types.Config, error) {
	fullConfig, err := s.client.GetConfig(ctx, &types.GetConfigRequest{Id: c.GetId()})
	if err != nil {
		return nil, fmt.Errorf("error fetching config content: %w", err)
	}
	return fullConfig.GetConfig(), nil
}

func (s *FilesystemSnapshot) Remove(ctx context.Context, path string) error {
	config := s.ConfigByPath(path)
	if config == nil {
		return syscall.ENOENT
	}
	_, err := s.client.DeleteConfig(ctx, &types.DeleteConfigRequest{
		Id: config.GetId(),
	})
	if err != nil {
		return err
	}
	return s.Refresh(ctx)
}

func (s *FilesystemSnapshot) Create(ctx context.Context, c *types.Config) (*types.Config, error) {
	created, err := s.client.SetConfig(ctx, &types.SetConfigRequest{
		Config: c,
	})
	if err != nil {
		return nil, err
	}
	if err := s.Refresh(ctx); err != nil {
		return nil, err
	}
	return s.Config(created.Config.GetId()), nil
}

func (s *FilesystemSnapshot) Write(ctx context.Context, c *types.Config, h func(context.Context, *types.Config) error) error {
	full, err := s.FullConfig(ctx, c)
	if err != nil {
		return err
	}
	if err := h(ctx, full); err != nil {
		return err
	}
	if _, err := s.client.SetConfig(ctx, &types.SetConfigRequest{Config: full}); err != nil {
		return fmt.Errorf("error writing config content: %w", err)
	}
	return s.Refresh(ctx)
}

func mergeOptsWithAttr(opts *RemoteConfigFSOptions, a *fuse.Attr) {
	if opts == nil {
		return
	}
	if opts.FileMode != nil {
		a.Mode = a.Mode | *opts.FileMode
	}
	if opts.Owner != nil {
		a.Uid = *opts.Owner
	}
	if opts.Group != nil {
		a.Gid = *opts.Group
	}
}

// accessTimes records, in memory only, the last time each config (keyed by its
// id) was read through the mount. It is session-scoped: nothing is persisted, so
// the recorded times reset whenever the client restarts. It backs the atime
// attribute, which the server cannot supply — the server tracks only updated_at
// (a modification time), never access. Methods are safe to call on a nil
// *accessTimes, which behaves as an empty tracker so nodes constructed without
// one (e.g. in tests) degrade gracefully rather than panicking.
type accessTimes struct {
	// now is the time source for recorded accesses; tests override it.
	now func() time.Time

	mu    sync.Mutex
	times map[string]time.Time
}

func newAccessTimes() *accessTimes {
	return &accessTimes{
		now:   time.Now,
		times: map[string]time.Time{},
	}
}

// touch records that the config with the given id was just accessed.
func (a *accessTimes) touch(id string) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.times[id] = a.now()
}

// get returns the last recorded access time for id and whether one exists.
func (a *accessTimes) get(id string) (time.Time, bool) {
	if a == nil {
		return time.Time{}, false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	t, ok := a.times[id]
	return t, ok
}

// Dir implements both Node and Handle for the root directory.
type Dir struct {
	path     string
	snapshot *FilesystemSnapshot
	opts     *RemoteConfigFSOptions
	access   *accessTimes
}

func (d *Dir) dir() *types.Directory {
	return d.snapshot.Directory(d.path)
}

func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = d.dir().NumericalInode()
	a.Mode = a.Mode | os.ModeDir
	mergeOptsWithAttr(d.opts, a)
	return nil
}

func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	fmt.Printf("Lookup %+v\n", name)
	dir := d.dir()
	for _, dir := range dir.GetDirectories() {
		if dir.Name == name {
			return &Dir{snapshot: d.snapshot, opts: d.opts, path: dir.FullPath(), access: d.access}, nil
		}
	}
	for _, conf := range dir.GetConfigs() {
		fmt.Printf("Lookup(%s) returning config %q\n", d.path, conf.GetName())
		if conf.Name == name {
			return &Config{id: conf.GetId(), snapshot: d.snapshot, opts: d.opts, access: d.access}, nil
		}
	}
	return nil, syscall.ENOENT
}

func (d *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	return d.snapshot.Remove(ctx, fmt.Sprintf("%s/%s", d.path, req.Name))
}

func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	fmt.Printf("Create %+v\n", req)
	created, err := d.snapshot.Create(ctx, &types.Config{
		Name:        req.Name,
		Content:     []byte{},
		ContentSize: 0,
		// Files created through the mount default to being readable and writable
		// by everyone; tighter ACLs can be applied out of band via SetConfig.
		Acls: []*types.ConfigAcl{{Acl: types.Acl_WRITE, Everyone: true}},
		Path: d.path,
	})
	if err != nil {
		return nil, nil, err
	}
	con := &Config{id: created.GetId(), snapshot: d.snapshot, opts: d.opts, access: d.access}
	con.Attr(ctx, &resp.Attr)
	return con, con, nil
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	fmt.Printf("ReadDirAll: %+v\n", d.path)
	dir := d.dir()
	contents := []fuse.Dirent{}
	for _, d := range dir.GetDirectories() {
		contents = append(contents, fuse.Dirent{
			Inode: d.NumericalInode(),
			Name:  d.GetName(),
			Type:  fuse.DT_Dir,
		})
	}
	for _, c := range dir.GetConfigs() {
		contents = append(contents, fuse.Dirent{
			Inode: c.NumericalInode(),
			Name:  c.GetName(),
			Type:  fuse.DT_File,
		})
	}
	return contents, nil
}

// File implements both Node and Handle for the hello file.
type Config struct {
	id       string
	snapshot *FilesystemSnapshot
	opts     *RemoteConfigFSOptions
	access   *accessTimes
}

func (f Config) Attr(ctx context.Context, a *fuse.Attr) error {
	c := f.snapshot.Config(f.id)
	a.Inode = c.NumericalInode()
	a.Size = c.GetContentSize()
	// Configs stored before the server tracked timestamps have them unset; keep
	// the attr times zeroed in that case rather than reporting a bogus instant.
	// created_at has no home here: fuse.Attr carries no birth time (Linux FUSE
	// GETATTR has no such field), so it stays API-only.
	if updated := c.GetUpdatedAt(); updated != nil {
		// updated_at moves on any modification (content or ACL), which is ctime
		// semantics; it is also the closest available stand-in for mtime and, until
		// the file is first read this session, atime — the server tracks neither
		// content-only nor access times.
		a.Mtime = updated.AsTime()
		a.Ctime = updated.AsTime()
		a.Atime = updated.AsTime()
	}
	// atime is tracked client-side in memory: once the file has been read through
	// the mount this session, report that access time in preference to updated_at.
	if accessed, ok := f.access.get(f.id); ok {
		a.Atime = accessed
	}
	mergeOptsWithAttr(f.opts, a)
	fmt.Printf("Config Attr (%s): %+v\n", c.GetName(), a)
	return nil
}

func (f Config) ReadAll(ctx context.Context) ([]byte, error) {
	fullConfig, err := f.snapshot.FullConfig(ctx, f.snapshot.Config(f.id))
	if err != nil {
		return nil, fmt.Errorf("error fetching config content: %w", err)
	}
	// Record the read so atime reflects genuine reads through the mount.
	f.access.touch(f.id)
	return fullConfig.GetContent(), nil
}

func (f Config) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	fmt.Printf("Write: %+v\n", req)
	if err := f.snapshot.Write(ctx, f.snapshot.Config(f.id), func(ctx context.Context, conf *types.Config) error {
		content := conf.GetContent()
		end := int(req.Offset) + len(req.Data)
		if end > len(content) {
			grown := make([]byte, end)
			copy(grown, content)
			content = grown
		}
		copy(content[int(req.Offset):end], req.Data)
		conf.Content = content
		conf.ContentSize = uint64(len(content))
		return nil
	}); err != nil {
		return err
	}
	resp.Size = len(req.Data)
	return nil
}
