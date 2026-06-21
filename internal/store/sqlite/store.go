package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/davefinster/configfs/internal/log"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	sq "github.com/Masterminds/squirrel"
	types "github.com/davefinster/configfs/internal/proto"
	"github.com/jmoiron/sqlx"
)

var schema = `
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS inode (id INTEGER PRIMARY KEY);
-- created_at/updated_at columns hold unix nanoseconds; 0 means the row predates
-- the columns (value unknown). config.updated_at bumps on every update.
-- config_content rows are immutable versions: each content-changing SetConfig
-- writes a new row (created_at == updated_at) and repoints config.current_content_id.
-- content_size lives on config_content (per version), not on config.
CREATE TABLE IF NOT EXISTS config (
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL,
	path STRING NOT NULL,
	-- current_content_id points at the config_content row holding the live
	-- content. It carries no FK on purpose: config_content.config_id already
	-- references config(id), and a reciprocal FK here would form a cycle that
	-- makes deletion impossible without deferred constraints. The store keeps it
	-- consistent transactionally instead.
	current_content_id INTEGER,
	created_at INTEGER NOT NULL DEFAULT 0,
	updated_at INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS directory (
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL,
	path TEXT NOT NULL,
	parent_id INTEGER,
	FOREIGN KEY (parent_id) REFERENCES directory (id) ON DELETE CASCADE ON UPDATE NO ACTION
);
-- config_content holds one row per content version. id is an independent inode
-- (no longer shared with config.id); config_id links the version back to its
-- config. Multiple rows may share a config_id; config.current_content_id selects
-- the live one. content_size is the byte size of this version's content.
CREATE TABLE IF NOT EXISTS config_content (
	id INTEGER PRIMARY KEY,
	config_id INTEGER NOT NULL,
	content BLOB NOT NULL,
	content_size INTEGER NOT NULL DEFAULT 0,
	created_at INTEGER NOT NULL DEFAULT 0,
	updated_at INTEGER NOT NULL DEFAULT 0,
	FOREIGN KEY (config_id) REFERENCES config (id) ON DELETE CASCADE ON UPDATE NO ACTION
);
CREATE TABLE IF NOT EXISTS config_acl (
	id INTEGER PRIMARY KEY,
	config_id INTEGER NOT NULL,
	acl INTEGER NOT NULL,
	tag TEXT NOT NULL DEFAULT '',
	everyone INTEGER NOT NULL DEFAULT 0,
	FOREIGN KEY (config_id) REFERENCES config (id) ON DELETE CASCADE ON UPDATE NO ACTION
);
CREATE INDEX IF NOT EXISTS config_acl_config_id ON config_acl (config_id);
-- directory_acl mirrors config_acl: one row per ACL entry on a directory. A
-- directory with no rows is transparent (imposes no restriction); rows make it
-- enforce like a config. It is a brand-new table, so CREATE TABLE IF NOT EXISTS
-- materialises it on both fresh and existing databases with no ALTER needed.
CREATE TABLE IF NOT EXISTS directory_acl (
	id INTEGER PRIMARY KEY,
	directory_id INTEGER NOT NULL,
	acl INTEGER NOT NULL,
	tag TEXT NOT NULL DEFAULT '',
	everyone INTEGER NOT NULL DEFAULT 0,
	FOREIGN KEY (directory_id) REFERENCES directory (id) ON DELETE CASCADE ON UPDATE NO ACTION
);
CREATE INDEX IF NOT EXISTS directory_acl_directory_id ON directory_acl (directory_id);
`

// migrations are idempotent statements that bring databases created before a
// column existed up to the current schema. CREATE TABLE IF NOT EXISTS is a
// no-op for existing tables, so any column added to the schema above must also
// be added here; the "duplicate column name" error this produces on databases
// that already have the column is expected and ignored. The trailing UPDATEs are
// backfills written so re-running them is a no-op. Restructuring config_content
// for versioning cannot be expressed as an idempotent statement here (it must
// inspect the existing schema), so it runs separately in migrateContentVersions.
var migrations = []string{
	`ALTER TABLE config ADD COLUMN created_at INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE config ADD COLUMN updated_at INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE config_content ADD COLUMN created_at INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE config_content ADD COLUMN updated_at INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE config ADD COLUMN current_content_id INTEGER`,
	// Pre-versioning content lived in a config_content row whose id equalled the
	// config id, so the current version of a migrated config is that same id.
	`UPDATE config SET current_content_id = id WHERE current_content_id IS NULL`,
}

type Store struct {
	db  *sqlx.DB
	sql sq.StatementBuilderType
	// now is the time source for created_at/updated_at stamps; tests override it.
	now func() time.Time
}

func NewStore(db *sqlx.DB) (*Store, error) {
	// Serialize all access through a single physical connection. SQLite permits
	// only one writer at a time; with the default (unlimited) pool, concurrent
	// SetConfig calls start their transactions on separate connections and race
	// for the write lock, so the loser fails immediately with "database is
	// locked" (SQLITE_BUSY). go-sqlite3's default deferred BEGIN makes this worse:
	// each transaction opens as a reader and only upgrades to a writer on its
	// first INSERT/UPDATE, the exact lock-upgrade case where SQLite returns BUSY
	// without waiting on any busy handler. Capping the pool at one connection
	// makes database/sql queue concurrent callers instead: a second transaction
	// blocks in BeginTxx until the in-flight one releases the connection (commit
	// or rollback), i.e. it waits for the inflight transaction to complete rather
	// than failing. The wait respects the caller's context, so a cancelled or
	// timed-out request still unblocks. This is safe from self-deadlock because no
	// single operation needs two connections at once: queries inside a
	// transaction run on the tx connection (querier(ctx) returns the tx) and the
	// API layer never nests a bare-db query inside an open transaction.
	db.SetMaxOpenConns(1)
	db.MustExec(schema)
	for _, migration := range migrations {
		if _, err := db.Exec(migration); err != nil {
			if strings.Contains(err.Error(), "duplicate column name") {
				continue
			}
			return nil, fmt.Errorf("error applying migration %q: %w", migration, err)
		}
	}
	if err := migrateContentVersions(db); err != nil {
		return nil, fmt.Errorf("error migrating config_content for versioning: %w", err)
	}
	if err := migrateContentSize(db); err != nil {
		return nil, fmt.Errorf("error migrating content_size onto config_content: %w", err)
	}
	// Index config_content by config_id only now that the column is guaranteed to
	// exist (created by the fresh schema, or added by migrateContentVersions on a
	// legacy database). It cannot live in `schema` above: there CREATE TABLE IF NOT
	// EXISTS is a no-op on a legacy config_content that still lacks config_id, so
	// the CREATE INDEX would fail before the migration adds the column.
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS config_content_config_id ON config_content (config_id)`); err != nil {
		return nil, fmt.Errorf("error creating config_content config_id index: %w", err)
	}
	return &Store{
		db:  db,
		sql: sq.StatementBuilder,
		now: time.Now,
	}, nil
}

// tableHasColumn reports whether the named table already has the named column.
// Used to detect which schema shape a database is in before migrating.
func tableHasColumn(db *sqlx.DB, table, column string) (bool, error) {
	names := []string{}
	if err := db.Select(&names, "SELECT name FROM pragma_table_info(?)", table); err != nil {
		return false, err
	}
	return slices.Contains(names, column), nil
}

// migrateContentVersions converts a pre-versioning config_content table — where
// id is the config id and a FOREIGN KEY (id) REFERENCES config(id) ties the two
// together one-to-one — into the versioned shape with an independent id and a
// config_id FK. It is a no-op once config_content already has a config_id column.
//
// The table is rebuilt rather than ALTERed because SQLite cannot drop the
// obsolete id->config foreign key in place, and that key — enforced now that the
// pool is pinned to a single connection (NewStore) — would reject any new
// version row whose id is not also an existing config id. Each legacy row's id
// is its config id, so the rebuild sets config_id := id; the flat migrations run
// first, guaranteeing created_at/updated_at exist to copy across.
func migrateContentVersions(db *sqlx.DB) error {
	has, err := tableHasColumn(db, "config_content", "config_id")
	if err != nil {
		return fmt.Errorf("inspecting config_content: %w", err)
	}
	if has {
		return nil
	}
	stmts := []string{
		`ALTER TABLE config_content RENAME TO config_content_legacy`,
		`CREATE TABLE config_content (
			id INTEGER PRIMARY KEY,
			config_id INTEGER NOT NULL,
			content BLOB NOT NULL,
			created_at INTEGER NOT NULL DEFAULT 0,
			updated_at INTEGER NOT NULL DEFAULT 0,
			FOREIGN KEY (config_id) REFERENCES config (id) ON DELETE CASCADE ON UPDATE NO ACTION
		)`,
		`INSERT INTO config_content (id, config_id, content, created_at, updated_at)
			SELECT id, id, content, created_at, updated_at FROM config_content_legacy`,
		`DROP TABLE config_content_legacy`,
	}
	tx, err := db.Beginx()
	if err != nil {
		return fmt.Errorf("starting rebuild transaction: %w", err)
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("rebuilding config_content (%q): %w", stmt, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing config_content rebuild: %w", err)
	}
	return nil
}

// migrateContentSize relocates content_size from config onto config_content, so
// each version carries its own size. It is a no-op once config_content already
// has a content_size column (fresh databases, where `schema` created it there).
//
// Existing version rows are backfilled from length(content) — the authoritative
// byte count, and the only size available for historical versions that never had
// a stored size of their own. The now-redundant config.content_size column is
// dropped. It runs after migrateContentVersions, so config_content is already in
// its versioned shape (independent id + config_id) before the column is added.
func migrateContentSize(db *sqlx.DB) error {
	has, err := tableHasColumn(db, "config_content", "content_size")
	if err != nil {
		return fmt.Errorf("inspecting config_content: %w", err)
	}
	if has {
		return nil
	}
	stmts := []string{
		`ALTER TABLE config_content ADD COLUMN content_size INTEGER NOT NULL DEFAULT 0`,
		`UPDATE config_content SET content_size = length(content)`,
		`ALTER TABLE config DROP COLUMN content_size`,
	}
	tx, err := db.Beginx()
	if err != nil {
		return fmt.Errorf("starting content_size migration transaction: %w", err)
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("migrating content_size (%q): %w", stmt, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing content_size migration: %w", err)
	}
	return nil
}

// querier is the common subset of *sqlx.DB and *sqlx.Tx that Store methods use.
type querier interface {
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

type txCtxKey struct{}

// withTx starts a transaction and returns a context carrying it plus a finish
// callback. Pass the operation error to finish exactly once: nil commits,
// non-nil rolls back. Commit and rollback errors are logged, not returned, to
// preserve the original operation error.
func (s *Store) withTx(ctx context.Context) (context.Context, func(opErr error), error) {
	tx, err := s.db.BeginTxx(ctx, &sql.TxOptions{})
	if err != nil {
		return ctx, nil, err
	}
	txCtx := context.WithValue(ctx, txCtxKey{}, tx)
	finish := func(opErr error) {
		if opErr != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				log.WarningfCtx(ctx, "error performing rollback of errored transaction: %s", rbErr.Error())
			}
			return
		}
		if cmErr := tx.Commit(); cmErr != nil {
			log.WarningfCtx(ctx, "error performing commit of otherwise successful transaction: %s", cmErr.Error())
		}
	}
	return txCtx, finish, nil
}

// querier returns the active transaction from ctx if one was started via
// withTx, otherwise the bare *sqlx.DB.
func (s *Store) querier(ctx context.Context) querier {
	if tx, ok := ctx.Value(txCtxKey{}).(*sqlx.Tx); ok {
		return tx
	}
	return s.db
}

// rootNormalised treats an empty config path as the root "/" so a config stored
// directly at the root attaches to the synthesised tree root, whose FullPath()
// is "/".
func rootNormalised(path string) string {
	if path == "" {
		return "/"
	}
	return path
}

func populateDirectory(directory *types.Directory, otherDirectories []*types.Directory, configs []*types.Config) {
	for _, config := range configs {
		if rootNormalised(config.GetPath()) == directory.FullPath() {
			copy := proto.Clone(config).(*types.Config)
			directory.Configs = append(directory.Configs, copy)
		}
	}
	for _, otherDir := range otherDirectories {
		if otherDir.GetPath() == directory.FullPath() {
			copy := proto.Clone(otherDir).(*types.Directory)
			populateDirectory(copy, otherDirectories, configs)
			directory.Directories = append(directory.Directories, copy)
		}
	}
}

func (s *Store) TreeForACLTags(ctx context.Context, aclTags []string, prefix string) (*types.Directory, error) {
	// Load every directory with its ACLs and work out which the caller may
	// traverse. Readable but empty directories are surfaced too (unlike the
	// configs, which still gate on their own ACLs), so the tree mirrors what the
	// caller can walk — e.g. a directory created via mkdir shows up under ls even
	// before it holds any config.
	allDirs, err := s.allDirectoriesWithACLs(ctx)
	if err != nil {
		return nil, fmt.Errorf("error loading directories: %w", err)
	}
	dirByPath := make(map[string]*types.Directory, len(allDirs))
	for _, d := range allDirs {
		dirByPath[d.FullPath()] = d
	}
	readable := readableDirectories(allDirs, aclTags)

	where := sq.And{}
	if prefix != "" {
		// Constrain to configs living in the prefix directory or any descendant.
		where = append(where, sq.Or{
			sq.Eq{"c.path": prefix},
			sq.Like{"c.path": prefix + "/%"},
		})
	}
	// content_size lives on config_content now, so join the config's current
	// version to surface it. Columns shared by both tables (id, created_at,
	// updated_at) are qualified to avoid ambiguity; COALESCE guards the (in
	// practice impossible) case of a config with no current content row.
	builder := s.sql.Select("c.id", "c.name", "COALESCE(cc.content_size, 0) AS content_size", "c.path", "c.current_content_id", "c.created_at", "c.updated_at").
		From("config c").
		LeftJoin("config_content cc ON cc.id = c.current_content_id")
	if len(where) > 0 {
		builder = builder.Where(where)
	}
	sql, args, err := builder.ToSql()
	if err != nil {
		return nil, fmt.Errorf("error forming sql for selecting configs: %w", err)
	}
	log.InfofCtx(ctx, "SQL: %s args: %+v", sql, args)
	cs := []configSelect{}
	if err := s.querier(ctx).SelectContext(ctx, &cs, sql, args...); err != nil {
		return nil, fmt.Errorf("error fetching config: %w", err)
	}
	// Tags now live in the separate config_acl table; load the ACLs for every
	// candidate config in a single query, then filter on read access in Go.
	configIDs := make([]int, 0, len(cs))
	for _, c := range cs {
		configIDs = append(configIDs, c.ID)
	}
	aclsByConfig, err := s.loadConfigACLs(ctx, configIDs)
	if err != nil {
		return nil, fmt.Errorf("error loading config acls: %w", err)
	}
	configs := []*types.Config{}
	for _, c := range cs {
		// Enforce the path prefix precisely; the SQL LIKE above is only a prefilter.
		if prefix != "" && c.Path != prefix && !strings.HasPrefix(c.Path, prefix+"/") {
			continue
		}
		config := &types.Config{
			Id:               fmt.Sprintf("%d", c.ID),
			Name:             c.Name,
			Path:             c.Path,
			ContentSize:      uint64(c.ContentSize),
			Acls:             aclsByConfig[c.ID],
			CurrentContentId: idStringFromNullInt(c.CurrentContentID),
			CreatedAt:        timestampFromUnixNano(c.CreatedAt),
			UpdatedAt:        timestampFromUnixNano(c.UpdatedAt),
		}
		// Per-config gate: the caller must be permitted to read it.
		if !config.Allows(aclTags, types.Acl_READ) {
			continue
		}
		// A non-root config must live in a directory that actually exists.
		if c.Path != "" && c.Path != "/" {
			if _, ok := dirByPath[c.Path]; !ok {
				return nil, fmt.Errorf("missing directory %q for config %q", c.Path, config.FullPath())
			}
		}
		// Hierarchical gate: every ancestor directory must grant READ. readable is
		// keyed by the containing directory's full path and already folds in the
		// whole chain (root and "" are seeded readable for root-level configs).
		if !readable[c.Path] {
			continue
		}
		configs = append(configs, config)
	}
	// Surface every readable directory, even empty ones. Because a directory is
	// readable only when its parent is too, the readable set is a connected
	// subtree rooted at the synthesised root, so populateDirectory can wire it up.
	includeDirs := make([]*types.Directory, 0, len(allDirs))
	for _, d := range allDirs {
		if readable[d.FullPath()] {
			includeDirs = append(includeDirs, d)
		}
	}
	treeRoot := &types.Directory{
		Id:   "0",
		Name: "/",
		Path: "",
	}
	populateDirectory(treeRoot, includeDirs, configs)
	return treeRoot, nil
}

// allDirectoriesWithACLs loads every directory row with its ACLs populated.
func (s *Store) allDirectoriesWithACLs(ctx context.Context) ([]*types.Directory, error) {
	sql, args, err := s.sql.Select("id", "name", "path").From("directory").OrderBy("path").ToSql()
	if err != nil {
		return nil, fmt.Errorf("error building sql for selecting directories: %w", err)
	}
	rows := []directorySelect{}
	if err := s.querier(ctx).SelectContext(ctx, &rows, sql, args...); err != nil {
		return nil, fmt.Errorf("error fetching directories: %w", err)
	}
	ids := make([]int, 0, len(rows))
	for _, r := range rows {
		ids = append(ids, r.ID)
	}
	aclsByDir, err := s.loadDirectoryACLs(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("error loading directory acls: %w", err)
	}
	dirs := make([]*types.Directory, 0, len(rows))
	for _, r := range rows {
		dirs = append(dirs, &types.Directory{
			Id:   fmt.Sprintf("%d", r.ID),
			Name: r.Name,
			Path: r.Path,
			Acls: aclsByDir[r.ID],
		})
	}
	return dirs, nil
}

// readableDirectories returns, keyed by full path, whether the caller may
// traverse each directory: a directory is readable iff its parent is readable and
// it grants READ (a directory with no ACLs is transparent). Directories are
// processed shallowest-first so a parent's verdict is known before its children;
// the synthesised root ("" and "/") is seeded readable, which also lets
// root-level configs pass the gate.
func readableDirectories(dirs []*types.Directory, tags []string) map[string]bool {
	sorted := make([]*types.Directory, len(dirs))
	copy(sorted, dirs)
	slices.SortFunc(sorted, func(a, b *types.Directory) int {
		return strings.Count(a.FullPath(), "/") - strings.Count(b.FullPath(), "/")
	})
	readable := map[string]bool{"": true, "/": true}
	for _, d := range sorted {
		// d.GetPath() is the parent's full path; top-level directories have "/".
		readable[d.FullPath()] = readable[d.GetPath()] && d.Allows(tags, types.Acl_READ)
	}
	return readable
}

func (s *Store) nextInode(ctx context.Context) (int, error) {
	res, err := s.querier(ctx).ExecContext(ctx, "INSERT INTO inode (id) VALUES ($1)", nil)
	if err != nil {
		return 0, fmt.Errorf("error execing for next inode: %w", err)
	}
	inode, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("error getting next inode: %w", err)
	}
	return int(inode), nil
}

type directorySelect struct {
	ID   int    `db:"id"`
	Name string `db:"name"`
	Path string `db:"path"`
}

func (s *Store) getDirectoriesByPathString(ctx context.Context, targets []string) ([]*types.Directory, []string, error) {
	constraints := sq.Or{}
	for _, target := range targets {
		pathParts := strings.Split(target, "/")
		filtered := []string{}
		for _, pathPart := range pathParts {
			clean := strings.TrimSpace(pathPart)
			if len(clean) > 0 {
				filtered = append(filtered, clean)
			}
		}
		pathParts = filtered
		for idx, name := range pathParts {
			constraints = append(constraints, sq.Eq{
				"path": "/" + strings.Join(pathParts[:idx], "/"),
				"name": name,
			})
		}
	}
	// No real path components to look up (e.g. only the root was requested):
	// return everything as missing rather than building a constraint-less WHERE
	// that would select every directory.
	if len(constraints) == 0 {
		return []*types.Directory{}, append([]string(nil), targets...), nil
	}
	sql, args, err := s.sql.Select("id", "name", "path").From("directory").Where(constraints).OrderBy("path").ToSql()
	if err != nil {
		return nil, nil, fmt.Errorf("error building sql query for fetching directories: %w", err)
	}
	directories := []directorySelect{}
	if err := s.querier(ctx).SelectContext(ctx, &directories, sql, args...); err != nil {
		return nil, nil, fmt.Errorf("error fetching directories: %w", err)
	}
	dirIDs := make([]int, 0, len(directories))
	for _, d := range directories {
		dirIDs = append(dirIDs, d.ID)
	}
	aclsByDir, err := s.loadDirectoryACLs(ctx, dirIDs)
	if err != nil {
		return nil, nil, fmt.Errorf("error loading directory acls: %w", err)
	}
	protoDirectories := []*types.Directory{}
	fetchedPaths := []string{}
	for _, d := range directories {
		dir := &types.Directory{
			Id:   fmt.Sprintf("%d", d.ID),
			Name: d.Name,
			Path: d.Path,
			Acls: aclsByDir[d.ID],
		}
		protoDirectories = append(protoDirectories, dir)
		fetchedPaths = append(fetchedPaths, dir.FullPath())
	}
	missing := []string{}
	for _, target := range targets {
		if !slices.Contains(fetchedPaths, target) {
			missing = append(missing, target)
		}
	}
	return protoDirectories, missing, nil
}

func (s *Store) createDirectory(ctx context.Context, path string, name string, parentID *string, acls []*types.ConfigAcl) (*types.Directory, error) {
	inode, err := s.nextInode(ctx)
	if err != nil {
		return nil, fmt.Errorf("error getting inode for new directory: %w", err)
	}
	sql, args, err := sq.Insert("directory").Columns("id", "name", "path", "parent_id").Values(inode, name, path, parentID).ToSql()
	if err != nil {
		return nil, fmt.Errorf("error building sql query for inserting directory: %w", err)
	}
	if _, err := s.querier(ctx).ExecContext(ctx, sql, args...); err != nil {
		return nil, fmt.Errorf("error executing directory insert: %w", err)
	}
	if err := s.insertDirectoryACLs(ctx, inode, acls); err != nil {
		return nil, fmt.Errorf("error inserting directory acls: %w", err)
	}
	return &types.Directory{Id: fmt.Sprintf("%d", inode), Name: name, Path: path, Acls: acls}, nil
}

// ensureDirectories materialises every directory along fullPath, creating any
// that are missing and leaving existing ones untouched. Newly created
// directories all carry the supplied acls (option: "apply the supplied ACL to
// every level it creates"); pre-existing directories keep their own ACLs. It
// returns the full set of directories making up fullPath (existing + created),
// each with its ACLs populated. Callers should run it inside a transaction.
func (s *Store) ensureDirectories(ctx context.Context, fullPath string, acls []*types.ConfigAcl) ([]*types.Directory, error) {
	log.InfofCtx(ctx, "Ensuring path %q", fullPath)
	pathParts := strings.Split(fullPath, "/")
	cleanParts := []string{}
	for _, part := range pathParts {
		trimmed := strings.TrimSpace(part)
		if len(trimmed) == 0 {
			continue
		}
		cleanParts = append(cleanParts, trimmed)
	}
	pathParts = cleanParts
	pathsToFetch := []string{}
	for idx := range pathParts {
		pathsToFetch = append(pathsToFetch, "/"+strings.Join(pathParts[:idx+1], "/"))
	}
	log.InfofCtx(ctx, "Fetching paths %+v", pathsToFetch)
	directories, missing, err := s.getDirectoriesByPathString(ctx, pathsToFetch)
	if err != nil {
		return nil, err
	}
	if len(missing) == 0 {
		return directories, nil
	}
	slices.Sort(missing)
	for _, miss := range missing {
		lastSlash := strings.LastIndex(miss, "/")
		parentPath := miss[:lastSlash]
		if parentPath == "" {
			parentPath = "/"
		}
		name := miss[lastSlash+1:]
		var parentID *string
		if parentPath != "/" {
			for _, existing := range directories {
				if existing.FullPath() == parentPath {
					parentID = proto.String(existing.GetId())
					break
				}
			}
		}
		newDir, err := s.createDirectory(ctx, parentPath, name, parentID, acls)
		if err != nil {
			return nil, fmt.Errorf("error creating directory %s/%s: %w", parentPath, name, err)
		}
		directories = append(directories, newDir)
	}
	return directories, nil
}

// requireDirectoryExists returns an error if the directory at the given config
// path does not exist. The root ("" or "/") always exists (it is synthesised and
// has no row), so it is accepted without a lookup. This is the gate that makes
// SetConfig reject configs whose directory has not been created — directories are
// no longer auto-vivified on the config write path; use CreateDirectory first.
func (s *Store) requireDirectoryExists(ctx context.Context, path string) error {
	if path == "" || path == "/" {
		return nil
	}
	_, missing, err := s.getDirectoriesByPathString(ctx, []string{path})
	if err != nil {
		return err
	}
	if len(missing) > 0 {
		return fmt.Errorf("directory %q does not exist", path)
	}
	return nil
}

type configSelect struct {
	ID               int           `db:"id"`
	Name             string        `db:"name"`
	ContentSize      int           `db:"content_size"`
	Path             string        `db:"path"`
	CurrentContentID sql.NullInt64 `db:"current_content_id"`
	CreatedAt        int64         `db:"created_at"`
	UpdatedAt        int64         `db:"updated_at"`
}

// idStringFromNullInt renders a nullable integer id as a decimal string, or ""
// when null. Used for config.current_content_id, which is unset on configs that
// predate content versioning.
func idStringFromNullInt(v sql.NullInt64) string {
	if !v.Valid {
		return ""
	}
	return strconv.FormatInt(v.Int64, 10)
}

// timestampFromUnixNano converts a stored unix-nanosecond value to a proto
// timestamp. Zero (the default backfilled into rows that predate the
// timestamp columns) means "unknown" and maps to an unset field.
func timestampFromUnixNano(ns int64) *timestamppb.Timestamp {
	if ns == 0 {
		return nil
	}
	return timestamppb.New(time.Unix(0, ns))
}

type configContentSelect struct {
	ID          int    `db:"id"`
	Content     []byte `db:"content"`
	ContentSize int    `db:"content_size"`
}

// aclRow is the shared row shape for config_acl and directory_acl. The owner
// foreign key (config_id / directory_id) is aliased to owner_id by loadACLs so a
// single struct serves both tables.
type aclRow struct {
	ID       int    `db:"id"`
	OwnerID  int    `db:"owner_id"`
	Acl      int    `db:"acl"`
	Tag      string `db:"tag"`
	Everyone int    `db:"everyone"`
}

func (r aclRow) toProto() *types.ConfigAcl {
	return &types.ConfigAcl{
		Acl:      types.Acl(r.Acl),
		Tag:      r.Tag,
		Everyone: r.Everyone != 0,
	}
}

// config and directory ACLs live in identically-shaped tables (config_acl /
// directory_acl), each with an owner foreign-key column (config_id /
// directory_id) plus acl/tag/everyone. The load/insert/delete/replace logic is
// therefore shared, parameterised by table and owner column; the config and
// directory wrappers below pin those parameters.

// loadACLs fetches the ACL entries for the supplied owner IDs from the given
// table, grouped by owner ID. Owner IDs with no rows are simply absent from the
// map.
func (s *Store) loadACLs(ctx context.Context, table, ownerCol string, ownerIDs []int) (map[int][]*types.ConfigAcl, error) {
	out := map[int][]*types.ConfigAcl{}
	if len(ownerIDs) == 0 {
		return out, nil
	}
	sql, args, err := s.sql.Select("id", ownerCol+" AS owner_id", "acl", "tag", "everyone").
		From(table).
		Where(sq.Eq{ownerCol: ownerIDs}).
		OrderBy("id").
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("error building sql for selecting %s: %w", table, err)
	}
	rows := []aclRow{}
	if err := s.querier(ctx).SelectContext(ctx, &rows, sql, args...); err != nil {
		return nil, fmt.Errorf("error fetching %s: %w", table, err)
	}
	for _, row := range rows {
		out[row.OwnerID] = append(out[row.OwnerID], row.toProto())
	}
	return out, nil
}

// aclsForOwner is a convenience wrapper around loadACLs for a single owner.
func (s *Store) aclsForOwner(ctx context.Context, table, ownerCol string, ownerID int) ([]*types.ConfigAcl, error) {
	byID, err := s.loadACLs(ctx, table, ownerCol, []int{ownerID})
	if err != nil {
		return nil, err
	}
	return byID[ownerID], nil
}

// insertACLs writes the supplied ACL entries into the given table for an owner.
// It is a no-op when acls is empty. UNKNOWN_ACL entries are skipped as they
// grant nothing.
func (s *Store) insertACLs(ctx context.Context, table, ownerCol string, ownerID int, acls []*types.ConfigAcl) error {
	ib := s.sql.Insert(table).Columns(ownerCol, "acl", "tag", "everyone")
	count := 0
	for _, acl := range acls {
		// Only persist recognised access levels; UNKNOWN_ACL and any out-of-range
		// value grant nothing, so storing them would just leave junk rows.
		if acl.GetAcl() != types.Acl_READ && acl.GetAcl() != types.Acl_WRITE {
			continue
		}
		everyone := 0
		if acl.GetEveryone() {
			everyone = 1
		}
		ib = ib.Values(ownerID, int(acl.GetAcl()), acl.GetTag(), everyone)
		count++
	}
	if count == 0 {
		return nil
	}
	sql, args, err := ib.ToSql()
	if err != nil {
		return fmt.Errorf("error building sql for inserting %s: %w", table, err)
	}
	if _, err := s.querier(ctx).ExecContext(ctx, sql, args...); err != nil {
		return fmt.Errorf("error inserting %s: %w", table, err)
	}
	return nil
}

// deleteACLs removes every ACL entry belonging to an owner in the given table.
func (s *Store) deleteACLs(ctx context.Context, table, ownerCol string, ownerID int) error {
	sql, args, err := s.sql.Delete(table).Where(sq.Eq{ownerCol: ownerID}).ToSql()
	if err != nil {
		return fmt.Errorf("error building sql for deleting %s: %w", table, err)
	}
	if _, err := s.querier(ctx).ExecContext(ctx, sql, args...); err != nil {
		return fmt.Errorf("error deleting %s: %w", table, err)
	}
	return nil
}

// replaceACLs atomically swaps the full ACL set for an owner, used on update.
// Callers should run this inside a transaction.
func (s *Store) replaceACLs(ctx context.Context, table, ownerCol string, ownerID int, acls []*types.ConfigAcl) error {
	if err := s.deleteACLs(ctx, table, ownerCol, ownerID); err != nil {
		return err
	}
	return s.insertACLs(ctx, table, ownerCol, ownerID, acls)
}

// loadConfigACLs fetches the ACL entries for the supplied config IDs, grouped by
// config ID. Config IDs with no ACL rows are simply absent from the map.
func (s *Store) loadConfigACLs(ctx context.Context, configIDs []int) (map[int][]*types.ConfigAcl, error) {
	return s.loadACLs(ctx, "config_acl", "config_id", configIDs)
}

// configACLs is a convenience wrapper around loadConfigACLs for a single config.
func (s *Store) configACLs(ctx context.Context, configID int) ([]*types.ConfigAcl, error) {
	return s.aclsForOwner(ctx, "config_acl", "config_id", configID)
}

func (s *Store) insertConfigACLs(ctx context.Context, configID int, acls []*types.ConfigAcl) error {
	return s.insertACLs(ctx, "config_acl", "config_id", configID, acls)
}

func (s *Store) deleteConfigACLs(ctx context.Context, configID int) error {
	return s.deleteACLs(ctx, "config_acl", "config_id", configID)
}

func (s *Store) replaceConfigACLs(ctx context.Context, configID int, acls []*types.ConfigAcl) error {
	return s.replaceACLs(ctx, "config_acl", "config_id", configID, acls)
}

// loadDirectoryACLs fetches the ACL entries for the supplied directory IDs,
// grouped by directory ID. Directory IDs with no ACL rows are simply absent from
// the map (and are transparent — see Directory.Allows).
func (s *Store) loadDirectoryACLs(ctx context.Context, directoryIDs []int) (map[int][]*types.ConfigAcl, error) {
	return s.loadACLs(ctx, "directory_acl", "directory_id", directoryIDs)
}

// directoryACLs is a convenience wrapper around loadDirectoryACLs for one directory.
func (s *Store) directoryACLs(ctx context.Context, directoryID int) ([]*types.ConfigAcl, error) {
	return s.aclsForOwner(ctx, "directory_acl", "directory_id", directoryID)
}

func (s *Store) insertDirectoryACLs(ctx context.Context, directoryID int, acls []*types.ConfigAcl) error {
	return s.insertACLs(ctx, "directory_acl", "directory_id", directoryID, acls)
}

func (s *Store) deleteDirectoryACLs(ctx context.Context, directoryID int) error {
	return s.deleteACLs(ctx, "directory_acl", "directory_id", directoryID)
}

func (s *Store) replaceDirectoryACLs(ctx context.Context, directoryID int, acls []*types.ConfigAcl) error {
	return s.replaceACLs(ctx, "directory_acl", "directory_id", directoryID, acls)
}

func (s *Store) getConfigByPathAndName(ctx context.Context, path string, name string) (*types.Config, error) {
	sql, args, err := s.sql.Select("c.id", "c.name", "COALESCE(cc.content_size, 0) AS content_size", "c.path", "c.current_content_id", "c.created_at", "c.updated_at").
		From("config c").
		LeftJoin("config_content cc ON cc.id = c.current_content_id").
		Where(sq.Eq{
			"c.name": name,
			"c.path": path,
		}).ToSql()
	if err != nil {
		return nil, fmt.Errorf("error creating sql for selecting config: %w", err)
	}
	cs := configSelect{}
	if err := s.querier(ctx).GetContext(ctx, &cs, sql, args...); err != nil {
		if strings.Contains(err.Error(), "no rows in result set") {
			return nil, nil
		}
		return nil, fmt.Errorf("error fetching config: %w", err)
	}
	return &types.Config{
		Id:               fmt.Sprintf("%d", cs.ID),
		Name:             cs.Name,
		ContentSize:      uint64(cs.ContentSize),
		Path:             cs.Path,
		CurrentContentId: idStringFromNullInt(cs.CurrentContentID),
		CreatedAt:        timestampFromUnixNano(cs.CreatedAt),
		UpdatedAt:        timestampFromUnixNano(cs.UpdatedAt),
	}, nil
}

// GetConfigByID loads a config by id. When includeContent is true the content
// blob is loaded as well: version selects which config_content version to return
// (a content id as surfaced by Config.current_content_id), and an empty version
// loads the config's current (latest) version. version is ignored when
// includeContent is false. It returns (nil, nil) when no such config exists or,
// for an explicit version, when that config has no such version.
func (s *Store) GetConfigByID(ctx context.Context, id string, includeContent bool, version string) (*types.Config, error) {
	numericID, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("unable to convert ID to number: %w", err)
	}
	// content_size lives on config_content; join the current version to surface it
	// (overridden below if an explicit, non-current version is loaded). Shared
	// columns are qualified to disambiguate the join.
	sql, args, err := s.sql.Select("c.id", "c.name", "COALESCE(cc.content_size, 0) AS content_size", "c.path", "c.current_content_id", "c.created_at", "c.updated_at").
		From("config c").
		LeftJoin("config_content cc ON cc.id = c.current_content_id").
		Where(sq.Eq{
			"c.id": numericID,
		}).ToSql()
	if err != nil {
		return nil, fmt.Errorf("error creating sql for selecting config: %w", err)
	}
	q := s.querier(ctx)
	cs := configSelect{}
	if err := q.GetContext(ctx, &cs, sql, args...); err != nil {
		if strings.Contains(err.Error(), "no rows in result set") {
			return nil, nil
		}
		return nil, fmt.Errorf("error fetching config: %w", err)
	}
	acls, err := s.configACLs(ctx, cs.ID)
	if err != nil {
		return nil, fmt.Errorf("error fetching config acls: %w", err)
	}
	conf := &types.Config{
		Id:               fmt.Sprintf("%d", cs.ID),
		Name:             cs.Name,
		Acls:             acls,
		ContentSize:      uint64(cs.ContentSize),
		Path:             cs.Path,
		CurrentContentId: idStringFromNullInt(cs.CurrentContentID),
		CreatedAt:        timestampFromUnixNano(cs.CreatedAt),
		UpdatedAt:        timestampFromUnixNano(cs.UpdatedAt),
	}
	if includeContent {
		// Resolve which content version to load: the explicitly requested one, or
		// the config's current (latest) version when none was asked for.
		var contentID int64
		switch {
		case version != "":
			contentID, err = strconv.ParseInt(version, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("unable to convert version to number: %w", err)
			}
		case cs.CurrentContentID.Valid:
			contentID = cs.CurrentContentID.Int64
		default:
			// Config exists but records no current content version; nothing to load.
			return nil, nil
		}
		// Constrain by config_id as well as id so a caller cannot read another
		// config's content by passing a version id that belongs to a different one.
		contentSQL, contentArgs, err := s.sql.Select("id", "content", "content_size").From("config_content").Where(sq.Eq{
			"id":        contentID,
			"config_id": numericID,
		}).ToSql()
		if err != nil {
			return nil, fmt.Errorf("error creating sql for selecting config content: %w", err)
		}
		content := configContentSelect{}
		if err := q.GetContext(ctx, &content, contentSQL, contentArgs...); err != nil {
			if strings.Contains(err.Error(), "no rows in result set") {
				return nil, nil
			}
			return nil, fmt.Errorf("error fetching config content: %w", err)
		}
		conf.Content = content.Content
		// Report the size of the version actually loaded, which differs from the
		// current version's size (set above) when an explicit old version is asked for.
		conf.ContentSize = uint64(content.ContentSize)
	}
	return conf, nil
}

// DirectoryStore is the contract for CRUD-ing directories directly, independent
// of the configs they hold. Directories are first-class, ACL-bearing objects:
// they are created, read, updated (their ACLs replaced) and deleted on their own,
// and are no longer materialised as a side effect of writing a config.
//
// DirectoryChain is the read used for hierarchical access enforcement — it
// resolves the ancestor directories of a path (each with its ACLs) so callers can
// require READ on every ancestor to read/list and WRITE on the containing
// directory to write. *Store implements it; the assertion keeps the two in sync.
type DirectoryStore interface {
	CreateDirectory(ctx context.Context, dir *types.Directory) (*types.Directory, error)
	GetDirectoryByID(ctx context.Context, id string) (*types.Directory, error)
	UpdateDirectory(ctx context.Context, dir *types.Directory) (*types.Directory, error)
	DeleteDirectory(ctx context.Context, id string) error
	DirectoryChain(ctx context.Context, fullPath string) ([]*types.Directory, []string, error)
}

var _ DirectoryStore = (*Store)(nil)

func (s *Store) GetDirectoryByID(ctx context.Context, id string) (*types.Directory, error) {
	numericID, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("unable to convert ID to number: %w", err)
	}
	sql, args, err := s.sql.Select("id", "name", "path").From("directory").Where(sq.Eq{
		"id": numericID,
	}).ToSql()
	if err != nil {
		return nil, fmt.Errorf("error creating sql for selecting directory: %w", err)
	}
	ds := directorySelect{}
	if err := s.querier(ctx).GetContext(ctx, &ds, sql, args...); err != nil {
		if strings.Contains(err.Error(), "no rows in result set") {
			return nil, nil
		}
		return nil, fmt.Errorf("error fetching directory: %w", err)
	}
	acls, err := s.directoryACLs(ctx, ds.ID)
	if err != nil {
		return nil, fmt.Errorf("error fetching directory acls: %w", err)
	}
	return &types.Directory{
		Id:   fmt.Sprintf("%d", ds.ID),
		Name: ds.Name,
		Path: ds.Path,
		Acls: acls,
	}, nil
}

// DirectoryChain returns the directories making up fullPath — one per existing
// path component, ordered root-first — each with its ACLs loaded, plus the list
// of components that do not exist. The synthesised root ("" or "/") has no row,
// so it yields an empty chain and no missing entries. Callers use the chain to
// enforce hierarchical access: every ancestor directory must grant READ to read
// a config or list, and the containing directory must grant WRITE to write one.
func (s *Store) DirectoryChain(ctx context.Context, fullPath string) ([]*types.Directory, []string, error) {
	if fullPath == "" || fullPath == "/" {
		return []*types.Directory{}, nil, nil
	}
	return s.getDirectoriesByPathString(ctx, []string{fullPath})
}

// CreateDirectory creates the directory at dir.path/dir.name, materialising any
// missing ancestor directories along the way (each carrying dir's acls). It fails
// if a directory already exists at that full path. dir.id and any child
// directories/configs on dir are ignored. The created leaf is returned with its
// ACLs populated.
func (s *Store) CreateDirectory(ctx context.Context, dir *types.Directory) (*types.Directory, error) {
	name := strings.TrimSpace(dir.GetName())
	if name == "" {
		return nil, fmt.Errorf("a directory name must be provided")
	}
	// dir.GetPath() is the parent's full path. Normalise an empty parent to the
	// root so a top-level directory can be created with either "" or "/" — note
	// Directory.FullPath() collapses an empty path to "/" regardless of name, so
	// the full path is composed here rather than read from it.
	parent := dir.GetPath()
	if parent == "" {
		parent = "/"
	}
	var fullPath string
	if parent == "/" {
		fullPath = "/" + name
	} else {
		fullPath = parent + "/" + name
	}
	txCtx, finish, err := s.withTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("error starting create directory transaction: %w", err)
	}
	created, err := func() (*types.Directory, error) {
		// Reject duplicates: a directory already living at this full path.
		_, missing, err := s.getDirectoriesByPathString(txCtx, []string{fullPath})
		if err != nil {
			return nil, fmt.Errorf("error checking for existing directory: %w", err)
		}
		if !slices.Contains(missing, fullPath) {
			return nil, fmt.Errorf("a directory already exists at %q", fullPath)
		}
		components, err := s.ensureDirectories(txCtx, fullPath, dir.GetAcls())
		if err != nil {
			return nil, fmt.Errorf("error creating directory %q: %w", fullPath, err)
		}
		for _, c := range components {
			if c.FullPath() == fullPath {
				// Re-load to return the canonical stored ACL set (UNKNOWN entries
				// dropped, etc.).
				return s.GetDirectoryByID(txCtx, c.GetId())
			}
		}
		return nil, fmt.Errorf("created directory %q not found after creation", fullPath)
	}()
	finish(err)
	return created, err
}

// UpdateDirectory replaces the ACL set of the directory identified by dir.id. A
// directory's name and path are immutable; if dir carries them they must match
// the stored values. The updated directory is returned with its ACLs populated.
func (s *Store) UpdateDirectory(ctx context.Context, dir *types.Directory) (*types.Directory, error) {
	numericID, err := strconv.ParseInt(dir.GetId(), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("unable to convert ID to number: %w", err)
	}
	txCtx, finish, err := s.withTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("error starting update directory transaction: %w", err)
	}
	updated, err := func() (*types.Directory, error) {
		existing, err := s.GetDirectoryByID(txCtx, dir.GetId())
		if err != nil {
			return nil, fmt.Errorf("error fetching existing directory: %w", err)
		}
		if existing == nil {
			return nil, fmt.Errorf("directory with id %q does not exist", dir.GetId())
		}
		if dir.GetName() != "" && dir.GetName() != existing.GetName() {
			return nil, fmt.Errorf("supplied directory name does not match existing value of %q", existing.GetName())
		}
		if dir.GetPath() != "" && dir.GetPath() != existing.GetPath() {
			return nil, fmt.Errorf("supplied directory path does not match existing value of %q", existing.GetPath())
		}
		if err := s.replaceDirectoryACLs(txCtx, int(numericID), dir.GetAcls()); err != nil {
			return nil, fmt.Errorf("error updating directory acls: %w", err)
		}
		return s.GetDirectoryByID(txCtx, dir.GetId())
	}()
	finish(err)
	return updated, err
}

// DeleteDirectory removes the directory with the given id. It refuses to delete a
// directory that still contains configs or subdirectories (the caller must empty
// it first). The directory's own ACL rows are removed alongside it.
func (s *Store) DeleteDirectory(ctx context.Context, id string) error {
	numericID, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return fmt.Errorf("unable to convert ID to number: %w", err)
	}
	txCtx, finish, err := s.withTx(ctx)
	if err != nil {
		return fmt.Errorf("error starting delete directory transaction: %w", err)
	}
	err = func() error {
		existing, err := s.GetDirectoryByID(txCtx, id)
		if err != nil {
			return fmt.Errorf("error fetching directory: %w", err)
		}
		if existing == nil {
			return fmt.Errorf("directory with id %q does not exist", id)
		}
		fullPath := existing.FullPath()
		// Refuse to delete a non-empty directory: any child directory (by
		// parent_id) or any config living at or beneath this directory's full path.
		var childDirs int
		if err := s.querier(txCtx).GetContext(txCtx, &childDirs, "SELECT COUNT(*) FROM directory WHERE parent_id = ?", numericID); err != nil {
			return fmt.Errorf("error counting child directories: %w", err)
		}
		if childDirs > 0 {
			return fmt.Errorf("directory %q is not empty: it contains %d subdirectories", fullPath, childDirs)
		}
		var childConfigs int
		if err := s.querier(txCtx).GetContext(txCtx, &childConfigs,
			"SELECT COUNT(*) FROM config WHERE path = ? OR path LIKE ?", fullPath, fullPath+"/%"); err != nil {
			return fmt.Errorf("error counting child configs: %w", err)
		}
		if childConfigs > 0 {
			return fmt.Errorf("directory %q is not empty: it contains %d configs", fullPath, childConfigs)
		}
		if err := s.deleteDirectoryACLs(txCtx, int(numericID)); err != nil {
			return err
		}
		sql, args, err := s.sql.Delete("directory").Where(sq.Eq{"id": numericID}).ToSql()
		if err != nil {
			return fmt.Errorf("error building sql for deleting directory: %w", err)
		}
		if _, err := s.querier(txCtx).ExecContext(txCtx, sql, args...); err != nil {
			return fmt.Errorf("error deleting directory: %w", err)
		}
		return nil
	}()
	finish(err)
	return err
}

func (s *Store) insert(ctx context.Context, config *types.Config) (int, error) {
	configInode, err := s.nextInode(ctx)
	if err != nil {
		return 0, fmt.Errorf("error getting inode for config creation: %w", err)
	}
	// The first content version gets its own inode, independent of the config's;
	// config.current_content_id points at it. That column carries no FK, so it can
	// be set here before the config_content row is inserted below.
	contentInode, err := s.nextInode(ctx)
	if err != nil {
		return 0, fmt.Errorf("error getting inode for config content creation: %w", err)
	}
	now := s.now().UnixNano()
	sql, args, err := s.sql.Insert("config").Columns("id", "name", "path", "current_content_id", "created_at", "updated_at").Values(
		configInode,
		config.GetName(),
		config.GetPath(),
		contentInode,
		now,
		now,
	).ToSql()
	if err != nil {
		return 0, fmt.Errorf("error creating sql to insert config: %w", err)
	}
	q := s.querier(ctx)
	if _, err := q.ExecContext(ctx, sql, args...); err != nil {
		return 0, fmt.Errorf("error inserting config: %w", err)
	}
	contentSQL, contentArgs, err := s.sql.Insert("config_content").Columns("id", "config_id", "content", "content_size", "created_at", "updated_at").Values(
		contentInode,
		configInode,
		config.GetContent(),
		int(config.GetContentSize()),
		now,
		now,
	).ToSql()
	if err != nil {
		return 0, fmt.Errorf("error creating sql to insert config content: %w", err)
	}
	if _, err := q.ExecContext(ctx, contentSQL, contentArgs...); err != nil {
		return 0, fmt.Errorf("error inserting config content: %w", err)
	}
	if err := s.insertConfigACLs(ctx, configInode, config.GetAcls()); err != nil {
		return 0, fmt.Errorf("error inserting config acls: %w", err)
	}
	return configInode, nil
}

func (s *Store) update(ctx context.Context, config *types.Config) error {
	numericID, err := strconv.ParseInt(config.GetId(), 10, 64)
	if err != nil {
		return fmt.Errorf("unable to convert ID to number: %w", err)
	}
	q := s.querier(ctx)
	now := s.now().UnixNano()
	// Every update bumps config.updated_at, including ACL-only ones where the
	// content is untouched.
	configSet := map[string]interface{}{
		"updated_at": now,
	}
	// The proto content field is optional; a nil slice means "leave content
	// unchanged", letting callers update ACLs (or other metadata) without
	// resending the blob. When content IS supplied we never mutate an existing
	// config_content row — versions are immutable — but instead insert a fresh
	// version (its own inode, created_at == updated_at) and repoint
	// config.current_content_id at it, so prior versions stay fetchable. The
	// config_content blob is never written NULL, so an ACL-only update can't abort
	// the upsert transaction and silently drop the ACL change.
	if config.Content != nil {
		contentInode, err := s.nextInode(ctx)
		if err != nil {
			return fmt.Errorf("error getting inode for new config content version: %w", err)
		}
		contentSQL, contentArgs, err := s.sql.Insert("config_content").Columns("id", "config_id", "content", "content_size", "created_at", "updated_at").Values(
			contentInode,
			numericID,
			config.GetContent(),
			int(config.GetContentSize()),
			now,
			now,
		).ToSql()
		if err != nil {
			return fmt.Errorf("error creating sql to insert config content version: %w", err)
		}
		if _, err := q.ExecContext(ctx, contentSQL, contentArgs...); err != nil {
			return fmt.Errorf("error inserting config content version: %w", err)
		}
		// content_size moved to config_content (set on the new version row above);
		// the config row only needs its current_content_id repointed.
		configSet["current_content_id"] = contentInode
	}
	sql, args, err := s.sql.Update("config").SetMap(configSet).Where(sq.Eq{"id": numericID}).ToSql()
	if err != nil {
		return fmt.Errorf("error creating sql to update config: %w", err)
	}
	if _, err := q.ExecContext(ctx, sql, args...); err != nil {
		return fmt.Errorf("error updating config: %w", err)
	}
	if err := s.replaceConfigACLs(ctx, int(numericID), config.GetAcls()); err != nil {
		return fmt.Errorf("error updating config acls: %w", err)
	}
	return nil
}

func (s *Store) Delete(ctx context.Context, id string) error {
	numeric, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return err
	}
	txCtx, finish, err := s.withTx(ctx)
	if err != nil {
		return fmt.Errorf("error starting delete transaction: %w", err)
	}
	err = func() error {
		// Remove dependents explicitly rather than relying on ON DELETE CASCADE:
		// PRAGMA foreign_keys is per-connection and the pooled DB may not have it
		// enabled on the connection serving this request.
		if err := s.deleteConfigACLs(txCtx, int(numeric)); err != nil {
			return err
		}
		// Remove every content version of this config, not just one row: with
		// versioning, config_content.id is the version's own inode and config_id is
		// what ties its rows back to the config.
		contentSQL, contentArgs, err := s.sql.Delete("config_content").Where(sq.Eq{"config_id": int(numeric)}).ToSql()
		if err != nil {
			return fmt.Errorf("error building sql for deleting config content: %w", err)
		}
		if _, err := s.querier(txCtx).ExecContext(txCtx, contentSQL, contentArgs...); err != nil {
			return fmt.Errorf("error deleting config content: %w", err)
		}
		sql, args, err := s.sql.Delete("config").Where(sq.Eq{"id": int(numeric)}).ToSql()
		if err != nil {
			return fmt.Errorf("error building sql for deleting config: %w", err)
		}
		if _, err := s.querier(txCtx).ExecContext(txCtx, sql, args...); err != nil {
			return fmt.Errorf("error deleting config: %w", err)
		}
		return nil
	}()
	finish(err)
	return err
}

func (s *Store) Upsert(ctx context.Context, config *types.Config) (*types.Config, error) {
	txCtx, finish, err := s.withTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("error starting upsert transaction: %w", err)
	}
	savedConfig, err := func() (*types.Config, error) {
		// The config's directory must already exist; it is no longer auto-created
		// here. Callers create directories explicitly via CreateDirectory (which is
		// also where ancestor auto-creation now lives), so a config written to a
		// non-existent directory is an error rather than a silent mkdir -p.
		if err := s.requireDirectoryExists(txCtx, config.GetPath()); err != nil {
			return nil, err
		}
		var fetchInode int
		if config.GetId() == "" {
			// Insert
			existing, err := s.getConfigByPathAndName(txCtx, config.GetPath(), config.GetName())
			if err != nil {
				return nil, fmt.Errorf("error searching for existing config: %w", err)
			}
			if existing != nil {
				return nil, fmt.Errorf("another config already exists at the same path/name")
			}
			createdInode, err := s.insert(txCtx, config)
			if err != nil {
				return nil, fmt.Errorf("error inserting new config: %w", err)
			}
			fetchInode = createdInode
		} else {
			// Update
			existing, err := s.GetConfigByID(txCtx, config.GetId(), false, "")
			if err != nil {
				return nil, fmt.Errorf("error fetching existing config: %w", err)
			}
			if existing.GetName() != config.GetName() {
				return nil, fmt.Errorf("supplied config name does not match existing value of %q", existing.GetName())
			}
			if existing.GetPath() != config.GetPath() {
				return nil, fmt.Errorf("supplied config path does not match existing value of %q", existing.GetPath())
			}
			existing.Acls = config.GetAcls()
			existing.ContentSize = config.ContentSize
			existing.Content = config.Content
			if err := s.update(txCtx, existing); err != nil {
				return nil, fmt.Errorf("error updating config: %w", err)
			}
			inode, _ := strconv.ParseInt(existing.GetId(), 10, 64)
			fetchInode = int(inode)
		}
		return s.GetConfigByID(txCtx, fmt.Sprintf("%d", fetchInode), false, "")
	}()
	finish(err)
	return savedConfig, err
}
