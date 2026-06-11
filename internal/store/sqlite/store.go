package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
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
-- the columns (value unknown). config.updated_at bumps on every update,
-- config_content.updated_at only when the content blob is rewritten.
CREATE TABLE IF NOT EXISTS config (
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL,
	content_size INTEGER NOT NULL,
	path STRING NOT NULL,
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
CREATE TABLE IF NOT EXISTS config_content (
	id INTEGER PRIMARY KEY,
	content BLOB NOT NULL,
	created_at INTEGER NOT NULL DEFAULT 0,
	updated_at INTEGER NOT NULL DEFAULT 0,
	FOREIGN KEY (id) REFERENCES config (id) ON DELETE CASCADE ON UPDATE NO ACTION
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
`

// migrations are idempotent ALTERs that bring databases created before a
// column existed up to the current schema. CREATE TABLE IF NOT EXISTS is a
// no-op for existing tables, so any column added to the schema above must also
// be added here; the "duplicate column name" error this produces on databases
// that already have the column is expected and ignored.
var migrations = []string{
	`ALTER TABLE config ADD COLUMN created_at INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE config ADD COLUMN updated_at INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE config_content ADD COLUMN created_at INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE config_content ADD COLUMN updated_at INTEGER NOT NULL DEFAULT 0`,
}

type Store struct {
	db  *sqlx.DB
	sql sq.StatementBuilderType
	// now is the time source for created_at/updated_at stamps; tests override it.
	now func() time.Time
}

func NewStore(db *sqlx.DB) (*Store, error) {
	db.MustExec(schema)
	for _, migration := range migrations {
		if _, err := db.Exec(migration); err != nil {
			if strings.Contains(err.Error(), "duplicate column name") {
				continue
			}
			return nil, fmt.Errorf("error applying migration %q: %w", migration, err)
		}
	}
	return &Store{
		db:  db,
		sql: sq.StatementBuilder,
		now: time.Now,
	}, nil
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
	where := sq.And{}
	if prefix != "" {
		// Constrain to configs living in the prefix directory or any descendant.
		where = append(where, sq.Or{
			sq.Eq{"path": prefix},
			sq.Like{"path": prefix + "/%"},
		})
	}
	builder := s.sql.Select("id", "name", "content_size", "path", "created_at", "updated_at").From("config")
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
	fullPathsToFetch := map[string]bool{}
	for _, c := range cs {
		// Enforce the path prefix precisely; the SQL LIKE above is only a prefilter.
		if prefix != "" && c.Path != prefix && !strings.HasPrefix(c.Path, prefix+"/") {
			continue
		}
		config := &types.Config{
			Id:          fmt.Sprintf("%d", c.ID),
			Name:        c.Name,
			Path:        c.Path,
			ContentSize: uint64(c.ContentSize),
			Acls:        aclsByConfig[c.ID],
			CreatedAt:   timestampFromUnixNano(c.CreatedAt),
			UpdatedAt:   timestampFromUnixNano(c.UpdatedAt),
		}
		// Only surface configs the caller is permitted to read.
		if !config.Allows(aclTags, types.Acl_READ) {
			continue
		}
		configs = append(configs, config)
		// The root has no row in the directory table (it is synthesised below), so
		// only request real subdirectories; a root-level config otherwise makes the
		// whole tree fail the missing-directory check for every caller.
		if c.Path != "" && c.Path != "/" {
			fullPathsToFetch[c.Path] = true
		}
	}
	log.InfofCtx(ctx, "Full paths to fetch: %+v", fullPathsToFetch)
	directories, missing, err := s.getDirectoriesByPathString(ctx, slices.Collect(maps.Keys(fullPathsToFetch)))
	if err != nil {
		return nil, fmt.Errorf("error loading directories: %w", err)
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("there are %d missing directories: %+v", len(missing), missing)
	}
	treeRoot := &types.Directory{
		Id:   "0",
		Name: "/",
		Path: "",
	}
	populateDirectory(treeRoot, directories, configs)
	return treeRoot, nil
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
	sql, args, err := s.sql.Select("id", "name", "path").From("directory").Where(constraints).OrderBy("path").ToSql()
	if err != nil {
		return nil, nil, fmt.Errorf("error building sql query for fetching directories: %w", err)
	}
	directories := []directorySelect{}
	if err := s.querier(ctx).SelectContext(ctx, &directories, sql, args...); err != nil {
		return nil, nil, fmt.Errorf("error fetching directories: %w", err)
	}
	protoDirectories := []*types.Directory{}
	fetchedPaths := []string{}
	for _, d := range directories {
		dir := &types.Directory{
			Id:   fmt.Sprintf("%d", d.ID),
			Name: d.Name,
			Path: d.Path,
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

func (s *Store) createDirectory(ctx context.Context, path string, name string, parentID *string) (*types.Directory, error) {
	inode, err := s.nextInode(ctx)
	if err != nil {
		return nil, fmt.Errorf("error getting inode for new directory: %w", err)
	}
	sql, args, err := sq.Insert("directory").Columns("id", "name", "path", "parent_id").Values(inode, name, path, parentID).ToSql()
	if err != nil {
		return nil, fmt.Errorf("error building sql query for inserting directory: %w", err)
	}
	res, err := s.querier(ctx).ExecContext(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("error executing directory insert: %w", err)
	}
	insertedInode, err := res.LastInsertId()
	if err != nil {
		return nil, fmt.Errorf("error fetching last inserted inode: %w", err)
	}
	return &types.Directory{Id: fmt.Sprintf("%d", insertedInode), Name: name, Path: path}, nil
}

func (s *Store) ensurePath(ctx context.Context, path string) error {
	log.InfofCtx(ctx, "Ensuring path %q", path)
	pathParts := strings.Split(path, "/")
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
	existingDirectories, missing, err := s.getDirectoriesByPathString(ctx, pathsToFetch)
	if err != nil {
		return err
	}
	if len(missing) == 0 {
		return nil
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
			for _, existing := range existingDirectories {
				if existing.FullPath() == parentPath {
					parentID = proto.String(existing.GetId())
					break
				}
			}
		}
		newDir, err := s.createDirectory(ctx, parentPath, name, parentID)
		if err != nil {
			return fmt.Errorf("error creating directory %s/%s: %w", parentPath, name, err)
		}
		existingDirectories = append(existingDirectories, newDir)
	}
	return nil
}

type configSelect struct {
	ID          int    `db:"id"`
	Name        string `db:"name"`
	ContentSize int    `db:"content_size"`
	Path        string `db:"path"`
	CreatedAt   int64  `db:"created_at"`
	UpdatedAt   int64  `db:"updated_at"`
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
	ID      int    `db:"id"`
	Content []byte `db:"content"`
}

type configACLRow struct {
	ID       int    `db:"id"`
	ConfigID int    `db:"config_id"`
	Acl      int    `db:"acl"`
	Tag      string `db:"tag"`
	Everyone int    `db:"everyone"`
}

func (r configACLRow) toProto() *types.ConfigAcl {
	return &types.ConfigAcl{
		Acl:      types.Acl(r.Acl),
		Tag:      r.Tag,
		Everyone: r.Everyone != 0,
	}
}

// loadConfigACLs fetches the ACL entries for the supplied config IDs, grouped
// by config ID. Config IDs with no ACL rows are simply absent from the map.
func (s *Store) loadConfigACLs(ctx context.Context, configIDs []int) (map[int][]*types.ConfigAcl, error) {
	out := map[int][]*types.ConfigAcl{}
	if len(configIDs) == 0 {
		return out, nil
	}
	sql, args, err := s.sql.Select("id", "config_id", "acl", "tag", "everyone").
		From("config_acl").
		Where(sq.Eq{"config_id": configIDs}).
		OrderBy("id").
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("error building sql for selecting config acls: %w", err)
	}
	rows := []configACLRow{}
	if err := s.querier(ctx).SelectContext(ctx, &rows, sql, args...); err != nil {
		return nil, fmt.Errorf("error fetching config acls: %w", err)
	}
	for _, row := range rows {
		out[row.ConfigID] = append(out[row.ConfigID], row.toProto())
	}
	return out, nil
}

// configACLs is a convenience wrapper around loadConfigACLs for a single config.
func (s *Store) configACLs(ctx context.Context, configID int) ([]*types.ConfigAcl, error) {
	byID, err := s.loadConfigACLs(ctx, []int{configID})
	if err != nil {
		return nil, err
	}
	return byID[configID], nil
}

// insertConfigACLs writes the supplied ACL entries for a config. It is a no-op
// when acls is empty. UNKNOWN_ACL entries are skipped as they grant nothing.
func (s *Store) insertConfigACLs(ctx context.Context, configID int, acls []*types.ConfigAcl) error {
	ib := s.sql.Insert("config_acl").Columns("config_id", "acl", "tag", "everyone")
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
		ib = ib.Values(configID, int(acl.GetAcl()), acl.GetTag(), everyone)
		count++
	}
	if count == 0 {
		return nil
	}
	sql, args, err := ib.ToSql()
	if err != nil {
		return fmt.Errorf("error building sql for inserting config acls: %w", err)
	}
	if _, err := s.querier(ctx).ExecContext(ctx, sql, args...); err != nil {
		return fmt.Errorf("error inserting config acls: %w", err)
	}
	return nil
}

// deleteConfigACLs removes every ACL entry belonging to a config.
func (s *Store) deleteConfigACLs(ctx context.Context, configID int) error {
	sql, args, err := s.sql.Delete("config_acl").Where(sq.Eq{"config_id": configID}).ToSql()
	if err != nil {
		return fmt.Errorf("error building sql for deleting config acls: %w", err)
	}
	if _, err := s.querier(ctx).ExecContext(ctx, sql, args...); err != nil {
		return fmt.Errorf("error deleting config acls: %w", err)
	}
	return nil
}

// replaceConfigACLs atomically swaps the full ACL set for a config, used when a
// config is updated. Callers should run this inside a transaction.
func (s *Store) replaceConfigACLs(ctx context.Context, configID int, acls []*types.ConfigAcl) error {
	if err := s.deleteConfigACLs(ctx, configID); err != nil {
		return err
	}
	return s.insertConfigACLs(ctx, configID, acls)
}

func (s *Store) getConfigByPathAndName(ctx context.Context, path string, name string) (*types.Config, error) {
	sql, args, err := s.sql.Select("id", "name", "content_size", "path", "created_at", "updated_at").From("config").Where(sq.Eq{
		"name": name,
		"path": path,
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
		Id:          fmt.Sprintf("%d", cs.ID),
		Name:        cs.Name,
		ContentSize: uint64(cs.ContentSize),
		Path:        cs.Path,
		CreatedAt:   timestampFromUnixNano(cs.CreatedAt),
		UpdatedAt:   timestampFromUnixNano(cs.UpdatedAt),
	}, nil
}

func (s *Store) GetConfigByID(ctx context.Context, id string, includeContent bool) (*types.Config, error) {
	numericID, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("unable to convert ID to number: %w", err)
	}
	sql, args, err := s.sql.Select("id", "name", "content_size", "path", "created_at", "updated_at").From("config").Where(sq.Eq{
		"id": numericID,
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
		Id:          fmt.Sprintf("%d", cs.ID),
		Name:        cs.Name,
		Acls:        acls,
		ContentSize: uint64(cs.ContentSize),
		Path:        cs.Path,
		CreatedAt:   timestampFromUnixNano(cs.CreatedAt),
		UpdatedAt:   timestampFromUnixNano(cs.UpdatedAt),
	}
	if includeContent {
		contentSQL, contentArgs, err := s.sql.Select("id", "content").From("config_content").Where(sq.Eq{
			"id": numericID,
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
	}
	return conf, nil
}

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
	return &types.Directory{
		Id:   fmt.Sprintf("%d", ds.ID),
		Name: ds.Name,
		Path: ds.Path,
	}, nil
}

func (s *Store) insert(ctx context.Context, config *types.Config) (int, error) {
	configInode, err := s.nextInode(ctx)
	if err != nil {
		return 0, fmt.Errorf("error getting inode for config creation: %w", err)
	}
	now := s.now().UnixNano()
	sql, args, err := s.sql.Insert("config").Columns("id", "name", "content_size", "path", "created_at", "updated_at").Values(
		configInode,
		config.GetName(),
		int(config.GetContentSize()),
		config.GetPath(),
		now,
		now,
	).ToSql()
	if err != nil {
		return 0, fmt.Errorf("error creating sql to insert config: %w", err)
	}
	q := s.querier(ctx)
	res, err := q.ExecContext(ctx, sql, args...)
	if err != nil {
		return 0, fmt.Errorf("error inserting config: %w", err)
	}
	insertedInode, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("error fetching inserted inode: %w", err)
	}
	contentSQL, contentArgs, err := s.sql.Insert("config_content").Columns("id", "content", "created_at", "updated_at").Values(
		insertedInode,
		config.GetContent(),
		now,
		now,
	).ToSql()
	if err != nil {
		return 0, fmt.Errorf("error creating sql to insert config content: %w", err)
	}
	if _, err := q.ExecContext(ctx, contentSQL, contentArgs...); err != nil {
		return 0, fmt.Errorf("error inserting config content: %w", err)
	}
	if err := s.insertConfigACLs(ctx, int(insertedInode), config.GetAcls()); err != nil {
		return 0, fmt.Errorf("error inserting config acls: %w", err)
	}
	return int(insertedInode), nil
}

func (s *Store) update(ctx context.Context, config *types.Config) error {
	numericID, err := strconv.ParseInt(config.GetId(), 10, 64)
	if err != nil {
		return fmt.Errorf("unable to convert ID to number: %w", err)
	}
	q := s.querier(ctx)
	now := s.now().UnixNano()
	// Every update bumps config.updated_at, including ACL-only ones where the
	// content blob is untouched.
	configSet := map[string]interface{}{
		"updated_at": now,
	}
	if config.Content != nil {
		configSet["content_size"] = config.GetContentSize()
	}
	sql, args, err := s.sql.Update("config").SetMap(configSet).Where(sq.Eq{"id": numericID}).ToSql()
	if err != nil {
		return fmt.Errorf("error creating sql to update config: %w", err)
	}
	if _, err := q.ExecContext(ctx, sql, args...); err != nil {
		return fmt.Errorf("error updating config: %w", err)
	}
	// The proto content field is optional; a nil slice means "leave content
	// unchanged", letting callers update ACLs (or other metadata) without
	// resending the blob. Skipping the UPDATE also avoids writing NULL into the
	// NOT NULL content column, which would abort the whole upsert transaction
	// and silently drop the ACL change. config_content.updated_at therefore
	// only moves when the blob is actually rewritten.
	if config.Content != nil {
		contentSQL, contentArgs, err := s.sql.Update("config_content").SetMap(map[string]interface{}{
			"content":    config.GetContent(),
			"updated_at": now,
		}).Where(sq.Eq{"id": numericID}).ToSql()
		if err != nil {
			return fmt.Errorf("error creating sql to update config content: %w", err)
		}
		if _, err := q.ExecContext(ctx, contentSQL, contentArgs...); err != nil {
			return fmt.Errorf("error updating config content: %w", err)
		}
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
		contentSQL, contentArgs, err := s.sql.Delete("config_content").Where(sq.Eq{"id": int(numeric)}).ToSql()
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
		// Ensure that we have the directory path needed to accomodate the config
		if err := s.ensurePath(txCtx, config.GetPath()); err != nil {
			return nil, fmt.Errorf("error ensuring path %q for config: %w", config.GetPath(), err)
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
			existing, err := s.GetConfigByID(txCtx, config.GetId(), false)
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
		return s.GetConfigByID(txCtx, fmt.Sprintf("%d", fetchInode), false)
	}()
	finish(err)
	return savedConfig, err
}
