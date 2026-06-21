package proto

import (
	"fmt"
	"strconv"
	"strings"
)

func (d *Directory) NumericalInode() uint64 {
	if d == nil {
		return 1
	}
	inode, err := strconv.ParseUint(d.GetId(), 10, 64)
	if err != nil {
		return 0
	}
	return inode
}

func (d *Directory) FullPath() string {
	if d.GetPath() == "" {
		return "/"
	}
	if d.GetPath() == "/" {
		return fmt.Sprintf("/%s", d.GetName())
	}
	return strings.Join([]string{d.GetPath(), d.GetName()}, "/")
}

func (c *Config) NumericalInode() uint64 {
	if c == nil {
		return 1
	}
	inode, err := strconv.ParseUint(c.GetId(), 10, 64)
	if err != nil {
		return 0
	}
	return inode
}

func (c *Config) FullPath() string {
	return strings.Join([]string{c.GetPath(), c.GetName()}, "/")
}

// aclGrants reports whether an ACL entry granting level `have` satisfies a
// request for level `required`. WRITE implies READ; UNKNOWN_ACL grants nothing.
func aclGrants(have, required Acl) bool {
	switch required {
	case Acl_READ:
		return have == Acl_READ || have == Acl_WRITE
	case Acl_WRITE:
		return have == Acl_WRITE
	default:
		return false
	}
}

// Allows reports whether a caller carrying the supplied tags is granted the
// required access level on this config. A ConfigAcl entry matches the caller if
// it applies to everyone, or if its tag is among the caller's tags. A config
// with no ACL entries grants no access to anyone (deny by default).
func (c *Config) Allows(tags []string, required Acl) bool {
	for _, acl := range c.GetAcls() {
		if !aclGrants(acl.GetAcl(), required) {
			continue
		}
		if acl.GetEveryone() {
			return true
		}
		for _, providedTag := range tags {
			if acl.GetTag() != "" && acl.GetTag() == providedTag {
				return true
			}
		}
	}
	return false
}

// VisibileTo reports whether the caller may read this config. It is a
// convenience wrapper around Allows for the READ level. (The misspelling is the
// canonical name used across the codebase.)
func (c *Config) VisibileTo(tags []string) bool {
	return c.Allows(tags, Acl_READ)
}

// Allows reports whether a caller carrying the supplied tags is granted the
// required access level on this directory.
//
// Unlike a Config, a directory is an OPTIONAL path gate: a directory with no ACL
// entries imposes no restriction (it is transparent and grants every level), so
// directories materialised implicitly — or created before directory ACLs existed
// — never lock out the configs beneath them, and the synthesised tree root (which
// has no row and therefore no ACL) is always traversable. A directory that does
// carry entries is enforced exactly like a Config: access is granted only if some
// entry matches (deny-by-default among the entries present). The same entry
// matching and WRITE-implies-READ rules apply.
//
// It is nil-safe: a nil receiver (e.g. the synthesised root) grants every level.
func (d *Directory) Allows(tags []string, required Acl) bool {
	acls := d.GetAcls()
	if len(acls) == 0 {
		return true
	}
	for _, acl := range acls {
		if !aclGrants(acl.GetAcl(), required) {
			continue
		}
		if acl.GetEveryone() {
			return true
		}
		for _, providedTag := range tags {
			if acl.GetTag() != "" && acl.GetTag() == providedTag {
				return true
			}
		}
	}
	return false
}

// VisibileTo reports whether the caller may traverse/read this directory. It is
// the READ-level alias of Allows, matching Config.VisibileTo (including the
// canonical misspelling).
func (d *Directory) VisibileTo(tags []string) bool {
	return d.Allows(tags, Acl_READ)
}
