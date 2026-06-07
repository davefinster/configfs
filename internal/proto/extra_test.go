package proto

import "testing"

func TestConfigAllows(t *testing.T) {
	cfg := func(acls ...*ConfigAcl) *Config { return &Config{Acls: acls} }
	read := func(tag string) *ConfigAcl { return &ConfigAcl{Acl: Acl_READ, Tag: tag} }
	write := func(tag string) *ConfigAcl { return &ConfigAcl{Acl: Acl_WRITE, Tag: tag} }

	cases := []struct {
		name     string
		config   *Config
		tags     []string
		required Acl
		want     bool
	}{
		// The security-critical asymmetry: WRITE implies READ, READ must NOT imply WRITE.
		{"read-only grant does not imply write", cfg(read("tag:a")), []string{"tag:a"}, Acl_WRITE, false},
		{"write grant satisfies write", cfg(write("tag:a")), []string{"tag:a"}, Acl_WRITE, true},
		{"write grant implies read", cfg(write("tag:a")), []string{"tag:a"}, Acl_READ, true},
		{"everyone read does not imply write", cfg(&ConfigAcl{Acl: Acl_READ, Everyone: true}), nil, Acl_WRITE, false},
		{"everyone read grants read to anyone", cfg(&ConfigAcl{Acl: Acl_READ, Everyone: true}), nil, Acl_READ, true},
		{"everyone write grants write to anyone", cfg(&ConfigAcl{Acl: Acl_WRITE, Everyone: true}), nil, Acl_WRITE, true},
		// Deny by default.
		{"no acls denies read", cfg(), []string{"tag:a"}, Acl_READ, false},
		{"no acls denies write", cfg(), []string{"tag:a"}, Acl_WRITE, false},
		{"nil acls denies read", &Config{}, []string{"tag:a"}, Acl_READ, false},
		// UNKNOWN_ACL grants nothing.
		{"unknown acl grants nothing", cfg(&ConfigAcl{Acl: Acl_UNKNOWN_ACL, Everyone: true}), []string{"tag:a"}, Acl_READ, false},
		// Tag matching.
		{"non-matching tag denied", cfg(read("tag:a")), []string{"tag:b"}, Acl_READ, false},
		{"matching tag among several allowed", cfg(read("tag:a")), []string{"tag:b", "tag:a"}, Acl_READ, true},
		// Empty-tag guard: an entry that is neither everyone nor a real tag matches no one.
		{"empty-tag entry matches no one (nil caller tags)", cfg(&ConfigAcl{Acl: Acl_READ, Tag: "", Everyone: false}), nil, Acl_READ, false},
		{"empty-tag entry matches no one (empty-string caller tag)", cfg(&ConfigAcl{Acl: Acl_READ, Tag: "", Everyone: false}), []string{""}, Acl_READ, false},
		// Multiple entries: any matching entry grants access.
		{"any matching entry grants access", cfg(read("tag:a"), write("tag:b")), []string{"tag:b"}, Acl_WRITE, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.config.Allows(tc.tags, tc.required); got != tc.want {
				t.Errorf("Allows(%v, %v) = %v, want %v", tc.tags, tc.required, got, tc.want)
			}
		})
	}
}

func TestConfigVisibileToIsReadAlias(t *testing.T) {
	c := &Config{Acls: []*ConfigAcl{{Acl: Acl_READ, Tag: "tag:a"}}}
	if !c.VisibileTo([]string{"tag:a"}) {
		t.Errorf("VisibileTo should be true for a matching read tag")
	}
	if c.VisibileTo([]string{"tag:b"}) {
		t.Errorf("VisibileTo should be false for a non-matching tag")
	}
	// A write-only grant still implies read visibility.
	w := &Config{Acls: []*ConfigAcl{{Acl: Acl_WRITE, Tag: "tag:a"}}}
	if !w.VisibileTo([]string{"tag:a"}) {
		t.Errorf("VisibileTo should be true for a write grant (implies read)")
	}
	// Deny by default.
	if (&Config{}).VisibileTo([]string{"tag:a"}) {
		t.Errorf("VisibileTo should be false for a config with no acls")
	}
}
