package main

import (
	"context"
	"strings"
	"testing"

	p "github.com/pulumi/pulumi-go-provider"
)

// TestProviderSchemaIncludesResources builds the provider and runs schema
// inference, asserting both resources are registered. Building exercises
// infer.Resource registration and GetSchema exercises the per-resource schema
// inference (args/state/nested types), so a malformed DirectoryResource — a
// mismatched method signature or an un-inferable field — fails here rather than
// only at runtime under the Pulumi engine.
func TestProviderSchemaIncludesResources(t *testing.T) {
	provider, err := buildProvider()
	if err != nil {
		t.Fatalf("building provider: %v", err)
	}
	spec, err := p.GetSchema(context.Background(), "configfs", "0.1.0", provider)
	if err != nil {
		t.Fatalf("GetSchema: %v", err)
	}

	for _, want := range []string{"ConfigResource", "DirectoryResource"} {
		found := false
		for tok := range spec.Resources {
			if strings.Contains(tok, want) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("schema has no resource whose token contains %q; tokens: %v", want, resourceTokens(spec.Resources))
		}
	}
}

func resourceTokens[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
