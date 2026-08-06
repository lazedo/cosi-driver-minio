// Copyright 2026 lazedo. Apache-2.0.
package discover

import "testing"

func TestClassNameForSecret(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"mys3-connection", "bind-mys3"},        // broker delivery
		{"minio-central", "bind-minio-central"}, // hand-made
		{"-connection", "bind--connection"},     // degenerate: keep as-is
	} {
		if got := classNameForSecret(tc.in); got != tc.want {
			t.Errorf("classNameForSecret(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestChooseDefault(t *testing.T) {
	a := source{connectionSecret: "objectstorage/a", origin: "secret:objectstorage/a"}
	b := source{connectionSecret: "objectstorage/b", origin: "secret:objectstorage/b"}
	bMarked := b
	bMarked.markedDefault = true

	// Single source: it is the default.
	if got, ok := chooseDefault(map[string]source{"bind-a": a}); !ok || got.connectionSecret != a.connectionSecret {
		t.Errorf("single source: got %+v ok=%v", got, ok)
	}
	// Several, none marked: no default.
	if _, ok := chooseDefault(map[string]source{"bind-a": a, "bind-b": b}); ok {
		t.Error("several unmarked: expected no default")
	}
	// Several, one marked: the marked one.
	if got, ok := chooseDefault(map[string]source{"bind-a": a, "bind-b": bMarked}); !ok || got.connectionSecret != b.connectionSecret {
		t.Errorf("one marked: got %+v ok=%v", got, ok)
	}
	// Several marked: ambiguous, no default.
	aMarked := a
	aMarked.markedDefault = true
	if _, ok := chooseDefault(map[string]source{"bind-a": aMarked, "bind-b": bMarked}); ok {
		t.Error("two marked: expected no default")
	}
	// Empty: no default.
	if _, ok := chooseDefault(map[string]source{}); ok {
		t.Error("empty: expected no default")
	}
}
