// Copyright 2026 lazedo. Apache-2.0.
package discover

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	cosiapi "sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha1"
	cosifake "sigs.k8s.io/container-object-storage-interface/client/clientset/versioned/fake"
)

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

// A retention policy is not routing, and this component only owns routing.
//
// ensureBucketClass used to demand Retain and write it back when it found
// anything else, so an operator editing a class to Delete had the edit accepted
// by the API server and undone on the next tick -- thirty seconds of looking
// like it had stuck. The cost was not theoretical: every migration away from a
// Retain class leaves a full bucket behind, and there was no declarative way to
// ask for anything else.
//
// So: Retain when this component CREATES a class, and never touched afterwards.
func TestDeletionPolicyIsNotDrift(t *testing.T) {
	ctx := context.Background()
	const name, secret = "minio-x", "cosi-minio/minio-x-conn"
	src := source{connectionSecret: secret, origin: "test"}

	newClass := func(policy cosiapi.DeletionPolicy) *cosiapi.BucketClass {
		return &cosiapi.BucketClass{
			ObjectMeta:     classMeta(name, src),
			DriverName:     "d",
			DeletionPolicy: policy,
			Parameters:     map[string]string{paramConnectionSecret: secret},
		}
	}

	t.Run("created with Retain", func(t *testing.T) {
		cs := cosifake.NewSimpleClientset()
		r := &reconciler{cosi: cs, opts: Options{DriverName: "d"}}
		if err := r.ensureBucketClass(ctx, name, src); err != nil {
			t.Fatalf("ensure: %v", err)
		}
		got, err := cs.ObjectstorageV1alpha1().BucketClasses().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if got.DeletionPolicy != cosiapi.DeletionPolicyRetain {
			t.Fatalf("a class we invent must start safe: got %q", got.DeletionPolicy)
		}
	})

	t.Run("Delete is left alone", func(t *testing.T) {
		cs := cosifake.NewSimpleClientset(newClass(cosiapi.DeletionPolicyDelete))
		r := &reconciler{cosi: cs, opts: Options{DriverName: "d"}}
		if err := r.ensureBucketClass(ctx, name, src); err != nil {
			t.Fatalf("ensure: %v", err)
		}
		got, err := cs.ObjectstorageV1alpha1().BucketClasses().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if got.DeletionPolicy != cosiapi.DeletionPolicyDelete {
			t.Fatalf("the operator's choice was overwritten: got %q", got.DeletionPolicy)
		}
	})

	// The other half: what IS drift still gets corrected, and correcting it must
	// not smuggle the policy back in.
	t.Run("routing is still fixed, policy survives", func(t *testing.T) {
		wrong := newClass(cosiapi.DeletionPolicyDelete)
		wrong.Parameters[paramConnectionSecret] = "cosi-minio/somewhere-else"
		cs := cosifake.NewSimpleClientset(wrong)
		r := &reconciler{cosi: cs, opts: Options{DriverName: "d"}}
		if err := r.ensureBucketClass(ctx, name, src); err != nil {
			t.Fatalf("ensure: %v", err)
		}
		got, err := cs.ObjectstorageV1alpha1().BucketClasses().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if got.Parameters[paramConnectionSecret] != secret {
			t.Fatalf("routing not corrected: got %q", got.Parameters[paramConnectionSecret])
		}
		if got.DeletionPolicy != cosiapi.DeletionPolicyDelete {
			t.Fatalf("fixing routing overwrote the policy: got %q", got.DeletionPolicy)
		}
	})
}
