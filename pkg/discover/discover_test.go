// Copyright 2026 lazedo. Apache-2.0.
package discover

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	cosiapi "sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha1"
	cosiclient "sigs.k8s.io/container-object-storage-interface/client/clientset/versioned"
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

// Every LIST of a tick must ask for resourceVersion "0", which the apiserver
// serves from its watch cache.
//
// Without it a 1.29 apiserver answers from etcd: the cluster-wide secret LISTs
// read every Secret in the cluster (helm releases, megabytes) to keep a
// handful, twice per tick. Checked on the wire, with real clients: the
// client-go fakes drop the resourceVersion of a cluster-wide LIST
// (NewRootListActionWithOptions keeps only the selectors).
func TestListsServeFromWatchCache(t *testing.T) {
	// The list kind each LIST path answers with. An idle tick (no sources,
	// nothing managed) makes exactly six LISTs and nothing else: both
	// connection selectors on /api/v1/secrets, then the instances, the
	// synthesized-secret GC and the two class GCs.
	listKinds := map[string]string{
		"/api/v1/secrets":                                         "v1 SecretList",
		"/api/v1/namespaces/cosi-minio/secrets":                   "v1 SecretList",
		"/apis/storage.lazedo.dev/v1alpha1/minios":                "storage.lazedo.dev/v1alpha1 MinioList",
		"/apis/objectstorage.k8s.io/v1alpha1/bucketclasses":       "objectstorage.k8s.io/v1alpha1 BucketClassList",
		"/apis/objectstorage.k8s.io/v1alpha1/bucketaccessclasses": "objectstorage.k8s.io/v1alpha1 BucketAccessClassList",
	}
	var mu sync.Mutex
	var lists []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		lists = append(lists, req.URL.String())
		mu.Unlock()
		gvk, ok := listKinds[req.URL.Path]
		if req.Method != http.MethodGet || !ok {
			t.Errorf("unexpected request %s %s", req.Method, req.URL)
			http.Error(w, "unexpected request", http.StatusInternalServerError)
			return
		}
		if rv := req.URL.Query().Get("resourceVersion"); rv != "0" {
			t.Errorf("LIST %s has resourceVersion %q: served from etcd, not the watch cache", req.URL, rv)
		}
		apiVersion, kind, _ := strings.Cut(gvk, " ")
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"apiVersion":%q,"kind":%q,"metadata":{},"items":[]}`, apiVersion, kind)
	}))
	defer srv.Close()

	cfg := &rest.Config{Host: srv.URL}
	r := &reconciler{
		opts:            Options{DriverName: "d", WatchMinioInstances: true, Namespace: "cosi-minio"},
		k8s:             kubernetes.NewForConfigOrDie(cfg),
		cosi:            cosiclient.NewForConfigOrDie(cfg),
		dyn:             dynamic.NewForConfigOrDie(cfg),
		warnedUnmanaged: map[string]bool{},
	}
	if err := r.reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(lists) != 6 {
		t.Errorf("expected the 6 LISTs of a tick, got %d: %q", len(lists), lists)
	}
}

// What one tick does: an s3 broker connection and a hand-made one each get a
// class pair, a non-s3 broker connection does not, a ready instance gets a
// synthesized connection secret and a class pair, and managed objects whose
// source is gone are garbage-collected.
func TestReconcileTick(t *testing.T) {
	ctx := context.Background()
	const ns = "cosi-minio"
	secret := func(namespace, name string, labels map[string]string, data map[string][]byte) *corev1.Secret {
		return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, Labels: labels}, Data: data}
	}
	broker := map[string]string{LabelBrokerConnection: brokerConnectionTrue}

	kube := kubefake.NewClientset(
		secret("team-a", "mys3-connection", broker, map[string][]byte{dataKeyType: []byte(typeS3)}),
		secret("team-a", "couch-connection", broker, map[string][]byte{dataKeyType: []byte("couch")}),
		secret("team-b", "dev", map[string]string{LabelConnection: ConnectionMinio}, nil),
		secret("store", "m1-root", nil, map[string][]byte{"user": []byte("u"), "password": []byte("p")}),
		// synthesized for an instance that no longer exists
		secret(ns, "minio-gone-conn", map[string]string{LabelManagedBy: ManagedByValue}, nil),
	)
	cosi := cosifake.NewSimpleClientset(
		// managed class whose source no longer exists
		&cosiapi.BucketClass{ObjectMeta: classMeta("minio-gone", source{origin: "test"}), DriverName: "d"},
	)
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{minioGVR: "MinioList"},
		&unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": "storage.lazedo.dev/v1alpha1",
			"kind":       "Minio",
			"metadata":   map[string]interface{}{"namespace": "store", "name": "m1"},
			"status":     map[string]interface{}{"readyNodes": int64(1)},
		}})

	r := &reconciler{
		opts: Options{DriverName: "d", WatchMinioInstances: true, Namespace: ns},
		k8s:  kube, cosi: cosi, dyn: dyn,
		warnedUnmanaged: map[string]bool{},
	}
	if err := r.reconcile(ctx); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	classes := r.cosi.ObjectstorageV1alpha1()
	for _, name := range []string{"bind-mys3", "bind-dev", "minio-m1"} {
		if _, err := classes.BucketClasses().Get(ctx, name, metav1.GetOptions{}); err != nil {
			t.Errorf("bucketclass %s: %v", name, err)
		}
		if _, err := classes.BucketAccessClasses().Get(ctx, name, metav1.GetOptions{}); err != nil {
			t.Errorf("bucketaccessclass %s: %v", name, err)
		}
	}
	if _, err := classes.BucketClasses().Get(ctx, "bind-couch", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("a non-s3 broker connection got a class (err=%v)", err)
	}
	if _, err := classes.BucketClasses().Get(ctx, "minio-gone", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("stale managed class not garbage-collected (err=%v)", err)
	}
	if _, err := kube.CoreV1().Secrets(ns).Get(ctx, "minio-m1-conn", metav1.GetOptions{}); err != nil {
		t.Errorf("instance connection secret not synthesized: %v", err)
	}
	if _, err := kube.CoreV1().Secrets(ns).Get(ctx, "minio-gone-conn", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("stale synthesized secret not garbage-collected (err=%v)", err)
	}
}
