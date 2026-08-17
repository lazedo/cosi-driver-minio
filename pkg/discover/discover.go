// Copyright 2026 lazedo. Apache-2.0.
// Package discover reconciles COSI classes from discovered MinIO connections.
//
// It is the class-mapping automation described in bind-broker/docs/cosi-local.md
// ("Descoberta — no próprio pod do driver"): same binary as the driver,
// subcommand `discover`, running as a third container in the driver pod. It
// keeps cluster-scoped BucketClass/BucketAccessClass pairs in sync with two
// sources:
//
//  1. ALWAYS: connection secrets across all namespaces. The canonical
//     contract is the bind-broker delivery: label
//     bind.lazedo.dev/connection="true" with data key type="s3" (non-s3
//     connections — couch etc. — share the label and are skipped). A
//     secondary selector, cosi.lazedo.dev/connection=minio, accepts
//     hand-made/dev secrets. Each yields a class pair `bind-<name>`, where
//     <name> is the secret name with a trailing "-connection" trimmed
//     (broker secrets are `<instance>-connection`), pointing at
//     parameters.connectionSecret="<ns>/<secret-name>".
//  2. OPTIONAL (--watch-minio-instances): house minio-operator MinIO
//     instance CRs (storage.lazedo.dev/v1alpha1). Per ready instance,
//     discover derives the admin connection (root credentials from the
//     instance's root secret, in-cluster endpoint
//     `http://<name>-client.<ns>.svc:9000` — the operator serves plain
//     http), synthesizes a connection secret `minio-<name>-conn` in its
//     own namespace, and ensures the class pair `minio-<name>`. Root of a
//     PRIVATE local instance is acceptable by design: the instance is the
//     isolation boundary.
//
// A `default` alias pair is maintained when the choice is unambiguous: the
// single source when exactly one exists, or the source whose secret/CR is
// marked with label/annotation cosi.lazedo.dev/default="true" when several
// exist. Ambiguity (zero or several marked among many) means no managed
// `default`; an unmanaged class named `default` is never touched.
//
// Everything created here carries the label cosi.lazedo.dev/managed-by=discover
// and ONLY objects carrying that label are ever garbage-collected. GC is by
// full reconcile: cluster-scoped classes cannot ownerRef a namespaced secret.
//
// The loop is a periodic full LIST+reconcile (default 30s): simple and robust
// beats clever. Updates always re-GET the object inside RetryOnConflict —
// never Update from a cached copy (this codebase family had a stale-object
// wedge bug).
package discover

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"

	cosiapi "sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha1"
	cosiclient "sigs.k8s.io/container-object-storage-interface/client/clientset/versioned"
)

const (
	// LabelBrokerConnection marks connection secrets composed by the
	// bind-broker on delivery (ConnectionLabelKey in bind-broker's
	// api/v1alpha1/s3instance_types.go). The canonical contract.
	LabelBrokerConnection = "bind.lazedo.dev/connection"
	// brokerConnectionTrue is the LabelBrokerConnection value.
	brokerConnectionTrue = "true"
	// dataKeyType discriminates broker connections; only typeS3 is ours
	// (couch etc. share the label).
	dataKeyType = "type"
	typeS3      = "s3"

	// LabelConnection is the secondary selector for hand-made/dev
	// connection secrets (endpoint/accessKey/secretKey[/ca.crt]).
	LabelConnection = "cosi.lazedo.dev/connection"
	// ConnectionMinio is the LabelConnection value this reconciler acts on.
	ConnectionMinio = "minio"

	// LabelManagedBy marks the objects this reconciler owns; GC never
	// touches an object without it (static classes stay static).
	LabelManagedBy = "cosi.lazedo.dev/managed-by"
	// ManagedByValue is the LabelManagedBy value stamped on created objects.
	ManagedByValue = "discover"

	// MarkerDefault, as a label or annotation valued "true" on a connection
	// secret or MinIO instance CR, elects that source as the `default`
	// class alias when several sources exist.
	MarkerDefault = "cosi.lazedo.dev/default"

	// DefaultClassName is the alias class pair name.
	DefaultClassName = "default"

	// AnnotationSource records where a managed object came from, for humans.
	AnnotationSource = "cosi.lazedo.dev/source"

	// paramConnectionSecret is the BucketClass/BucketAccessClass parameter
	// the driver's connection resolver reads ("<ns>/<name>").
	paramConnectionSecret = "connectionSecret"

	// authenticationTypeKey matches the static classes shipped in the
	// consumer bundle (the sidecar/driver pair accepts it as KEY-style).
	authenticationTypeKey = cosiapi.AuthenticationType("KEY")

	// brokerSecretSuffix is trimmed off broker secret names for class
	// naming: secret `mys3-connection` -> classes `bind-mys3`.
	brokerSecretSuffix = "-connection"
)

// minioGVR is the house minio-operator MinIO instance CR (same GVR the
// bind-broker resolves: storage.lazedo.dev/v1alpha1, resource minios).
var minioGVR = schema.GroupVersionResource{Group: "storage.lazedo.dev", Version: "v1alpha1", Resource: "minios"}

// Options configures the discover reconciler.
type Options struct {
	// DriverName is written into created classes; must match the driver
	// container's --provisioner so the co-located sidecar picks the claims.
	DriverName string
	// Interval between full reconciles.
	Interval time.Duration
	// WatchMinioInstances enables the optional house MinIO instance CR
	// source (storage.lazedo.dev/v1alpha1).
	WatchMinioInstances bool
	// Namespace is where synthesized connection secrets live; resolved to
	// the pod's own namespace when empty.
	Namespace string
}

// source is one discovered connection a class pair should exist for.
type source struct {
	connectionSecret string // "<ns>/<name>" for parameters.connectionSecret
	origin           string // human-readable provenance for AnnotationSource
	markedDefault    bool   // carries the MarkerDefault label/annotation
	externalEndpoint string // instance's public URI (spec.expose.host), "" = none
}

// connSecret is a connection secret discover itself must materialize
// (derived from a MinIO instance CR).
type connSecret struct {
	name   string // secret name in the reconciler's own namespace
	data   map[string][]byte
	origin string
}

type reconciler struct {
	opts Options
	k8s  kubernetes.Interface
	cosi cosiclient.Interface
	dyn  dynamic.Interface

	// loggedNoInstanceCRD dedupes the "CRD absent" log: flag on without
	// the house minio-operator installed is a supported steady state, not
	// an error.
	loggedNoInstanceCRD bool
	// warnedUnmanaged dedupes skip-logs for name collisions with
	// unmanaged objects.
	warnedUnmanaged map[string]bool
}

// Run builds clients and reconciles forever (until ctx is done).
func Run(ctx context.Context, opts Options) error {
	cfg, err := buildConfig()
	if err != nil {
		return fmt.Errorf("kubernetes config: %w", err)
	}
	k8s, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("kubernetes client: %w", err)
	}
	cc, err := cosiclient.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("cosi client: %w", err)
	}
	var dyn dynamic.Interface
	if opts.WatchMinioInstances {
		if dyn, err = dynamic.NewForConfig(cfg); err != nil {
			return fmt.Errorf("dynamic client: %w", err)
		}
	}
	if opts.Namespace == "" {
		opts.Namespace = ownNamespace()
	}
	r := &reconciler{opts: opts, k8s: k8s, cosi: cc, dyn: dyn, warnedUnmanaged: map[string]bool{}}

	klog.InfoS("discover reconciling", "driverName", opts.DriverName,
		"interval", opts.Interval, "watchMinioInstances", opts.WatchMinioInstances,
		"namespace", opts.Namespace)
	ticker := time.NewTicker(opts.Interval)
	defer ticker.Stop()
	for {
		if err := r.reconcile(ctx); err != nil {
			// Errors are logged, never fatal: the next tick retries
			// from a fresh LIST.
			klog.ErrorS(err, "reconcile failed")
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// buildConfig prefers in-cluster config, falling back to the default
// kubeconfig chain for out-of-cluster development runs.
func buildConfig() (*rest.Config, error) {
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}
	loading := clientcmd.NewDefaultClientConfigLoadingRules()
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loading, nil).ClientConfig()
}

// ownNamespace resolves the pod's namespace for synthesized secrets.
func ownNamespace() string {
	if b, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
		if ns := strings.TrimSpace(string(b)); ns != "" {
			return ns
		}
	}
	if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
		return ns
	}
	return "objectstorage"
}

// reconcile computes the desired class set from all sources, materializes
// instance-derived connection secrets, ensures each class pair exists, then
// GCs managed objects with no backing source.
func (r *reconciler) reconcile(ctx context.Context) error {
	desired, err := r.desiredFromSecrets(ctx)
	if err != nil {
		return err
	}

	// Instance-derived connection secrets: always reconcile the set (even
	// when empty) so instance removal GCs the synthesized secrets.
	// Instance discovery failing must not stall secret-driven classes.
	if r.opts.WatchMinioInstances {
		conns, instSources, ierr := r.instanceSources(ctx)
		if ierr != nil {
			klog.ErrorS(ierr, "minio instance discovery failed")
		} else {
			if err := r.reconcileConnSecrets(ctx, conns); err != nil {
				klog.ErrorS(err, "reconciling instance connection secrets failed")
			}
			for name, src := range instSources {
				if prev, ok := desired[name]; ok {
					klog.InfoS("class name collision; keeping first",
						"class", name, "kept", prev.origin, "skipped", src.origin)
					continue
				}
				desired[name] = src
			}
		}
	}

	// The `default` alias: unambiguous choice only.
	if def, ok := chooseDefault(desired); ok {
		desired[DefaultClassName] = source{
			connectionSecret: def.connectionSecret,
			origin:           "default-alias:" + def.origin,
		}
	}

	var errs []error
	for name, src := range desired {
		if err := r.ensureBucketClass(ctx, name, src); err != nil {
			errs = append(errs, fmt.Errorf("bucketclass %s: %w", name, err))
		}
		if err := r.ensureBucketAccessClass(ctx, name, src); err != nil {
			errs = append(errs, fmt.Errorf("bucketaccessclass %s: %w", name, err))
		}
	}
	if err := r.gc(ctx, desired); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return fmt.Errorf("%d error(s), first: %w", len(errs), errs[0])
	}
	return nil
}

// chooseDefault picks the source the `default` alias should point at:
// the only source, or the single one marked cosi.lazedo.dev/default="true".
func chooseDefault(desired map[string]source) (source, bool) {
	if len(desired) == 1 {
		for _, s := range desired {
			return s, true
		}
	}
	var marked []source
	for _, s := range desired {
		if s.markedDefault {
			marked = append(marked, s)
		}
	}
	if len(marked) == 1 {
		return marked[0], true
	}
	if len(marked) > 1 {
		klog.InfoS("several sources marked default; not aliasing", "marked", len(marked))
	}
	return source{}, false
}

// isMarkedDefault reads the MarkerDefault election from labels/annotations.
func isMarkedDefault(obj metav1.Object) bool {
	return obj.GetLabels()[MarkerDefault] == "true" || obj.GetAnnotations()[MarkerDefault] == "true"
}

// classNameForSecret derives the `bind-*` class name from a connection
// secret name, trimming the broker's `-connection` suffix.
func classNameForSecret(secretName string) string {
	trimmed := strings.TrimSuffix(secretName, brokerSecretSuffix)
	if trimmed == "" { // a secret literally named "-connection"
		trimmed = secretName
	}
	return "bind-" + trimmed
}

// desiredFromSecrets lists connection secrets cluster-wide — broker contract
// first (label bind.lazedo.dev/connection=true, data type=s3), then the
// secondary cosi.lazedo.dev/connection=minio selector — and maps each to a
// `bind-<name>` class pair.
func (r *reconciler) desiredFromSecrets(ctx context.Context) (map[string]source, error) {
	broker, err := r.k8s.CoreV1().Secrets(metav1.NamespaceAll).List(ctx, metav1.ListOptions{
		LabelSelector: LabelBrokerConnection + "=" + brokerConnectionTrue,
	})
	if err != nil {
		return nil, fmt.Errorf("listing broker connection secrets: %w", err)
	}
	legacy, err := r.k8s.CoreV1().Secrets(metav1.NamespaceAll).List(ctx, metav1.ListOptions{
		LabelSelector: LabelConnection + "=" + ConnectionMinio,
	})
	if err != nil {
		return nil, fmt.Errorf("listing connection secrets: %w", err)
	}

	desired := map[string]source{}
	seen := map[string]bool{} // ns/name of processed secrets: both-labeled secrets count once
	add := func(sec *corev1.Secret) {
		ref := sec.Namespace + "/" + sec.Name
		if seen[ref] {
			return
		}
		seen[ref] = true
		name := classNameForSecret(sec.Name)
		src := source{
			connectionSecret: ref,
			origin:           "secret:" + ref,
			markedDefault:    isMarkedDefault(sec),
		}
		// Class names are cluster-scoped but secret names are not:
		// same-named secrets in two namespaces collide. First one wins
		// deterministically per LIST order; the loser is only logged —
		// resolving it is a naming decision, not ours to guess.
		if prev, ok := desired[name]; ok {
			klog.InfoS("class name collision; keeping first",
				"class", name, "kept", prev.origin, "skipped", src.origin)
			return
		}
		desired[name] = src
	}
	for i := range broker.Items {
		sec := &broker.Items[i]
		// The broker label is shared by non-s3 deliveries (couch etc.);
		// only s3 connections are ours.
		if string(sec.Data[dataKeyType]) != typeS3 {
			continue
		}
		add(sec)
	}
	for i := range legacy.Items {
		add(&legacy.Items[i])
	}
	return desired, nil
}

// instanceSources derives, per ready house MinIO instance CR, the connection
// secret discover must synthesize and the `minio-<name>` class source
// pointing at it.
//
// Derivation follows the operator/bind-broker conventions exactly
// (minio-operator internal/controller/minio_controller.go, bind-broker
// pkg/binddriver/instances.go):
//   - endpoint: `http://<name>-client.<ns>.svc:9000` — the operator's client
//     Service, plain http (the server never carries public.crt/private.key;
//     TLS termination is the ingress's job, irrelevant in-cluster).
//   - credentials: keys `user`/`password` of the secret named by
//     spec.rootSecretRef.name, default `<name>-root`.
//   - readiness: status.readyNodes >= 1 — an unready NEW instance is
//     skipped (its endpoint would be broken); an instance that was already
//     delivered keeps its classes through transient unreadiness.
func (r *reconciler) instanceSources(ctx context.Context) ([]connSecret, map[string]source, error) {
	instances, err := r.dyn.Resource(minioGVR).Namespace(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		// Flag on without the house minio-operator installed: supported
		// steady state — log once, keep reconciling other sources, and
		// reconcile the (empty) synthesized-secret set so stale
		// derivations from a removed operator still GC.
		if meta.IsNoMatchError(err) || apierrors.IsNotFound(err) {
			if !r.loggedNoInstanceCRD {
				klog.InfoS("minio instance CRD absent; instance discovery idle", "gvr", minioGVR.String())
				r.loggedNoInstanceCRD = true
			}
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("listing minio instances: %w", err)
	}
	r.loggedNoInstanceCRD = false

	var conns []connSecret
	sources := map[string]source{}
	for i := range instances.Items {
		inst := &instances.Items[i]
		cs, err := r.deriveInstanceConn(ctx, inst)
		if err != nil {
			klog.ErrorS(err, "minio instance connection not derivable; skipping",
				"instance", inst.GetNamespace()+"/"+inst.GetName())
			continue
		}
		if ready, _, _ := unstructured.NestedInt64(inst.Object, "status", "readyNodes"); ready < 1 {
			// Not ready: only skip if never delivered — an existing
			// managed conn secret means the instance was ready once,
			// and flapping classes on transient unreadiness would
			// break consumers for no gain.
			existing, gerr := r.k8s.CoreV1().Secrets(r.opts.Namespace).Get(ctx, cs.name, metav1.GetOptions{})
			if gerr != nil || !managed(existing.Labels) {
				klog.V(2).InfoS("minio instance not ready; deferring delivery",
					"instance", inst.GetNamespace()+"/"+inst.GetName())
				continue
			}
		}
		className := "minio-" + inst.GetName()
		if prev, ok := sources[className]; ok {
			klog.InfoS("class name collision; keeping first",
				"class", className, "kept", prev.origin, "skipped", cs.origin)
			continue
		}
		conns = append(conns, *cs)
		exposeHost, _, _ := unstructured.NestedString(inst.Object, "spec", "expose", "host")
		ext := ""
		if exposeHost != "" {
			ext = "https://" + exposeHost
		}
		sources[className] = source{
			connectionSecret: r.opts.Namespace + "/" + cs.name,
			origin:           cs.origin,
			markedDefault:    isMarkedDefault(inst),
			externalEndpoint: ext,
		}
	}
	return conns, sources, nil
}

// deriveInstanceConn builds the synthesized connection secret for one house
// MinIO instance.
func (r *reconciler) deriveInstanceConn(ctx context.Context, inst *unstructured.Unstructured) (*connSecret, error) {
	ns, name := inst.GetNamespace(), inst.GetName()

	rootName, _, _ := unstructured.NestedString(inst.Object, "spec", "rootSecretRef", "name")
	if rootName == "" {
		rootName = name + "-root" // operator default (rootSecretName)
	}
	root, err := r.k8s.CoreV1().Secrets(ns).Get(ctx, rootName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("root secret %s/%s: %w", ns, rootName, err)
	}
	user, pass := root.Data["user"], root.Data["password"]
	if len(user) == 0 || len(pass) == 0 {
		return nil, fmt.Errorf("root secret %s/%s: keys user/password required", ns, rootName)
	}

	data := map[string][]byte{
		"endpoint":  []byte("http://" + name + "-client." + ns + ".svc:9000"),
		"accessKey": user,
		"secretKey": pass,
	}
	// The instance's public URI (spec.expose.host) rides along so the driver
	// can advertise it in grants whose access class asks for the external
	// endpoint — consumers presigning for off-cluster clients need it.
	if host, _, _ := unstructured.NestedString(inst.Object, "spec", "expose", "host"); host != "" {
		data["externalEndpoint"] = []byte("https://" + host)
	}
	return &connSecret{
		name:   "minio-" + name + "-conn",
		data:   data,
		origin: "minio-instance:" + ns + "/" + name,
	}, nil
}

// reconcileConnSecrets ensures the synthesized instance connection secrets
// in the reconciler's own namespace and GCs managed ones with no instance
// left.
func (r *reconciler) reconcileConnSecrets(ctx context.Context, conns []connSecret) error {
	secrets := r.k8s.CoreV1().Secrets(r.opts.Namespace)
	desired := map[string]bool{}
	var errs []error

	for i := range conns {
		cs := &conns[i]
		desired[cs.name] = true
		existing, err := secrets.Get(ctx, cs.name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			_, err = secrets.Create(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:        cs.name,
					Namespace:   r.opts.Namespace,
					Labels:      map[string]string{LabelManagedBy: ManagedByValue},
					Annotations: map[string]string{AnnotationSource: cs.origin},
				},
				Data: cs.data,
			}, metav1.CreateOptions{})
			if err != nil && !apierrors.IsAlreadyExists(err) {
				errs = append(errs, fmt.Errorf("creating secret %s: %w", cs.name, err))
			}
			if err == nil {
				klog.InfoS("connection secret synthesized", "secret", cs.name, "source", cs.origin)
			}
			continue
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("getting secret %s: %w", cs.name, err))
			continue
		}
		if !managed(existing.Labels) {
			r.warnOnceUnmanaged("secret/"+cs.name, cs.origin)
			continue
		}
		if secretDataEqual(existing.Data, cs.data) {
			continue
		}
		// Drifted (e.g. rotated root credentials): fix it. Fresh GET
		// inside the retry loop — never Update from a cached copy.
		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			cur, err := secrets.Get(ctx, cs.name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			if !managed(cur.Labels) {
				return nil
			}
			cur.Data = cs.data
			if cur.Annotations == nil {
				cur.Annotations = map[string]string{}
			}
			cur.Annotations[AnnotationSource] = cs.origin
			_, err = secrets.Update(ctx, cur, metav1.UpdateOptions{})
			return err
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("updating secret %s: %w", cs.name, err))
			continue
		}
		klog.InfoS("connection secret updated", "secret", cs.name, "source", cs.origin)
	}

	// GC: managed synthesized secrets whose instance disappeared.
	list, err := secrets.List(ctx, metav1.ListOptions{LabelSelector: LabelManagedBy + "=" + ManagedByValue})
	if err != nil {
		errs = append(errs, fmt.Errorf("listing managed secrets: %w", err))
	} else {
		for i := range list.Items {
			name := list.Items[i].Name
			if desired[name] {
				continue
			}
			err := secrets.Delete(ctx, name, metav1.DeleteOptions{})
			if err != nil && !apierrors.IsNotFound(err) {
				errs = append(errs, fmt.Errorf("deleting secret %s: %w", name, err))
				continue
			}
			klog.InfoS("connection secret garbage-collected", "secret", name)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("%d error(s), first: %w", len(errs), errs[0])
	}
	return nil
}

// secretDataEqual compares synthesized secret payloads.
func secretDataEqual(a, b map[string][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if !bytes.Equal(b[k], v) {
			return false
		}
	}
	return true
}

// classMeta is the ObjectMeta stamped on every managed class.
func classMeta(name string, src source) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:        name,
		Labels:      map[string]string{LabelManagedBy: ManagedByValue},
		Annotations: map[string]string{AnnotationSource: src.origin},
	}
}

// managed reports whether this reconciler owns the object; unmanaged objects
// (e.g. the bundle's static classes) are never touched.
func managed(labels map[string]string) bool {
	return labels[LabelManagedBy] == ManagedByValue
}

// warnOnceUnmanaged logs a name collision with an unmanaged object once
// (per process) instead of every tick.
func (r *reconciler) warnOnceUnmanaged(what, origin string) {
	if r.warnedUnmanaged[what] {
		return
	}
	r.warnedUnmanaged[what] = true
	klog.InfoS("unmanaged object holds a discovered name; leaving it alone", "object", what, "wantedFor", origin)
}

func (r *reconciler) ensureBucketClass(ctx context.Context, name string, src source) error {
	classes := r.cosi.ObjectstorageV1alpha1().BucketClasses()
	existing, err := classes.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		// Retain on CREATION only. Losing data by default is far worse than
		// leaving it behind, so a class this component invents starts safe --
		// but see ensureBucketClass's drift check: the policy is not enforced
		// afterwards, because which of the two an operator wants is theirs to
		// decide and not a routing concern.
		_, err = classes.Create(ctx, &cosiapi.BucketClass{
			ObjectMeta:     classMeta(name, src),
			DriverName:     r.opts.DriverName,
			DeletionPolicy: cosiapi.DeletionPolicyRetain,
			Parameters:     map[string]string{paramConnectionSecret: src.connectionSecret},
		}, metav1.CreateOptions{})
		if apierrors.IsAlreadyExists(err) {
			return nil // lost a create race; next tick converges
		}
		if err == nil {
			klog.InfoS("bucketclass created", "name", name, "source", src.origin)
		}
		return err
	}
	if err != nil {
		return err
	}
	if !managed(existing.Labels) {
		r.warnOnceUnmanaged("bucketclass/"+name, src.origin)
		return nil
	}
	// DELETIONPOLICY IS NOT DRIFT. This used to demand Retain here and write it
	// back below, so editing a class to Delete was accepted by the API server
	// and undone on the next tick -- thirty seconds during which it looked like
	// it had stuck, which is worse than refusing the edit outright.
	//
	// What this component owns is ROUTING: the driver that serves the class and
	// the connection it points at. Those, wrong, send buckets to the wrong MinIO
	// and are worth correcting under anybody's feet. A retention policy breaks
	// nothing and belongs to whoever operates the storage -- and the cost of
	// imposing it is real: every migration away from a Retain class leaves a
	// full bucket behind, with no declarative way to ask for anything else.
	if existing.DriverName == r.opts.DriverName &&
		existing.Parameters[paramConnectionSecret] == src.connectionSecret {
		return nil
	}
	// Drifted (or the default alias repointing): fix it. Fresh GET inside
	// the retry loop — never Update from a cached copy.
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cur, err := classes.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if !managed(cur.Labels) {
			return nil
		}
		cur.DriverName = r.opts.DriverName
		// cur.DeletionPolicy deliberately untouched -- see the drift check above.
		if cur.Parameters == nil {
			cur.Parameters = map[string]string{}
		}
		cur.Parameters[paramConnectionSecret] = src.connectionSecret
		if cur.Annotations == nil {
			cur.Annotations = map[string]string{}
		}
		cur.Annotations[AnnotationSource] = src.origin
		_, err = classes.Update(ctx, cur, metav1.UpdateOptions{})
		if err == nil {
			klog.InfoS("bucketclass updated", "name", name, "source", src.origin)
		}
		return err
	})
}

func (r *reconciler) ensureBucketAccessClass(ctx context.Context, name string, src source) error {
	classes := r.cosi.ObjectstorageV1alpha1().BucketAccessClasses()
	existing, err := classes.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = classes.Create(ctx, &cosiapi.BucketAccessClass{
			ObjectMeta:         classMeta(name, src),
			DriverName:         r.opts.DriverName,
			AuthenticationType: authenticationTypeKey,
			Parameters:         map[string]string{paramConnectionSecret: src.connectionSecret},
		}, metav1.CreateOptions{})
		if apierrors.IsAlreadyExists(err) {
			return nil
		}
		if err == nil {
			klog.InfoS("bucketaccessclass created", "name", name, "source", src.origin)
		}
		return err
	}
	if err != nil {
		return err
	}
	if !managed(existing.Labels) {
		r.warnOnceUnmanaged("bucketaccessclass/"+name, src.origin)
		return nil
	}
	if existing.DriverName == r.opts.DriverName &&
		existing.AuthenticationType == authenticationTypeKey &&
		existing.Parameters[paramConnectionSecret] == src.connectionSecret {
		return nil
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cur, err := classes.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if !managed(cur.Labels) {
			return nil
		}
		cur.DriverName = r.opts.DriverName
		cur.AuthenticationType = authenticationTypeKey
		if cur.Parameters == nil {
			cur.Parameters = map[string]string{}
		}
		cur.Parameters[paramConnectionSecret] = src.connectionSecret
		if cur.Annotations == nil {
			cur.Annotations = map[string]string{}
		}
		cur.Annotations[AnnotationSource] = src.origin
		_, err = classes.Update(ctx, cur, metav1.UpdateOptions{})
		if err == nil {
			klog.InfoS("bucketaccessclass updated", "name", name, "source", src.origin)
		}
		return err
	})
}

// gc deletes managed classes whose source disappeared. Only classes labeled
// LabelManagedBy=ManagedByValue are candidates — static classes are safe.
func (r *reconciler) gc(ctx context.Context, desired map[string]source) error {
	sel := metav1.ListOptions{LabelSelector: LabelManagedBy + "=" + ManagedByValue}
	var errs []error

	bcs, err := r.cosi.ObjectstorageV1alpha1().BucketClasses().List(ctx, sel)
	if err != nil {
		return fmt.Errorf("listing managed bucketclasses: %w", err)
	}
	for i := range bcs.Items {
		name := bcs.Items[i].Name
		if _, ok := desired[name]; ok {
			continue
		}
		err := r.cosi.ObjectstorageV1alpha1().BucketClasses().Delete(ctx, name, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("deleting bucketclass %s: %w", name, err))
			continue
		}
		klog.InfoS("bucketclass garbage-collected", "name", name)
	}

	bacs, err := r.cosi.ObjectstorageV1alpha1().BucketAccessClasses().List(ctx, sel)
	if err != nil {
		return fmt.Errorf("listing managed bucketaccessclasses: %w", err)
	}
	for i := range bacs.Items {
		name := bacs.Items[i].Name
		if _, ok := desired[name]; ok {
			continue
		}
		err := r.cosi.ObjectstorageV1alpha1().BucketAccessClasses().Delete(ctx, name, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("deleting bucketaccessclass %s: %w", name, err))
			continue
		}
		klog.InfoS("bucketaccessclass garbage-collected", "name", name)
	}

	if len(errs) > 0 {
		return fmt.Errorf("gc: %d error(s), first: %w", len(errs), errs[0])
	}
	return nil
}
