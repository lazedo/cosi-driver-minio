// Copyright 2026 lazedo. Apache-2.0.
// Package discover reconciles COSI classes from discovered MinIO connections.
//
// It is the class-mapping automation described in bind-broker/docs/cosi-local.md
// ("Descoberta — no próprio pod do driver"): same binary as the driver,
// subcommand `discover`, running as a third container in the driver pod. It
// keeps cluster-scoped BucketClass/BucketAccessClass pairs in sync with two
// sources:
//
//  1. ALWAYS: connection secrets across all namespaces carrying the label
//     cosi.lazedo.dev/connection=minio (the contract stamped by the kbind
//     materializer on delivery) -> class pair `bind-<secret-name>` with
//     parameters.connectionSecret="<ns>/<name>".
//  2. OPTIONAL (--watch-minio-crs): local minio-operator Tenant CRs
//     (minio.min.io/v2) -> class pair `minio-<tenant>`. Skeleton only for
//     now — see reconcileTenants.
//
// Classes created here carry the label cosi.lazedo.dev/managed-by=discover
// and ONLY classes carrying that label are ever garbage-collected. GC is by
// full reconcile: cluster-scoped classes cannot ownerRef a namespaced secret.
//
// The loop is a periodic full LIST+reconcile (default 30s): simple and robust
// beats clever. Updates always re-GET the object inside RetryOnConflict —
// never Update from a cached copy (this codebase family had a stale-object
// wedge bug).
package discover

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	// LabelConnection marks a secret as a MinIO connection delivery
	// (endpoint/accessKey/secretKey[/ca.crt]); set by the kbind
	// materializer, or by hand for out-of-band deliveries.
	LabelConnection = "cosi.lazedo.dev/connection"
	// ConnectionMinio is the LabelConnection value this reconciler acts on.
	ConnectionMinio = "minio"

	// LabelManagedBy marks the classes this reconciler owns; GC never
	// touches a class without it (static classes stay static).
	LabelManagedBy = "cosi.lazedo.dev/managed-by"
	// ManagedByValue is the LabelManagedBy value stamped on created classes.
	ManagedByValue = "discover"

	// AnnotationSource records where a managed class came from, for humans.
	AnnotationSource = "cosi.lazedo.dev/source"

	// paramConnectionSecret is the BucketClass/BucketAccessClass parameter
	// the driver's connection resolver reads ("<ns>/<name>").
	paramConnectionSecret = "connectionSecret"

	// authenticationTypeKey matches the static classes shipped in the
	// consumer bundle (the sidecar/driver pair accepts it as KEY-style).
	authenticationTypeKey = cosiapi.AuthenticationType("KEY")
)

// tenantGVR is the local minio-operator Tenant CR (verified on the west
// cluster: `kubectl api-resources` -> tenants  minio.min.io/v2  Tenant).
var tenantGVR = schema.GroupVersionResource{Group: "minio.min.io", Version: "v2", Resource: "tenants"}

// Options configures the discover reconciler.
type Options struct {
	// DriverName is written into created classes; must match the driver
	// container's --provisioner so the co-located sidecar picks the claims.
	DriverName string
	// Interval between full reconciles.
	Interval time.Duration
	// WatchMinioCRs enables the optional Tenant CR source.
	WatchMinioCRs bool
}

// source is one discovered connection a class pair should exist for.
type source struct {
	connectionSecret string // "<ns>/<name>" for parameters.connectionSecret
	origin           string // human-readable provenance for AnnotationSource
}

type reconciler struct {
	opts Options
	k8s  kubernetes.Interface
	cosi cosiclient.Interface
	dyn  dynamic.Interface
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
	if opts.WatchMinioCRs {
		if dyn, err = dynamic.NewForConfig(cfg); err != nil {
			return fmt.Errorf("dynamic client: %w", err)
		}
	}
	r := &reconciler{opts: opts, k8s: k8s, cosi: cc, dyn: dyn}

	klog.InfoS("discover reconciling", "driverName", opts.DriverName,
		"interval", opts.Interval, "watchMinioCRs", opts.WatchMinioCRs)
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

// reconcile computes the desired class set from all sources, ensures each
// pair exists, then GCs managed classes with no backing source.
func (r *reconciler) reconcile(ctx context.Context) error {
	desired, err := r.desiredFromSecrets(ctx)
	if err != nil {
		return err
	}
	if r.opts.WatchMinioCRs {
		if err := r.reconcileTenants(ctx, desired); err != nil {
			// Tenant discovery failing must not stall secret-driven
			// classes; log and continue with what we have.
			klog.ErrorS(err, "tenant discovery failed")
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

// desiredFromSecrets lists labeled connection secrets cluster-wide and maps
// each to a `bind-<name>` class pair.
func (r *reconciler) desiredFromSecrets(ctx context.Context) (map[string]source, error) {
	secrets, err := r.k8s.CoreV1().Secrets(metav1.NamespaceAll).List(ctx, metav1.ListOptions{
		LabelSelector: LabelConnection + "=" + ConnectionMinio,
	})
	if err != nil {
		return nil, fmt.Errorf("listing connection secrets: %w", err)
	}
	desired := make(map[string]source, len(secrets.Items))
	for i := range secrets.Items {
		sec := &secrets.Items[i]
		name := "bind-" + sec.Name
		src := source{
			connectionSecret: sec.Namespace + "/" + sec.Name,
			origin:           "secret:" + sec.Namespace + "/" + sec.Name,
		}
		// Class names are cluster-scoped but secret names are not:
		// same-named secrets in two namespaces collide. First one wins
		// deterministically per LIST order; the loser is only logged —
		// resolving it is a naming decision, not ours to guess.
		if prev, ok := desired[name]; ok {
			klog.InfoS("class name collision; keeping first",
				"class", name, "kept", prev.origin, "skipped", src.origin)
			continue
		}
		desired[name] = src
	}
	return desired, nil
}

// reconcileTenants is the optional Tenant CR source: one `minio-<tenant>`
// class pair per local minio-operator Tenant.
//
// TODO(discover): deriving the tenant's admin connection secret is not yet
// cleanly determinable. A Tenant's root credentials live in the secret named
// by spec.configuration.name (a config.env blob with MINIO_ROOT_USER/
// MINIO_ROOT_PASSWORD), the S3 service is `minio.<ns>.svc`, and the CA
// depends on how the tenant's certs were issued (cert-manager vs
// operator-generated) — the driver's connection resolver needs a flat
// endpoint/accessKey/secretKey[/ca.crt] secret, so discover would have to
// materialize a derived secret. Until that derivation is settled we only
// log discovered tenants; no classes are created from this source.
func (r *reconciler) reconcileTenants(ctx context.Context, desired map[string]source) error {
	tenants, err := r.dyn.Resource(tenantGVR).Namespace(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("listing minio tenants: %w", err)
	}
	for i := range tenants.Items {
		t := &tenants.Items[i]
		klog.InfoS("minio tenant discovered (connection derivation TODO; no classes created)",
			"tenant", t.GetNamespace()+"/"+t.GetName())
	}
	return nil
}

// classMeta is the ObjectMeta stamped on every managed class.
func classMeta(name string, src source) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:        name,
		Labels:      map[string]string{LabelManagedBy: ManagedByValue},
		Annotations: map[string]string{AnnotationSource: src.origin},
	}
}

// managed reports whether this reconciler owns the object; unmanaged classes
// (e.g. the bundle's static ones) are never touched.
func managed(labels map[string]string) bool {
	return labels[LabelManagedBy] == ManagedByValue
}

func (r *reconciler) ensureBucketClass(ctx context.Context, name string, src source) error {
	classes := r.cosi.ObjectstorageV1alpha1().BucketClasses()
	existing, err := classes.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
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
		klog.V(2).InfoS("bucketclass exists unmanaged; leaving it alone", "name", name)
		return nil
	}
	if existing.DriverName == r.opts.DriverName &&
		existing.DeletionPolicy == cosiapi.DeletionPolicyRetain &&
		existing.Parameters[paramConnectionSecret] == src.connectionSecret {
		return nil
	}
	// Drifted: fix it. Fresh GET inside the retry loop — never Update from
	// a cached copy.
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cur, err := classes.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if !managed(cur.Labels) {
			return nil
		}
		cur.DriverName = r.opts.DriverName
		cur.DeletionPolicy = cosiapi.DeletionPolicyRetain
		if cur.Parameters == nil {
			cur.Parameters = map[string]string{}
		}
		cur.Parameters[paramConnectionSecret] = src.connectionSecret
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
		klog.V(2).InfoS("bucketaccessclass exists unmanaged; leaving it alone", "name", name)
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
