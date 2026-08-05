// Copyright 2026 lazedo. Apache-2.0.
package driver

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/minio/madmin-go/v4"
	"github.com/minio/minio-go/v7"
	miniocred "github.com/minio/minio-go/v7/pkg/credentials"
	"k8s.io/klog/v2"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Connections resolves per-class backends from a connection secret named in
// the Bucket(Access)Class parameters: parameters["connectionSecret"]="ns/name".
// The secret is the subscription's delivery (endpoint/accessKey/secretKey) —
// absent secret = not-yet-delivered subscription: requests fail and the COSI
// machinery retries, so the whole chain pends until the delivery lands.
// Rotation: the cache is keyed by resourceVersion — a rotated secret builds
// fresh clients on the next request.
type Connections struct {
	k8s kubernetes.Interface

	mu    sync.Mutex
	cache map[string]*Backend // key: ns/name@resourceVersion
}

const paramConnectionSecret = "connectionSecret"

// NewConnections wires the resolver; nil k8s disables it.
func NewConnections(k8s kubernetes.Interface) *Connections {
	return &Connections{k8s: k8s}
}

// backendFor resolves the backend for a "ns/name" connection secret ref.
func (c *Connections) backendFor(ctx context.Context, ref string) (*Backend, error) {
	if c == nil || c.k8s == nil {
		return nil, fmt.Errorf("connectionSecret %q requested but the driver has no kubernetes access", ref)
	}
	ns, name, ok := strings.Cut(ref, "/")
	if !ok {
		return nil, fmt.Errorf("connectionSecret %q: want namespace/name", ref)
	}
	sec, err := c.k8s.CoreV1().Secrets(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("connection secret %s: %w", ref, err)
	}
	key := ref + "@" + sec.ResourceVersion
	c.mu.Lock()
	if b, ok := c.cache[key]; ok {
		c.mu.Unlock()
		return b, nil
	}
	c.mu.Unlock()

	endpoint := string(sec.Data["endpoint"])
	access := string(sec.Data["accessKey"])
	secret := string(sec.Data["secretKey"])
	if endpoint == "" || access == "" || secret == "" {
		return nil, fmt.Errorf("connection secret %s: endpoint/accessKey/secretKey required", ref)
	}
	host := strings.TrimPrefix(strings.TrimPrefix(endpoint, "https://"), "http://")
	secure := strings.HasPrefix(endpoint, "https://")

	// ca.crt no secret = o endpoint é assinado por uma CA privada (o caso do
	// MinIO da casa). Sem isto, o modo connection só funcionava com
	// certificados públicos -- o bind-result tem de ser auto-suficiente.
	var transport http.RoundTripper
	if ca := sec.Data["ca.crt"]; len(ca) > 0 {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(ca) {
			return nil, fmt.Errorf("connection secret %s: ca.crt ilegível", ref)
		}
		transport = &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}
	}

	adm, err := madmin.NewWithOptions(host, &madmin.Options{
		Creds: miniocred.NewStaticV4(access, secret, ""), Secure: secure,
		Transport: transport,
	})
	if err != nil {
		return nil, fmt.Errorf("connection %s admin client: %w", ref, err)
	}
	mc, err := minio.New(host, &minio.Options{
		Creds: miniocred.NewStaticV4(access, secret, ""), Secure: secure,
		Transport: transport,
	})
	if err != nil {
		return nil, fmt.Errorf("connection %s s3 client: %w", ref, err)
	}
	b := &Backend{MC: mc, Adm: adm, S3Endpoint: endpoint}
	c.mu.Lock()
	if c.cache == nil {
		c.cache = map[string]*Backend{}
	}
	c.cache[key] = b
	c.mu.Unlock()
	klog.InfoS("connection backend resolved", "secret", ref, "endpoint", endpoint)
	return b, nil
}

// connBucketID encodes the connection ref so delete/grant/revoke route back.
func connBucketID(ref, bucket string) string { return "conn=" + ref + ":" + bucket }

// splitConnBucketID undoes connBucketID; ref empty if not a connection id.
func splitConnBucketID(id string) (ref, bucket string) {
	if rest, ok := strings.CutPrefix(id, "conn="); ok {
		if i := strings.LastIndex(rest, ":"); i >= 0 {
			return rest[:i], rest[i+1:]
		}
	}
	return "", id
}
