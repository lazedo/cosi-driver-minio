// Copyright 2026 lazedo. Apache-2.0.
package driver

import (
	"context"
	"strings"
	"testing"
)

func TestSplitConnBucketID(t *testing.T) {
	ref, bucket := splitConnBucketID("conn=ns/name:mybucket")
	if ref != "ns/name" || bucket != "mybucket" {
		t.Fatalf("conn id: ref=%q bucket=%q", ref, bucket)
	}
	if ref, _ := splitConnBucketID("plain"); ref != "" {
		t.Fatalf("plain id must not parse as conn id, got %q", ref)
	}
	if ref, _ := splitConnBucketID("minio=ns/name:bucket"); ref != "" {
		t.Fatalf("foreign id must not parse as conn id, got %q", ref)
	}
}

func TestBackendForCreateRequiresStatic(t *testing.T) {
	s := &ProvisionerServer{}
	if _, _, err := s.backendForCreate(context.Background(), map[string]string{}); err == nil ||
		!strings.Contains(err.Error(), "no static minio configured") {
		t.Fatalf("expected requireStatic error, got %v", err)
	}
}

// routerStub proves the extension point is consulted FIRST and its decline
// falls through to the generic path.
type routerStub struct{ handled bool }

func (r *routerStub) RouteCreate(ctx context.Context, params map[string]string) (*Backend, string, bool, error) {
	if r.handled {
		return &Backend{S3Endpoint: "routed"}, "key", true, nil
	}
	return nil, "", false, nil
}

func (r *routerStub) RouteID(ctx context.Context, id string) (*Backend, string, bool, error) {
	if r.handled {
		return &Backend{S3Endpoint: "routed"}, "bucket", true, nil
	}
	return nil, "", false, nil
}

func (r *routerStub) GrantEndpoint(ctx context.Context, params map[string]string, resolved string) string {
	return resolved
}

func TestRouterPrecedenceAndFallthrough(t *testing.T) {
	s := &ProvisionerServer{Router: &routerStub{handled: true}}
	b, key, err := s.backendForCreate(context.Background(), map[string]string{})
	if err != nil || b.S3Endpoint != "routed" || key != "key" {
		t.Fatalf("router must win: b=%v key=%q err=%v", b, key, err)
	}

	s = &ProvisionerServer{Router: &routerStub{handled: false}}
	if _, _, err := s.backendForCreate(context.Background(), map[string]string{}); err == nil ||
		!strings.Contains(err.Error(), "no static minio configured") {
		t.Fatalf("declined route must fall through to generic (and hit requireStatic), got %v", err)
	}
}
