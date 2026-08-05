// Copyright 2026 lazedo. Apache-2.0.
package driver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/minio/madmin-go/v4"
	"github.com/minio/minio-go/v7"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"

	cosi "sigs.k8s.io/container-object-storage-interface/proto"
)

// S3 credential secret keys the sidecar reads (client/apis/objectstorage/consts):
// they land verbatim in the BucketInfo JSON the csi.lazedo.dev driver parses.
const (
	s3Key              = "s3"
	keyEndpoint        = "endpoint"
	keyRegion          = "region"
	keyAccessKeyID     = "accessKeyID"
	keyAccessSecretKey = "accessSecretKey"
)

// Router is the extension point for routing a request to a Backend beyond the
// generic connectionSecret/static logic — e.g. per-tenancy instance cascades.
// Every method may decline (handled=false) to fall through to the generic
// path. This keeps the driver itself platform-agnostic: extensions live in
// their own projects and compose it.
type Router interface {
	// RouteCreate resolves a backend from create-request parameters. key is
	// recorded into the BucketId as "<key>:<bucket>" so later operations
	// route back ("" = plain static id).
	RouteCreate(ctx context.Context, params map[string]string) (b *Backend, key string, handled bool, err error)
	// RouteID resolves a backend (and the bare bucket name) from an issued
	// BucketId.
	RouteID(ctx context.Context, id string) (b *Backend, bucket string, handled bool, err error)
	// GrantEndpoint may override the endpoint advertised in a grant.
	GrantEndpoint(ctx context.Context, params map[string]string, resolved string) string
}

// ProvisionerServer implements the COSI Provisioner gRPC service against
// MinIO: buckets via the S3 API, per-access users+policies via the admin API.
// Generic routing: parameters.connectionSecret > static backend. An optional
// Router extends routing without forking this file.
type ProvisionerServer struct {
	cosi.UnimplementedProvisionerServer
	Provisioner string
	MC          *minio.Client       // S3 (bucket lifecycle), static backend
	Adm         *madmin.AdminClient // admin (users/policies), static backend
	// S3Endpoint is what consumers use to reach the bucket (written into the
	// BucketAccess secret); Region is optional.
	S3Endpoint string
	Region     string

	// Connections resolves class-declared connection secrets
	// (parameters.connectionSecret = "ns/name"); nil disables it.
	Connections *Connections
	// Router extends routing (e.g. tenancy/instance cascades); nil = generic.
	Router Router
}

// static packages the flag-configured backend.
func (s *ProvisionerServer) static() *Backend {
	return &Backend{MC: s.MC, Adm: s.Adm, S3Endpoint: s.S3Endpoint, Region: s.Region}
}

// requireStatic errors early when no static backend was configured (a
// connection-only driver got a request without parameters.connectionSecret).
func (s *ProvisionerServer) requireStatic() error {
	if s.MC == nil || s.Adm == nil {
		return fmt.Errorf("no static minio configured: this driver only serves classes with parameters.connectionSecret")
	}
	return nil
}

// backendForCreate picks the backend for a create given the request
// parameters; key is encoded into the BucketId ("" = plain static).
func (s *ProvisionerServer) backendForCreate(ctx context.Context, params map[string]string) (*Backend, string, error) {
	if s.Router != nil {
		if b, key, handled, err := s.Router.RouteCreate(ctx, params); handled {
			return b, key, err
		}
	}
	if ref := params[paramConnectionSecret]; ref != "" {
		b, err := s.Connections.backendFor(ctx, ref)
		return b, "conn=" + ref, err
	}
	if err := s.requireStatic(); err != nil {
		return nil, "", err
	}
	return s.static(), "", nil
}

// backendForID routes an operation by an already-issued BucketId.
func (s *ProvisionerServer) backendForID(ctx context.Context, id string) (*Backend, string, error) {
	if s.Router != nil {
		if b, bucket, handled, err := s.Router.RouteID(ctx, id); handled {
			return b, bucket, err
		}
	}
	if ref, bucket := splitConnBucketID(id); ref != "" {
		b, err := s.Connections.backendFor(ctx, ref)
		return b, bucket, err
	}
	if err := s.requireStatic(); err != nil {
		return nil, "", err
	}
	return s.static(), id, nil
}

func (s *ProvisionerServer) DriverCreateBucket(ctx context.Context, req *cosi.DriverCreateBucketRequest) (*cosi.DriverCreateBucketResponse, error) {
	name := req.GetName()
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "bucket name is empty")
	}
	be, key, err := s.backendForCreate(ctx, req.GetParameters())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "resolving backend: %v", err)
	}
	// A fronteira de tenancy do backend, verbatim (ver Backend.Prefix). O
	// nome prefixado segue no BucketId, portanto delete/grant/revoke não
	// precisam de saber que o prefixo existe.
	if be.Prefix != "" {
		name = be.Prefix + name
	}
	exists, err := be.MC.BucketExists(ctx, name)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "checking bucket %q: %v", name, err)
	}
	if !exists {
		if err := be.MC.MakeBucket(ctx, name, minio.MakeBucketOptions{Region: be.Region}); err != nil {
			// tolerate a race where another request created it first
			if exists2, e2 := be.MC.BucketExists(ctx, name); e2 != nil || !exists2 {
				return nil, status.Errorf(codes.Internal, "creating bucket %q: %v", name, err)
			}
		}
		klog.InfoS("bucket created", "bucket", name, "route", key)
	} else {
		klog.InfoS("bucket already exists", "bucket", name, "route", key)
	}
	id := name
	if key != "" {
		id = key + ":" + name
	}
	return &cosi.DriverCreateBucketResponse{BucketId: id}, nil
}

func (s *ProvisionerServer) DriverDeleteBucket(ctx context.Context, req *cosi.DriverDeleteBucketRequest) (*cosi.DriverDeleteBucketResponse, error) {
	id := req.GetBucketId()
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "bucket id is empty")
	}
	be, bucket, err := s.backendForID(ctx, id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "resolving backend for %q: %v", id, err)
	}
	// empty the bucket first: RemoveBucket fails on a non-empty bucket, which
	// would wedge a deletionPolicy=Delete BucketClaim forever.
	objs := be.MC.ListObjects(ctx, bucket, minio.ListObjectsOptions{Recursive: true})
	for rErr := range be.MC.RemoveObjects(ctx, bucket, objs, minio.RemoveObjectsOptions{}) {
		if rErr.Err != nil {
			return nil, status.Errorf(codes.Internal, "emptying bucket %q (object %q): %v", bucket, rErr.ObjectName, rErr.Err)
		}
	}
	if err := be.MC.RemoveBucket(ctx, bucket); err != nil {
		return nil, status.Errorf(codes.Internal, "removing bucket %q: %v", bucket, err)
	}
	klog.InfoS("bucket removed", "bucket", bucket)
	return &cosi.DriverDeleteBucketResponse{}, nil
}

func (s *ProvisionerServer) DriverGrantBucketAccess(ctx context.Context, req *cosi.DriverGrantBucketAccessRequest) (*cosi.DriverGrantBucketAccessResponse, error) {
	id := req.GetBucketId()
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "bucket id is empty")
	}
	be, bucket, err := s.backendForID(ctx, id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "resolving backend for %q: %v", id, err)
	}
	accessKey := "cosi-" + randHex(8)
	secretKey := randHex(20)

	if err := be.Adm.AddUser(ctx, accessKey, secretKey); err != nil {
		return nil, status.Errorf(codes.Internal, "creating minio user: %v", err)
	}
	policyName := "cosi-" + bucket
	if err := be.Adm.AddCannedPolicy(ctx, policyName, bucketPolicy(bucket)); err != nil {
		_ = be.Adm.RemoveUser(ctx, accessKey)
		return nil, status.Errorf(codes.Internal, "creating policy: %v", err)
	}
	if _, err := be.Adm.AttachPolicy(ctx, madmin.PolicyAssociationReq{Policies: []string{policyName}, User: accessKey}); err != nil {
		_ = be.Adm.RemoveUser(ctx, accessKey)
		return nil, status.Errorf(codes.Internal, "attaching policy: %v", err)
	}

	// Advertised endpoint: the resolved backend's public endpoint, falling
	// back to the static one; a Router may override (e.g. per-tenant hosts).
	endpoint := be.S3Endpoint
	if endpoint == "" {
		endpoint = s.S3Endpoint
	}
	if s.Router != nil {
		endpoint = s.Router.GrantEndpoint(ctx, req.GetParameters(), endpoint)
	}
	klog.InfoS("granted access", "bucket", bucket, "user", accessKey, "endpoint", endpoint)

	return &cosi.DriverGrantBucketAccessResponse{
		AccountId: accessKey,
		Credentials: map[string]*cosi.CredentialDetails{
			s3Key: {Secrets: map[string]string{
				keyEndpoint:        endpoint,
				keyRegion:          be.Region,
				keyAccessKeyID:     accessKey,
				keyAccessSecretKey: secretKey,
			}},
		},
	}, nil
}

func (s *ProvisionerServer) DriverRevokeBucketAccess(ctx context.Context, req *cosi.DriverRevokeBucketAccessRequest) (*cosi.DriverRevokeBucketAccessResponse, error) {
	account := req.GetAccountId()
	if account == "" {
		return nil, status.Error(codes.InvalidArgument, "account id is empty")
	}
	be, _, err := s.backendForID(ctx, req.GetBucketId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "resolving backend for %q: %v", req.GetBucketId(), err)
	}
	if err := be.Adm.RemoveUser(ctx, account); err != nil {
		return nil, status.Errorf(codes.Internal, "removing minio user %q: %v", account, err)
	}
	klog.InfoS("revoked access", "user", account)
	return &cosi.DriverRevokeBucketAccessResponse{}, nil
}

// bucketPolicy is an IAM policy granting full S3 actions scoped to one bucket.
func bucketPolicy(bucket string) []byte {
	p := map[string]any{
		"Version": "2012-10-17",
		"Statement": []map[string]any{{
			"Effect":   "Allow",
			"Action":   []string{"s3:*"},
			"Resource": []string{fmt.Sprintf("arn:aws:s3:::%s", bucket), fmt.Sprintf("arn:aws:s3:::%s/*", bucket)},
		}},
	}
	b, _ := json.Marshal(p)
	return b
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
