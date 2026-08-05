// Copyright 2026 lazedo. Apache-2.0.
package driver

import (
	"github.com/minio/madmin-go/v4"
	"github.com/minio/minio-go/v7"
)

// Backend is one MinIO endpoint the driver provisions against: buckets via
// the S3 API (MC), users+policies via the admin API (Adm). S3Endpoint is what
// consumers are told to connect to (written into BucketAccess credentials).
// Exported so routing extensions (see Router in provisioner.go) can resolve
// backends of their own — e.g. per-tenancy instances — without forking the
// provisioning logic.
type Backend struct {
	MC         *minio.Client
	Adm        *madmin.AdminClient
	S3Endpoint string
	Region     string
	// Prefix is prepended VERBATIM to every bucket name created through this
	// backend (separator included by whoever set it, e.g. "kz-eu-"). It is
	// the tenancy boundary of the local-COSI model: the provider decides it
	// in the connection secret it delivers, the MinIO service-account policy
	// is cut to the same prefix, and no consumer-side convention is needed.
	Prefix string
}

// COSI parameter conventions stamped by the (forked) objectstorage-sidecar:
// the namespaces of the originating BucketClaim/BucketAccess. Generic
// multi-tenancy metadata — any routing extension may use them.
const (
	ParamClaimNS  = "cosi.lazedo.dev/claim-namespace"
	ParamAccessNS = "cosi.lazedo.dev/access-namespace"
)
