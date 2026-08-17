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
	// ExternalEndpoint is the instance's public URI (from the connection
	// secret's externalEndpoint, sourced from spec.expose.host); grants whose
	// access class sets advertiseEndpoint=external advertise it instead of
	// S3Endpoint — for consumers that presign URLs off-cluster clients fetch.
	ExternalEndpoint string
}

// NOTA de desenho: já aqui viveu um Prefix de tenancy aplicado pelo driver.
// Saiu de propósito: prefixo imposto pelo CONSUMIDOR é tenancy por cortesia.
// A fronteira de tenant é do SERVIDOR — ver a proposta CreateTenantAccount no
// fork do MinIO: a identidade entregue ao consumer já vem presa ao namespace
// dela, e este driver não sabe (nem deve saber) que a tenancy existe.

// COSI parameter conventions stamped by the (forked) objectstorage-sidecar:
// the namespaces of the originating BucketClaim/BucketAccess. Generic
// multi-tenancy metadata — any routing extension may use them.
const (
	ParamClaimNS  = "cosi.lazedo.dev/claim-namespace"
	ParamAccessNS = "cosi.lazedo.dev/access-namespace"

	// ParamRequireNew, stamped by the (forked) controller when the creating
	// BucketClaim set spec.requireNew: the bucket must not already exist in
	// the object store — an existing one is refused with AlreadyExists.
	ParamRequireNew = "cosi.lazedo.dev/require-new"
)
