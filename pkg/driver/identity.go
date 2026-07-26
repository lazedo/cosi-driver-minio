// Copyright 2026 lazedo. Apache-2.0.
package driver

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	cosi "sigs.k8s.io/container-object-storage-interface/proto"
)

// IdentityServer answers DriverGetInfo with the provisioner name; the sidecar
// uses it to stamp BucketClass.driverName ownership.
type IdentityServer struct {
	cosi.UnimplementedIdentityServer
	Provisioner string
}

func (id *IdentityServer) DriverGetInfo(_ context.Context, _ *cosi.DriverGetInfoRequest) (*cosi.DriverGetInfoResponse, error) {
	if id.Provisioner == "" {
		return nil, status.Error(codes.InvalidArgument, "provisioner name is empty")
	}
	return &cosi.DriverGetInfoResponse{Name: id.Provisioner}, nil
}
