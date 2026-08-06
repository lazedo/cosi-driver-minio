// Copyright 2026 lazedo. Apache-2.0.
// minio-cosi-driver: COSI provisioner driver for MinIO (release-0.2 / v1alpha1).
// Runs alongside the objectstorage-sidecar over a shared unix socket; the sidecar
// does the k8s reconcile, this process implements the gRPC Provisioner+Identity.
//
// Subcommands on the same binary:
//   - (default)  serve the COSI gRPC socket
//   - discover   reconcile BucketClass/BucketAccessClass pairs from discovered
//     connections (labeled secrets, optionally house MinIO instance CRs);
//     runs as a third container in the driver pod. See pkg/discover.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/minio/madmin-go/v4"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"google.golang.org/grpc"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"

	cosi "sigs.k8s.io/container-object-storage-interface/proto"

	"github.com/lazedo/cosi-driver-minio/pkg/discover"
	"github.com/lazedo/cosi-driver-minio/pkg/driver"
)

func main() {
	// Subcommand dispatch: `minio-cosi-driver discover [flags]` runs the
	// class reconciler instead of the gRPC server.
	if len(os.Args) > 1 && os.Args[1] == "discover" {
		runDiscover(os.Args[2:])
		return
	}

	var (
		socket      = flag.String("driver-addr", "unix:///var/lib/cosi/cosi.sock", "unix socket the sidecar connects to")
		provisioner = flag.String("provisioner", "minio.objectstorage.k8s.lazedo.dev", "COSI provisioner name (matches BucketClass.driverName)")
		adminHost   = flag.String("minio-admin-endpoint", "", "MinIO endpoint host[:port] for the S3 + admin API (e.g. minio.minio.svc:443)")
		s3Endpoint  = flag.String("s3-endpoint", "", "S3 endpoint advertised in the BucketAccess secret (e.g. https://minio.minio.svc)")
		region      = flag.String("region", "", "optional S3 region")
		accessKey   = flag.String("minio-access-key", "", "MinIO admin access key")
		secretKey   = flag.String("minio-secret-key", "", "MinIO admin secret key")
		secure      = flag.Bool("secure", true, "use TLS to reach MinIO")
		caFile      = flag.String("ca-file", "", "PEM CA bundle to verify the MinIO endpoint")
	)
	klog.InitFlags(nil)
	flag.Parse()

	// credentials via env when flags are empty (avoids secrets on the cmdline)
	if *accessKey == "" {
		*accessKey = os.Getenv("MINIO_ACCESS_KEY")
	}
	if *secretKey == "" {
		*secretKey = os.Getenv("MINIO_SECRET_KEY")
	}

	// static backend is optional: a connection-only driver (subscription
	// model — BucketClass parameters.connectionSecret) runs without one.
	staticBackend := *adminHost != ""
	if staticBackend && (*accessKey == "" || *secretKey == "") {
		klog.Fatal("minio-access-key and minio-secret-key are required with minio-admin-endpoint")
	}
	if *s3Endpoint == "" && staticBackend {
		scheme := "https"
		if !*secure {
			scheme = "http"
		}
		*s3Endpoint = scheme + "://" + *adminHost
	}

	transport, err := buildTransport(*secure, *caFile)
	if err != nil {
		klog.Fatalf("building TLS transport: %v", err)
	}

	var mc *minio.Client
	var adm *madmin.AdminClient
	if staticBackend {
		mc, err = minio.New(*adminHost, &minio.Options{
			Creds:     credentials.NewStaticV4(*accessKey, *secretKey, ""),
			Secure:    *secure,
			Transport: transport,
			Region:    *region,
		})
		if err != nil {
			klog.Fatalf("minio client: %v", err)
		}
		adm, err = madmin.NewWithOptions(*adminHost, &madmin.Options{
			Creds:     credentials.NewStaticV4(*accessKey, *secretKey, ""),
			Secure:    *secure,
			Transport: transport,
		})
		if err != nil {
			klog.Fatalf("madmin client: %v", err)
		}
	}

	// connection-secret resolution is available when running in-cluster: a
	// BucketClass may declare parameters.connectionSecret.
	var conns *driver.Connections
	if cfg, err := rest.InClusterConfig(); err == nil {
		if k8s, err := kubernetes.NewForConfig(cfg); err == nil {
			conns = driver.NewConnections(k8s)
		}
	}

	lis, err := listen(*socket)
	if err != nil {
		klog.Fatalf("listen %s: %v", *socket, err)
	}
	srv := grpc.NewServer()
	cosi.RegisterIdentityServer(srv, &driver.IdentityServer{Provisioner: *provisioner})
	cosi.RegisterProvisionerServer(srv, &driver.ProvisionerServer{
		Provisioner: *provisioner, MC: mc, Adm: adm, S3Endpoint: *s3Endpoint, Region: *region,
		Connections: conns,
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() { <-ctx.Done(); srv.GracefulStop() }()

	klog.InfoS("minio-cosi-driver serving", "socket", *socket, "provisioner", *provisioner, "minio", *adminHost)
	if err := srv.Serve(lis); err != nil {
		klog.Fatalf("serve: %v", err)
	}
}

// runDiscover parses the discover subcommand's own flag set and runs the
// class reconciler until SIGINT/SIGTERM.
func runDiscover(args []string) {
	fs := flag.NewFlagSet("discover", flag.ExitOnError)
	var (
		driverName     = fs.String("driver-name", "minio.objectstorage.k8s.lazedo.dev", "driverName written into created classes (must match the driver container's --provisioner)")
		interval       = fs.Duration("interval", 30*time.Second, "full reconcile interval")
		watchInstances = fs.Bool("watch-minio-instances", false, "also discover local house MinIO instance CRs (storage.lazedo.dev/v1alpha1)")
	)
	klog.InitFlags(fs)
	if err := fs.Parse(args); err != nil {
		klog.Fatalf("parsing discover flags: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := discover.Run(ctx, discover.Options{
		DriverName:          *driverName,
		Interval:            *interval,
		WatchMinioInstances: *watchInstances,
	}); err != nil {
		klog.Fatalf("discover: %v", err)
	}
}

func buildTransport(secure bool, caFile string) (*http.Transport, error) {
	tr := &http.Transport{}
	if secure && caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, os.ErrInvalid
		}
		tr.TLSClientConfig = &tls.Config{RootCAs: pool}
	}
	return tr, nil
}

func listen(addr string) (net.Listener, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}
	if u.Scheme == "unix" {
		path := strings.TrimPrefix(addr, "unix://")
		_ = os.Remove(path)
		return net.Listen("unix", path)
	}
	return net.Listen("tcp", u.Host)
}
