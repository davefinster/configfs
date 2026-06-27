package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/davefinster/configfs/internal/fs"
	"github.com/davefinster/configfs/internal/log"
	"tailscale.com/client/local"

	types "github.com/davefinster/configfs/internal/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"tailscale.com/tsnet"
)

var (
	serverAddress         = flag.String("grpc_address", "configfs.tailbd60.ts.net:443", "Address to connect to server on")
	kernelNetworking      = flag.Bool("kernel_networking", false, "Whether the server should just listen on kernel networking.")
	mountPoint            = flag.String("mount_point", "/mnt/fusetest", "")
	readOnly              = flag.Bool("read_only", false, "Whether the FUSE mount should be read-only. When set, the kernel rejects writes, creates and deletes.")
	createMountPoint      = flag.Bool("create_mount_point", false, "When set, create the mount point directory (and any missing parents, like mkdir -p) if it does not already exist before mounting.")
	tailscaleDirectory    = flag.String("tailscale_directory", "", "Directory for storing Tailscale state")
	tailscaleAuthKey      = flag.String("tailscale_authkey", "", "Authentication key to use with Tailscale")
	tailscaleHostname     = flag.String("tailscale_hostname", "", "Hostname to use when registering with Tailscale")
	tailscaleEphemeral    = flag.Bool("tailscale_ephemeral", true, "Whether the Tailscale node should be registered as ephemeral.")
	tailscaleClientID     = flag.String("tailscale_client_id", "", "Client ID to use when authenticating with Tailscale")
	tailscaleClientSecret = flag.String("tailscale_client_secret", "", "Client Secret to use when authenticating with Tailscale")
	tailscaleSocketPath   = flag.String("tailscale_socket_path", "", "Path at which the Tailscale socket can be found for detecting local Tailscale status.")
	additionalACLOnCreate = flag.String("additional_acl_on_create", "", "tag:name@ACL comma separated list of ACLs to apply to newly created files.")
)

func run() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGABRT, syscall.SIGINT)
	defer stop()
	var s *tsnet.Server
	if !*kernelNetworking {
		s = &tsnet.Server{
			Hostname:     *tailscaleHostname,
			AuthKey:      *tailscaleAuthKey,
			Dir:          *tailscaleDirectory,
			Ephemeral:    *tailscaleEphemeral,
			ClientID:     *tailscaleClientID,
			ClientSecret: *tailscaleClientSecret,
		}
		state, err := s.Up(ctx)
		if err != nil {
			log.FatalfCtx(ctx, "unable to connect to Tailscale: %s", err.Error())
		}
		stringIP := []string{}
		for _, ip := range state.TailscaleIPs {
			stringIP = append(stringIP, ip.String())
		}
		log.InfofCtx(ctx, "Connected to Tailscale with IPs %s", strings.Join(stringIP, ", "))
		if err := s.Start(); err != nil {
			log.FatalfCtx(ctx, "error starting Tailscale server: %s", err.Error())
		}
		log.InfofCtx(ctx, "Tailscale server successfully started")
		defer s.Close()
	}
	if *kernelNetworking && *tailscaleSocketPath != "" {
		c := &local.Client{
			UseSocketOnly: true,
			Socket:        *tailscaleSocketPath,
		}
		c.UseSocketOnly = true
		for {
			s, err := c.Status(ctx)
			if err != nil {
				log.WarningfCtx(ctx, "error fetching tailscale status via socket: %s", err.Error())
			} else if s.BackendState == "Running" {
				break
			}
			log.InfofCtx(ctx, "Local Tailscale not yet running.")
			time.Sleep(5 * time.Second)
		}
	}
	creds := credentials.NewTLS(&tls.Config{})
	opts := []grpc.DialOption{grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
		if *kernelNetworking {
			return net.Dial("tcp", addr)
		}
		return s.Dial(ctx, "tcp", addr)
	}),
		grpc.WithTransportCredentials(creds),
	}
	conn, err := grpc.NewClient(fmt.Sprintf("passthrough:%s", *serverAddress), opts...)
	if err != nil {
		log.FatalfCtx(ctx, "error dialing server at %s with kernel_networking = %t: %s", *serverAddress, *kernelNetworking, err.Error())
	}
	defer conn.Close()
	grpcClient := types.NewConfigFSServerClient(conn)
	root := uint32(0)
	fileMode := os.FileMode(0o444)
	fsOpts := &fs.RemoteConfigFSOptions{
		Owner:           &root,
		Group:           &root,
		Writable:        !*readOnly,
		FileMode:        &fileMode,
		RefreshInterval: 10 * time.Second,
	}
	aclParts := strings.Split(*additionalACLOnCreate, ",")
	for _, aclPart := range aclParts {
		if len(aclPart) == 0 {
			continue
		}
		components := strings.Split(aclPart, "@")
		if len(components) != 2 {
			continue
		}
		if components[1] != "READ" && components[1] != "WRITE" {
			continue
		}
		acl := &types.ConfigAcl{}
		if components[0] == "everyone" {
			acl.Everyone = true
		} else {
			acl.Tag = components[0]
		}
		if components[1] == "READ" {
			acl.Acl = types.Acl_READ
		}
		if components[1] == "WRITE" {
			acl.Acl = types.Acl_WRITE
		}
		fsOpts.AdditionalACLOnCreate = append(fsOpts.AdditionalACLOnCreate, acl)
	}
	remoteFS := fs.NewRemoteConfigFS(grpcClient, *mountPoint, fsOpts)
	if err := remoteFS.LoadSnapshot(ctx); err != nil {
		log.FatalfCtx(ctx, "error doing initial root load: %s", err.Error())
	}
	if *createMountPoint {
		if err := remoteFS.EnsureMountPoint(ctx); err != nil {
			log.FatalfCtx(ctx, "error ensuring mount point exists: %s", err.Error())
		}
	}
	if err := remoteFS.AttemptUnmountIfNecessary(ctx); err != nil {
		log.ErrorfCtx(ctx, err, "error attempting to check for existing mounts - will attempt to mount anyway")
	}
	if err := remoteFS.Mount(); err != nil {
		log.FatalfCtx(ctx, "unable to mount file system: %s", err.Error())
	}
	go func() {
		log.InfofCtx(ctx, "Starting configfs FUSE serving")
		if err := remoteFS.Serve(ctx); err != nil {
			log.ErrorfCtx(ctx, err, "error while serving file system")
		}
	}()
	log.InfofCtx(ctx, "Waiting for exit")
	<-ctx.Done()
	log.InfofCtx(ctx, "Closing")
	if err := remoteFS.Close(); err != nil {
		log.ErrorfCtx(ctx, err, "error while closing fs mount")
	}
}

func main() {
	flag.Parse()
	run()
}
