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

	"github.com/davefinster/configfs/internal/api"
	"github.com/davefinster/configfs/internal/log"
	types "github.com/davefinster/configfs/internal/proto"
	"github.com/davefinster/configfs/internal/store/sqlite"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	proxyproto "github.com/pires/go-proxyproto"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"
	"tailscale.com/client/local"
	"tailscale.com/tsnet"
)

var (
	grpcPort           = flag.Int("grpc_port", 443, "Port to listen on")
	sqlitePath         = flag.String("sqlite_path", "configfs.db", "Path to SQLite file")
	kernelNetworking   = flag.Bool("kernel_networking", false, "Whether the server should just listen on kernel networking.")
	tailscaleDirectory = flag.String("tailscale_directory", "", "Directory for storing Tailscale state")
	tailscaleAuthKey   = flag.String("tailscale_authkey", "", "Authentication key to use with Tailscale")
	tailscaleHostname  = flag.String("tailscale_hostname", "", "Hostname to use when registering with Tailscale")
	tailscaleTags      = flag.String("tailscale_tags", "", "Comma separated list of Tailscale tags.")
	tailscaleService   = flag.String("tailscale_service", "", "Name of the Tailscale service to listen on.")
	createAllowedTags  = flag.String("create_allowed_tags", "", "Comma separated identities (tags like tag:foo and users like user:alice@example.com) permitted to create new configs. Empty means no restriction. Does not affect updates or deletes, which use per-config ACLs.")
)

func listener(ctx context.Context, s *tsnet.Server) (net.Listener, error) {
	if *kernelNetworking {
		return net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", *grpcPort))
	}
	if s == nil {
		log.FatalfCtx(ctx, "tailscale server is nil - unable to create listener")
	}
	if *tailscaleService != "" {
		return s.ListenService(*tailscaleService, tsnet.ServiceModeTCP{
			Port:                 uint16(*grpcPort),
			TerminateTLS:         false,
			PROXYProtocolVersion: 2,
		})
	}
	return s.Listen("tcp", fmt.Sprintf(":%d", *grpcPort))
}

func run() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGABRT, syscall.SIGINT)
	defer stop()
	dbFile, err := sqlx.Connect("sqlite3", *sqlitePath)
	if err != nil {
		log.FatalfCtx(ctx, "error connecting to sqlite database at path %q: %s", *sqlitePath, err.Error())
	}
	configStore, err := sqlite.NewStore(dbFile)
	if err != nil {
		log.FatalfCtx(ctx, "error configuring sqlite store: %s", err.Error())
	}
	var (
		s           *tsnet.Server
		localClient *local.Client
		grpcServer  *grpc.Server
	)
	if !*kernelNetworking {
		s = &tsnet.Server{
			Hostname: *tailscaleHostname,
			AuthKey:  *tailscaleAuthKey,
			Dir:      *tailscaleDirectory,
		}
		if *tailscaleTags != "" {
			tags := strings.Split(*tailscaleTags, ",")
			cleanTags := []string{}
			for _, tag := range tags {
				cleanTag := strings.TrimSpace(tag)
				if len(cleanTag) > 0 {
					cleanTags = append(cleanTags, cleanTag)
				}
			}
			tags = cleanTags
			s.AdvertiseTags = tags
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
		defer s.Close()
		log.InfofCtx(ctx, "Tailscale server successfully started")
		lc, err := s.LocalClient()
		if err != nil {
			log.FatalfCtx(ctx, "unable to obtain local client for Tailscale: %s", err.Error())
		}
		localClient = lc
	}
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		listen, err := listener(ctx, s)
		if err != nil {
			log.FatalfCtx(ctx, "%s", err.Error())
		}
		if listen == nil {
			log.FatalfCtx(ctx, "failed to create listener - return was nil")
		}
		defer listen.Close()
		interceptors := []grpc.UnaryServerInterceptor{
			logging.UnaryServerInterceptor(log.InterceptorLogger(log.StdLogger())),
			recovery.UnaryServerInterceptor(),
		}
		if !*kernelNetworking {
			interceptors = append(interceptors, api.TailscaleAuthenticationInterceptor(localClient))
		} else {
			// No trusted authenticator in kernel mode; drop any client-supplied
			// acl_tags so they cannot be forged. Only everyone-scoped configs are
			// reachable in this (local-dev) mode.
			interceptors = append(interceptors, api.StripACLTagsInterceptor())
		}
		chainOpts := grpc.ChainUnaryInterceptor(interceptors...)
		serv := api.NewServer(configStore, strings.Split(*createAllowedTags, ","))
		grpcServer = grpc.NewServer(chainOpts, grpc.Creds(credentials.NewTLS(&tls.Config{
			GetCertificate: localClient.GetCertificate,
		})))
		types.RegisterConfigFSServerServer(grpcServer, serv)
		reflection.Register(grpcServer)
		log.InfofCtx(ctx, "gRPC Server Listening on port %d with kernel_networking = %t", *grpcPort, *kernelNetworking)
		if *tailscaleService != "" {
			proxyListener := &proxyproto.Listener{Listener: listen}
			defer proxyListener.Close()
			listen = proxyListener
		}
		return grpcServer.Serve(listen)
	})
	<-ctx.Done()

	if grpcServer != nil {
		grpcServer.GracefulStop()
	}
}

func main() {
	flag.Parse()
	run()
}
