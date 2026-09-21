package main

import (
	"context"
	"crypto/tls"
	"errors"
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
	grpcPort              = flag.Int("grpc_port", 443, "Port to listen on")
	sqlitePath            = flag.String("sqlite_path", "configfs.db", "Path to SQLite file")
	kernelNetworking      = flag.Bool("kernel_networking", false, "Whether the server should just listen on kernel networking.")
	tailscaleDirectory    = flag.String("tailscale_directory", "", "Directory for storing Tailscale state")
	tailscaleAuthKey      = flag.String("tailscale_authkey", "", "Authentication key to use with Tailscale")
	tailscaleClientID     = flag.String("tailscale_client_id", "", "Client ID to use when authenticating with Tailscale")
	tailscaleClientSecret = flag.String("tailscale_client_secret", "", "Client Secret to use when authenticating with Tailscale")
	tailscaleHostname     = flag.String("tailscale_hostname", "", "Hostname to use when registering with Tailscale")
	tailscaleTags         = flag.String("tailscale_tags", "", "Comma separated list of Tailscale tags.")
	tailscaleService      = flag.String("tailscale_service", "", "Name of the Tailscale service to listen on.")
	tailscaleEphemeral    = flag.Bool("tailscale_ephemeral", true, "Whether the Tailscale node should be registered as ephemeral.")
	createAllowedTags     = flag.String("create_allowed_tags", "", "Comma separated identities (tags like tag:foo and users like user:alice@example.com) permitted to create new configs. Empty means no restriction. Does not affect updates or deletes, which use per-config ACLs.")

	// A second tailnet is a second node rather than a second listener on the
	// first: tailnets hand out overlapping 100.x addresses, so a peer can only
	// be identified by asking the node it connected through.
	additionalTailnetDirectory    = flag.String("additional_tailnet_directory", "", "Directory for storing the Tailscale state of a second tailnet to also serve on. Setting it joins that tailnet; the other additional_tailnet flags configure it and do not fall back to the tailscale ones.")
	additionalTailnetAuthKey      = flag.String("additional_tailnet_authkey", "", "Authentication key to use with the second tailnet")
	additionalTailnetClientID     = flag.String("additional_tailnet_client_id", "", "Client ID to use when authenticating with the second tailnet")
	additionalTailnetClientSecret = flag.String("additional_tailnet_client_secret", "", "Client Secret to use when authenticating with the second tailnet")
	additionalTailnetHostname     = flag.String("additional_tailnet_hostname", "", "Hostname to use when registering with the second tailnet")
	additionalTailnetTags         = flag.String("additional_tailnet_tags", "", "Comma separated list of Tailscale tags for the second tailnet.")
	additionalTailnetService      = flag.String("additional_tailnet_service", "", "Name of the Tailscale service to listen on in the second tailnet.")
)

type tailnet struct {
	directory    string
	authKey      string
	clientID     string
	clientSecret string
	hostname     string
	tags         string
	service      string
}

func joinTailnet(ctx context.Context, t tailnet) (*tsnet.Server, *local.Client, error) {
	s := &tsnet.Server{
		Hostname:     t.hostname,
		AuthKey:      t.authKey,
		Dir:          t.directory,
		Ephemeral:    *tailscaleEphemeral,
		ClientID:     t.clientID,
		ClientSecret: t.clientSecret,
	}
	for _, tag := range strings.Split(t.tags, ",") {
		if cleanTag := strings.TrimSpace(tag); len(cleanTag) > 0 {
			s.AdvertiseTags = append(s.AdvertiseTags, cleanTag)
		}
	}
	state, err := s.Up(ctx)
	if err != nil {
		s.Close()
		return nil, nil, fmt.Errorf("unable to connect to Tailscale: %w", err)
	}
	stringIP := []string{}
	for _, ip := range state.TailscaleIPs {
		stringIP = append(stringIP, ip.String())
	}
	log.InfofCtx(ctx, "Connected to Tailscale with IPs %s (state in %s)", strings.Join(stringIP, ", "), t.directory)
	if err := s.Start(); err != nil {
		s.Close()
		return nil, nil, fmt.Errorf("error starting Tailscale server: %w", err)
	}
	log.InfofCtx(ctx, "Tailscale server successfully started")
	lc, err := s.LocalClient()
	if err != nil {
		s.Close()
		return nil, nil, fmt.Errorf("unable to obtain local client for Tailscale: %w", err)
	}
	return s, lc, nil
}

func listener(ctx context.Context, s *tsnet.Server, service string) (net.Listener, error) {
	if *kernelNetworking {
		return net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", *grpcPort))
	}
	if s == nil {
		log.FatalfCtx(ctx, "tailscale server is nil - unable to create listener")
	}
	if service != "" {
		listen, err := s.ListenService(service, tsnet.ServiceModeTCP{
			Port:                 uint16(*grpcPort),
			TerminateTLS:         false,
			PROXYProtocolVersion: 2,
		})
		if err != nil {
			return nil, err
		}
		return &proxyproto.Listener{Listener: listen}, nil
	}
	return s.Listen("tcp", fmt.Sprintf(":%d", *grpcPort))
}

func newGRPCServer(serv *api.Server, localClient *local.Client) *grpc.Server {
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
	grpcServer := grpc.NewServer(grpc.ChainUnaryInterceptor(interceptors...), grpc.Creds(credentials.NewTLS(&tls.Config{
		GetCertificate: localClient.GetCertificate,
	})))
	types.RegisterConfigFSServerServer(grpcServer, serv)
	reflection.Register(grpcServer)
	return grpcServer
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
	)
	if !*kernelNetworking {
		s, localClient, err = joinTailnet(ctx, tailnet{
			directory:    *tailscaleDirectory,
			authKey:      *tailscaleAuthKey,
			clientID:     *tailscaleClientID,
			clientSecret: *tailscaleClientSecret,
			hostname:     *tailscaleHostname,
			tags:         *tailscaleTags,
			service:      *tailscaleService,
		})
		if err != nil {
			log.FatalfCtx(ctx, "%s", err.Error())
		}
		defer s.Close()
	} else if *additionalTailnetDirectory != "" {
		log.FatalfCtx(ctx, "additional_tailnet_directory requires kernel_networking = false")
	}
	serv := api.NewServer(configStore, strings.Split(*createAllowedTags, ","))
	g, ctx := errgroup.WithContext(ctx)

	listen, err := listener(ctx, s, *tailscaleService)
	if err != nil {
		log.FatalfCtx(ctx, "%s", err.Error())
	}
	grpcServer := newGRPCServer(serv, localClient)
	log.InfofCtx(ctx, "gRPC Server Listening on port %d with kernel_networking = %t", *grpcPort, *kernelNetworking)
	g.Go(func() error {
		return grpcServer.Serve(listen)
	})

	if *additionalTailnetDirectory != "" {
		// Joined in the background, and a failure here only logged: the clients
		// of the first tailnet are already being served, and a second tailnet
		// that cannot join must not take them down with it.
		g.Go(func() error {
			as, lc, err := joinTailnet(ctx, tailnet{
				directory:    *additionalTailnetDirectory,
				authKey:      *additionalTailnetAuthKey,
				clientID:     *additionalTailnetClientID,
				clientSecret: *additionalTailnetClientSecret,
				hostname:     *additionalTailnetHostname,
				tags:         *additionalTailnetTags,
				service:      *additionalTailnetService,
			})
			if err != nil {
				log.ErrorfCtx(ctx, err, "not serving on the additional tailnet")
				return nil
			}
			defer as.Close()
			listen, err := listener(ctx, as, *additionalTailnetService)
			if err != nil {
				log.ErrorfCtx(ctx, err, "not serving on the additional tailnet")
				return nil
			}
			server := newGRPCServer(serv, lc)
			go func() {
				<-ctx.Done()
				server.GracefulStop()
			}()
			log.InfofCtx(ctx, "gRPC Server Listening on port %d on the additional tailnet", *grpcPort)
			if err := server.Serve(listen); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				log.ErrorfCtx(ctx, err, "stopped serving on the additional tailnet")
			}
			return nil
		})
	}
	<-ctx.Done()

	grpcServer.GracefulStop()
	g.Wait()
}

func main() {
	flag.Parse()
	run()
}
