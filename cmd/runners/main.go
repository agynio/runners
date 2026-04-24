package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	authorizationv1 "github.com/agynio/runners/.gen/go/agynio/api/authorization/v1"
	identityv1 "github.com/agynio/runners/.gen/go/agynio/api/identity/v1"
	notificationsv1 "github.com/agynio/runners/.gen/go/agynio/api/notifications/v1"
	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	zitimanagementv1 "github.com/agynio/runners/.gen/go/agynio/api/ziti_management/v1"
	"github.com/agynio/runners/internal/config"
	"github.com/agynio/runners/internal/db"
	"github.com/agynio/runners/internal/server"
	"github.com/agynio/runners/internal/zitimanager"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("runners: %v", err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("parse database url: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return fmt.Errorf("create connection pool: %w", err)
	}
	defer pool.Close()

	if err := db.ApplyMigrations(ctx, pool); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}

	identityConn, err := grpc.DialContext(ctx, cfg.IdentityAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial identity service: %w", err)
	}
	defer identityConn.Close()

	authorizationConn, err := grpc.DialContext(ctx, cfg.AuthorizationAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial authorization service: %w", err)
	}
	defer authorizationConn.Close()

	zitiManagementConn, err := grpc.DialContext(ctx, cfg.ZitiManagementAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial ziti management service: %w", err)
	}
	defer zitiManagementConn.Close()

	zitiMgmtClient := zitimanagementv1.NewZitiManagementServiceClient(zitiManagementConn)
	zitiManager, err := zitimanager.New(ctx, zitiMgmtClient, cfg.ZitiEnrollmentTimeout, cfg.ZitiLeaseRenewalInterval)
	if err != nil {
		return fmt.Errorf("initialize ziti manager: %w", err)
	}
	defer zitiManager.Close()
	go zitiManager.RunLeaseRenewal(ctx)

	notificationsConn, err := grpc.DialContext(ctx, cfg.NotificationsAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial notifications service: %w", err)
	}
	defer notificationsConn.Close()

	grpcServer := grpc.NewServer()
	runnersv1.RegisterRunnersServiceServer(grpcServer, server.New(server.Options{
		Pool:                 pool,
		IdentityClient:       identityv1.NewIdentityServiceClient(identityConn),
		AuthorizationClient:  authorizationv1.NewAuthorizationServiceClient(authorizationConn),
		ZitiManagementClient: zitiMgmtClient,
		NotificationsClient:  notificationsv1.NewNotificationsServiceClient(notificationsConn),
		ZitiDialer:           zitiManager,
	}))

	listener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.GRPCAddr, err)
	}
	defer listener.Close()

	errCh := make(chan error, 1)
	go func() {
		log.Print("runners: ready")
		if err := grpcServer.Serve(listener); err != nil {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		if errors.Is(err, grpc.ErrServerStopped) {
			return nil
		}
		return err
	case <-ctx.Done():
		grpcServer.GracefulStop()
		return nil
	}
}
