package config

import (
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	defaultIdentityAddress       = "identity:50051"
	defaultAuthorizationAddress  = "authorization:50051"
	defaultAgentsAddress         = "agents:50051"
	defaultZitiManagementAddress = "ziti-management:50051"
	defaultNotificationsAddress  = "notifications:50051"
	defaultGRPCAddr              = ":50051"
)

// Config captures runtime configuration derived from the environment.
type Config struct {
	DatabaseURL              string
	IdentityAddress          string
	AuthorizationAddress     string
	AgentsAddress            string
	ZitiManagementAddress    string
	NotificationsAddress     string
	ZitiLeaseRenewalInterval time.Duration
	ZitiEnrollmentTimeout    time.Duration
	GRPCAddr                 string
}

// Load reads configuration from environment variables, applying defaults when
// values are not provided. Returns an error when supplied values are invalid.
func Load() (Config, error) {
	var cfg Config

	cfg.DatabaseURL = strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL must be set")
	}

	cfg.IdentityAddress = readEnv("IDENTITY_ADDRESS", defaultIdentityAddress)
	cfg.AuthorizationAddress = readEnv("AUTHORIZATION_ADDRESS", defaultAuthorizationAddress)
	cfg.AgentsAddress = readEnv("AGENTS_ADDRESS", defaultAgentsAddress)
	cfg.ZitiManagementAddress = readEnv("ZITI_MANAGEMENT_ADDRESS", defaultZitiManagementAddress)
	cfg.NotificationsAddress = readEnv("NOTIFICATIONS_ADDRESS", defaultNotificationsAddress)
	leaseRenewalInterval := strings.TrimSpace(os.Getenv("ZITI_LEASE_RENEWAL_INTERVAL"))
	if leaseRenewalInterval == "" {
		cfg.ZitiLeaseRenewalInterval = 2 * time.Minute
	} else {
		parsed, err := time.ParseDuration(leaseRenewalInterval)
		if err != nil {
			return Config{}, fmt.Errorf("parse ZITI_LEASE_RENEWAL_INTERVAL: %w", err)
		}
		cfg.ZitiLeaseRenewalInterval = parsed
	}
	if cfg.ZitiLeaseRenewalInterval <= 0 {
		return Config{}, fmt.Errorf("ZITI_LEASE_RENEWAL_INTERVAL must be greater than 0")
	}

	enrollmentTimeout := strings.TrimSpace(os.Getenv("ZITI_ENROLLMENT_TIMEOUT"))
	if enrollmentTimeout == "" {
		cfg.ZitiEnrollmentTimeout = 2 * time.Minute
	} else {
		parsed, err := time.ParseDuration(enrollmentTimeout)
		if err != nil {
			return Config{}, fmt.Errorf("parse ZITI_ENROLLMENT_TIMEOUT: %w", err)
		}
		cfg.ZitiEnrollmentTimeout = parsed
	}
	if cfg.ZitiEnrollmentTimeout <= 0 {
		return Config{}, fmt.Errorf("ZITI_ENROLLMENT_TIMEOUT must be greater than 0")
	}
	cfg.GRPCAddr = readEnv("GRPC_ADDR", defaultGRPCAddr)

	return cfg, nil
}

func readEnv(key, def string) string {
	if value, ok := os.LookupEnv(key); ok {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" {
			return trimmed
		}
	}
	return def
}
