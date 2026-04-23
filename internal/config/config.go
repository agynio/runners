package config

import (
	"fmt"
	"os"
	"strings"
)

const (
	defaultIdentityAddress       = "identity:50051"
	defaultAuthorizationAddress  = "authorization:50051"
	defaultZitiManagementAddress = "ziti-management:50051"
	defaultNotificationsAddress  = "notifications:50051"
	defaultGRPCAddr              = ":50051"
)

// Config captures runtime configuration derived from the environment.
type Config struct {
	DatabaseURL           string
	IdentityAddress       string
	AuthorizationAddress  string
	ZitiManagementAddress string
	NotificationsAddress  string
	GRPCAddr              string
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
	cfg.ZitiManagementAddress = readEnv("ZITI_MANAGEMENT_ADDRESS", defaultZitiManagementAddress)
	cfg.NotificationsAddress = readEnv("NOTIFICATIONS_ADDRESS", defaultNotificationsAddress)
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
