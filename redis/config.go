package redis

import "time"

// Config is a configuration of a database client.
type Config struct {
	// Network and address
	// "tcp" or "unix". Default: "tcp"
	Network string
	// eg. "173.194.32.33:6379". Default: "127.0.0.1:6379"
	Address string

	// Database number. Default: 0
	Database int

	// Password for authentication. Leave as "" to not use authentication.
	// Default: ""
	Password string

	// Connection pool capacity. Default: 50
	PoolCapacity int

	// Socket timeout. Default: 0 (no timeouts)
	Timeout time.Duration

	// Retry on LOADING error? Default: true
	RetryLoading bool
}

// DefaultConfig returns a new Config with default settings.
func DefaultConfig() Config {
	return Config{
		Network:      "tcp",
		Address:      "127.0.0.1:6379",
		PoolCapacity: 50,
		RetryLoading: true,
	}
}
