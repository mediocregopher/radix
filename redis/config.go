package redis

import "time"

// Config is a configuration of a database client.
type Config struct {
	// Database number. 
	// Default: 0
	Database int

	// Password for authentication. Leave as "" to not use authentication.
	// Default: ""
	Password string

	// Socket timeout. 
	// Default: 0 (no timeouts)
	Timeout time.Duration
}

// DefaultConfig returns a new Config with default settings.
func DefaultConfig() Config {
	return Config{}
}
