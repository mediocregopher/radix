package redis

// Config is a configuration of a database client.
type Config struct {
	// TCP/IP Address and unix path. Unix connections are prioritized if both are set.
	Address string // eg. "173.194.32.33:6379"
	Path    string // eg. "/tmp/redis.sock"

	Database int    // Database number. Default: 0
	Password string // Password for authentication. Leave as "" to not use authentication.
	// Default: ""
	PoolCapacity int  // Connection pool capacity. Default: 50
	Timeout      int  // Socket timeout in seconds. Default: 0 (no timeouts)
	RetryLoading bool // Retry on LOADING error? Default: true
}

// DefaultConfig returns a new Config with default settings.
func DefaultConfig() Config {
	return Config{
		Address:      "127.0.0.1:6379",
		PoolCapacity: 50,
		RetryLoading: true,
	}
}
