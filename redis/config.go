package redis

import (
	"time"
)

// Config is a configuration of a database client.
type Config struct {
	Address        string
	Path           string
	Database       int
	Password       string
	PoolCapacity   int
	Timeout        time.Duration
	NoLoadingRetry bool
}
