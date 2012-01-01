// Package redis implements an asynchronous Redis client.
//
// Simple client for accessing the Redis database. After establishing a connection
// with NewRedisDatabase() commands can be executed with Command(). So every command
// is possible. The method returns a ResultSet with different methods for success 
// testing and access to the retrieved values. The method MultiCommand() can be used
// for transactions. The passed function gets a MultiCommand instance as argument for
// calling the inner Command() methods.
package redis
