// This package implements an asynchronous Redis client.
//
// Client is a structure for accessing a Redis database. 
// After establishing a connection with NewClient, commands can be executed with Client.Command.
// Client.Command returns a Reply with different methods for accessing the retrieved values.
// Client.MultiCommand can be used for sending multiple commands in a single request and
// Client.Transaction offers a simple way for executing atomic requests.
// Client.Subscription returns a Subscription that can be used for listening published messages.
package redis
