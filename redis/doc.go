// This package implements a Redis client.
//
// Client is a structure for accessing a Redis database. 
// Client.MultiCall can be used for sending multiple commands in a single request and
// Client.Transaction offers a simple way for executing atomic requests.
// Client.Subscription returns a Subscription that can be used for listening published messages.
package redis
