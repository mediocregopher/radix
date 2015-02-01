# loadtest

This is a little script that can be used for load-testing and making sure that
reconnect/redistirbution logic works alright. It will make a connection to the
test cluster (ports 7000 and 7001) and continuously make GET and SET requests on
random keys, sometimes going back and doing GET/SET on keys it looked at in the
past.

While this is running you can kill redis nodes, redistribute the nodes, etc...
to see how the cluster reacts. All errors will be reported to the console.
