# testconfs

The purpose of this directory is to aid in setting up and tearing down a small,
simple redis cluster for testing. Calling `make up` will bring up a two-node
redis cluster on ports `7000` and `7001`, while calling `make down` will
subsequenty kill it and all the state that it contained.
