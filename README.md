Radix
=====

Radix is a minimalistic Redis client for Go.
If you need an advanced Redis client, look elsewhere.


## Installation

    go get github.com/fzzy/radix/redis

To run the tests:

    go get -u launchpad.net/gocheck
    cd $GOROOT/src/pkg/github.com/fzzy/radix/redis
    go test -v -bench=".*"


## API reference

API reference is available in http://gopkgdoc.appspot.com/pkg/github.com/fzzy/radix/redis.

Alternatively, run godoc for API reference:

	godoc -http=:8080

and point your browser to http://localhost:8080/pkg/github.com/fzzy/radix/redis.


## HACKING

If you make contributions to the project, please follow the guidelines below:

*  Maximum line-width is 100 characters.
*  Run "gofmt -w -s" for all Go code before pushing your code. 
*  Write terse code without too much newlines or other non-essential whitespace.


## Copyright and licensing

*Copyright 2013 Juhani Åhman*.
Unless otherwise noted, the source files are distributed under the
*MIT License* found in the LICENSE file.
