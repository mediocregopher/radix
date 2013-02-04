Radix
=====

Radix is a Redis client for Go.


## Installation

    go get github.com/fzzy/radix/redis

To run the tests:

    go get -u launchpad.net/gocheck
    cd $GOROOT/src/pkg/github.com/fzzy/radix/redis
    go test -v -bench=".*"


## Status

TODO

## Getting started

TODO

## API reference

API reference is available in http://gopkgdoc.appspot.com/pkg/github.com/fzzy/radix/redis.

Alternatively, run godoc for API reference:

	godoc -http=:8080

and point your browser to http://localhost:8080/pkg/github.com/fzzy/radix/redis.


## HACKING

If you make contributions to the project, please follow the guidelines below:

*  Maximum line-width is 100 characters.
*  Run "gofmt -w -s" for all Go code before pushing your code. 
*  Avoid commenting trivial or otherwise obvious code.
*  Avoid writing fancy ascii-artsy comments. 
*  Write terse code without too much newlines or other non-essential whitespace.
*  Separate code sections with "//* My section"-styled comments.

New developers should add themselves to the list in CREDITS file,
when submitting their first commit.


## Copyright and licensing

*Copyright 2012 Juhani Åhman*.
Unless otherwise noted, the source files are distributed under the
*MIT License* found in the LICENSE file.
