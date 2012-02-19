Radix
=====

Radix is an asynchronous Redis client for Go.
Radix was originally forked from the Tideland-rdc redis client (http://code.google.com/p/tideland-rdc/)
developed by Frank Mueller.

## Installation

    cd /path/to/radix/redis   
    make install

To run the tests:

	cd $GOROOT/src/pkg/redis &&
	gotest -v && make clean && cd -


## Configuration

TODO


## Operating instructions

TODO

## HACKING

If you make contributions to the project, please follow the guidelines below:

*  Maximum line-width is 110 characters.
*  Run "gofmt -tabs=true -tabwidth=4" for any Go code before committing. 
   You may do this for all code files by running "make format".
*  Any copyright notices, etc. should not be put in any files containing program code to avoid clutter. 
   Place them in separate files instead. 
*  Avoid commenting trivial or otherwise obvious code.
*  Avoid writing fancy ascii-artsy comments. 
*  Write terse code without too much newlines or other non-essential whitespace.
*  Separate code sections with "//* My section"-styled comments.

New developers should add themselves to the lists in AUTHORS and/or CONTRIBUTORS files,
when submitting their first commit. See the CONTRIBUTORS file for details.


## Copyright and licensing

*Copyright 2011  The "radix" Authors*. See file AUTHORS and CONTRIBUTORS.  
Unless otherwise noted, the source files are distributed under the
*BSD 3-Clause License* found in the LICENSE file.
