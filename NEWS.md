###### Notice

*This file documents the changes in **radix** versions that are listed below.*

*Items should be added to this file as:*

	### YYYY-MM-DD Release

	+ Additional changes.
	+ More changes.

* * *

### 2012-04-20 Release v0.1.4

+ New API for commands (thanks Bobby Powers).
+ Source code moved to its own directory.

### 2012-04-10 Release v0.1.3

+ Go v1.0 compatible.
+ Fixed broken tests.
+ Fixed issue #1: Very high memory usage due to bug in timeout error handling.
+ Fixed issue #2: Graceful reset of Connections when connection error is encountered.
+ Removed support for Client.Select method. Use separate clients for and MOVE command for controlling multiple databases instead.

### 2012-03-06 Release v0.1.2

+ Various small fixes to make the package work with Go weekly/Go 1.
+ Renames the package name to radix.
+ Removed Makefiles as Go 1's "go build" deprecates them.
+ Moved files from redis/ to the root directory of the project.
+ Rewrote most of the godoc strings to match more Go conventions.
+ Minor improvements to channel usage.

* NOTE: The go tests are currently partially broken, because gocheck seems to be broken with newest version
        of Go.

### 2012-03-01 Release v0.1.1

+ Updated connection pool to reuse old connections more efficiently.

### 2012-03-01 Release v0.1

+ First stable release.