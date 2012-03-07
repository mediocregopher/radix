###### Notice

*This file documents the changes in **Radix** versions that are listed below.*

*Items should be added to this file as:*

	### YYYY-MM-DD Release

	+ Additional changes.

	+ More changes.

* * *

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