#!/bin/bash
# used to regenerate command.go
set -e

command="command.go"

if [ $# == 1 ]; then
    redis_h=$1
else
    redis_h=`locate redis.h | grep '/redis\.h$' | head -n 1`
fi

if [ ! -e "$redis_h" ]; then
    echo "usage: $0 path/to/redis.h"
    echo "(or make sure you have mlocate and redis-devel installed)"
    exit 1
fi

cmds=`cat $redis_h | egrep '^void ([a-z])*Command\(' | sed 's/void \([a-z]*\)Command.*/\1/' | sort`

cat >$command <<EOF
package radix

type Command string

const (
EOF
for cmd in $cmds; do
    if [ "$cmd" == "client" ]; then
	keyword="ClientCmd"
    else
	keyword="${cmd~}"
    fi
    echo "	$keyword Command = \"$cmd\"" >>$command
done
echo ")" >>$command

gofmt -w $command
