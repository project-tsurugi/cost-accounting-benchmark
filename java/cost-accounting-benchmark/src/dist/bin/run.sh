#!/bin/bash
set -e

BASEDIR=$(cd $(dirname $0)/..; pwd)

PROPERTY=$1
if [ -z "$PROPERTY" ]
then
  echo usage $0 property-file
  exit 1
fi

shift

function call_java() {
  java -cp "$BASEDIR/*:$BASEDIR/lib/*" $JAVA_OPTS -Dproperty="$PROPERTY" $RUN_JAVA_OPTS $@
}

call_java com.tsurugidb.benchmark.costaccounting.Main $@
