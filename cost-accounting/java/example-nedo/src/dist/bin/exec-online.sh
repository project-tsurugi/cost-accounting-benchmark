#!/bin/bash
set -e

BASEDIR=$(cd $(dirname $0)/..; pwd)

PROPERTY=$1
if [ -z "$PROPERTY" ]
then
  echo usage $0 property-file
  exit 1
fi

function call_java() {
  echo $1
  java -cp "$BASEDIR/*:$BASEDIR/lib/*" $JAVA_OPTS -Dproperty="$PROPERTY" $@
}

call_java com.example.nedo.online.BenchOnline 2020-09-15 "1-2,3-4,5-6"
# ex) 1,2-4
# -> thead1: factoryId=1; thread2=factoryId=2,3,4
