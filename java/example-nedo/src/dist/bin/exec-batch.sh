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
  java -cp "$BASEDIR/*:$BASEDIR/lib/*" -Dproperty="$PROPERTY" $@
}

call_java com.example.nedo.batch.BenchBatch 2020-09-15 "1" 100
