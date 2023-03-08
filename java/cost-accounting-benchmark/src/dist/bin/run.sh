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
  xTXLOG_OPTS="
    -Diceaxe.tx.log.dir=/tmp/iceaxe-tx-log
    -Diceaxe.tx.log.auto_flush=true
    -Diceaxe.tx.log.explain=0
    -Diceaxe.tx.log.read_progress=100000
    "
  java -cp "$BASEDIR/*:$BASEDIR/lib/*" $JAVA_OPTS -Dproperty="$PROPERTY" $TXLOG_OPTS $RUN_JAVA_OPTS $@
}

call_java com.tsurugidb.benchmark.costaccounting.Main $@
