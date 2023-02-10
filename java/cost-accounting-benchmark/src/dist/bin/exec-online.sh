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
# java -cp "$BASEDIR/*:$BASEDIR/lib/*" $JAVA_OPTS -Dproperty="$PROPERTY" -Diceaxe.tx.log.dir=/tmp/iceaxe-tx-log -Diceaxe.tx.log.explain=2 $@
}

call_java com.tsurugidb.benchmark.costaccounting.online.CostAccountingOnline 2020-09-15
# args[0]: batch date
