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
  java -cp "$BASEDIR/lib/*" $JAVA_OPTS -Dproperty="$PROPERTY" $RUN_JAVA_OPTS $@
}

if [[ "$PROPERTY" != *"online_"* ]]; then
  # init data
  call_java com.tsurugidb.benchmark.costaccounting.init.InitialData

  # pre execute
  call_java com.tsurugidb.benchmark.costaccounting.batch.CostAccountingBatch
fi

# batch-command
call_java com.tsurugidb.benchmark.costaccounting.Main executeBatchCB
