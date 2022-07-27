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
  java -cp "$BASEDIR/*:$BASEDIR/lib/*" $JAVA_OPTS -Dproperty="$PROPERTY" $1
}

call_java com.tsurugidb.benchmark.costaccounting.init.InitialData00CreateTable
call_java com.tsurugidb.benchmark.costaccounting.init.InitialData01MeasurementMaster
call_java com.tsurugidb.benchmark.costaccounting.init.InitialData02FactoryMaster
call_java com.tsurugidb.benchmark.costaccounting.init.InitialData03ItemMaster
call_java com.tsurugidb.benchmark.costaccounting.init.InitialData04ItemManufacturingMaster
call_java com.tsurugidb.benchmark.costaccounting.init.InitialData05CostMaster
call_java com.tsurugidb.benchmark.costaccounting.init.InitialData06ResultTable
