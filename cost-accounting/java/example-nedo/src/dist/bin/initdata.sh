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
  java -cp "$BASEDIR/*:$BASEDIR/lib/*" -Dproperty="$PROPERTY" $1
}

call_java com.example.nedo.init.InitialData01MeasurementMaster
call_java com.example.nedo.init.InitialData02FactoryMaster
call_java com.example.nedo.init.InitialData03ItemMaster
call_java com.example.nedo.init.InitialData04ItemManufacturingMaster
call_java com.example.nedo.init.InitialData05CostMaster
call_java com.example.nedo.init.InitialData06Increase
