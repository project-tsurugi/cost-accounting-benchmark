#!/bin/bash
set -e

BASEDIR=$(cd $(dirname $0); pwd)

PROPERTY=$1
if [ -z "$PROPERTY" ]
then
  echo usage $0 property-file command
  exit 1
fi

shift

export COSTACCOUNTING_OPTS="-Dproperty=${PROPERTY} ${COSTACCOUNTING_OPTS}"
$BASEDIR/costaccounting $@
