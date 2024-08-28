#!/bin/bash
#
# Copyright 2023-2024 Project Tsurugi.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

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
  xTXLOG_OPTS="
    -Dbench.tx.log.target=ONLINE
    -Diceaxe.tx.log.dir=/tmp/iceaxe-tx-log
    -Diceaxe.tx.log.auto_flush=true
    -Diceaxe.tx.log.explain=0
    -Diceaxe.tx.log.read_progress=100000
    "
  java -cp "$BASEDIR/lib/*" $JAVA_OPTS -Dproperty="$PROPERTY" $TXLOG_OPTS $@
}

call_java com.tsurugidb.benchmark.costaccounting.online.CostAccountingOnline 2020-09-15
# args[0]: batch date
