#!/bin/bash
set -e

for i in $(seq 1 60); do
  sqlplus -S cbdb/cbdb @call-batch-par.sql $i &
done
wait
