#!/bin/bash
set -e

for i in $(seq 1 60); do
  psql -U cbdb cbdb -c "call bench_batch('2020-09-15','"$i"')" &
done
wait
