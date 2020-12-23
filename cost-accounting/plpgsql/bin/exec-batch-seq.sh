#!/bin/bash
set -e

psql -U cbdb cbdb -c "call bench_batch('2020-09-15','')"
