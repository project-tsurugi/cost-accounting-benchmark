#!/bin/bash
set -e

sqlplus -S cbdb/cbdb @call-batch-seq.sql
