#!/bin/bash
set -e

sqlplus -S cbdb/cbdb @call-batch-par2.sql
