# cost accouting benchmark (Oracle)

## How to build an execution environment

Execute the following sql file (create package, etc.) in Oracle.

- bb_measurement.sql
- bb_measurement.body.sql
- bb_measurement_value.sql
- bb_measurement_value.body.sql
- bench_batch.sql
- bench_batch.body.sql



## How to execute

Execute from SQL*Plus as follows.

```sql
set serveroutput on
set timing on
call bench_batch.bench_batch('2020-09-15', '1,2,3');
```

The first argument is the batch reference date (same as the one specified when creating the initial data).

The second argument is the factory ID to be processed. Multiple specifications can be specified by separating them with commas. You can specify a range using a hyphen, such as "1-3".
If omitted, all factories are targeted.

The third argument is the commit rate. 100 to always commit, 0 to always rollback. The default is 100.

