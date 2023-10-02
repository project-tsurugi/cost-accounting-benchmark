# cost accounting benchmark (PL/pgSQL)

## How to build an execution environment

Execute the following sql file (create procedure, etc.) in PostgreSQL.

- bb_measurement.sql
- bb_measurement_value.sql
- bench_batch.sql



## How to execute

Execute from psql as follows.

```sql
\timing on
call bench_batch('2020-09-15', '1,2,3');
```

The first argument is the batch reference date (same as the one specified when creating the initial data).

The second argument is the factory ID to be processed. Multiple specifications can be specified by separating them with commas. You can specify a range using a hyphen, such as "1-3".
If omitted, all factories are targeted.

The third argument is the commit rate. 100 to always commit, 0 to always rollback. The default is 100.

