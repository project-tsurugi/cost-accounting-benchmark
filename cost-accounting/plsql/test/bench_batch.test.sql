-- for test
create or replace package bench_batch
is
  procedure test;
/* execute from sqlplus
exec bench_batch.test
*/
end;
/
