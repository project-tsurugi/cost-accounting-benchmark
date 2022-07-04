set serveroutput on
declare
  task_name varchar2(100);
  sql_stmt  clob;
  status    number;
begin
  task_name := dbms_parallel_execute.generate_task_name();
  dbms_parallel_execute.create_task(task_name);
  dbms_parallel_execute.create_chunks_by_number_col(task_name, 'CBDB', 'FACTORY_MASTER', 'F_ID', 1);
  sql_stmt := 'begin
    dbms_output.put_line(:start_id || ''-'' || :end_id);
    bench_batch.bench_batch(''2020-09-15'', to_char(:start_id));
    end;';
  dbms_parallel_execute.run_task(task_name, sql_stmt, DBMS_SQL.NATIVE, parallel_level => null);

  status := dbms_parallel_execute.task_status(task_name);
  if status = dbms_parallel_execute.FINISHED then
    dbms_output.put_line('end FINISHED');
  else
    dbms_output.put_line('end status=' || status);
  end if;

  dbms_parallel_execute.drop_task(task_name);
end;
/
exit
