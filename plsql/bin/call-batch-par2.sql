--
-- Copyright 2023-2024 Project Tsurugi.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--
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
