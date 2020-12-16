-- for test
create or replace package bench_batch
is
  type bb_factory_id_list is table of factory_master.f_id%type;
  function bb_get_factory_list(factories varchar2) return bb_factory_id_list;
/*
declare
  list bench_batch.bb_factory_id_list;
begin
  list := bench_batch.bb_get_factory_list(null);
  dbms_output.put('null:');
  for i in 1..list.count loop
    dbms_output.put(' '||i);
  end loop;
  dbms_output.put_line('');

  list := bench_batch.bb_get_factory_list('');
  dbms_output.put('empty:');
  for i in 1..list.count loop
    dbms_output.put(' '||i);
  end loop;
  dbms_output.put_line('');

  list := bench_batch.bb_get_factory_list('1');
  dbms_output.put('one:');
  for i in 1..list.count loop
    dbms_output.put(' '||i);
  end loop;
  dbms_output.put_line('');

  list := bench_batch.bb_get_factory_list('1,2');
  dbms_output.put('two:');
  for i in 1..list.count loop
    dbms_output.put(' '||i);
  end loop;
  dbms_output.put_line('');
end;
*/

end;
/
