create or replace package bench_batch
is
  procedure bench_batch(
    batch_date date,
    factories varchar2 := '',
    commit_ratio integer := 100
  );
end;
/
