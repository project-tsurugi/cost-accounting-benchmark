※bench_batch.body.sqlに追加して呼び出す

  --test
  procedure test_get_factory_list
  is
    list bb_factory_id_list;
  begin
    list := bb_get_factory_list(null);
    dbms_output.put('null:');
    for i in 1..list.count loop
      dbms_output.put(' '||i);
    end loop;
    dbms_output.put_line('');

    list := bb_get_factory_list('');
    dbms_output.put('empty:');
    for i in 1..list.count loop
      dbms_output.put(' '||i);
    end loop;
    dbms_output.put_line('');

    list := bb_get_factory_list('1');
    dbms_output.put('one:');
    for i in 1..list.count loop
      dbms_output.put(' '||i);
    end loop;
    dbms_output.put_line('');

    list := bb_get_factory_list('1,2');
    dbms_output.put('two:');
    for i in 1..list.count loop
      dbms_output.put(' '||i);
    end loop;
    dbms_output.put_line('');
  end;

  procedure debug_dump_bom_tree(
    node_list bb_bom_node_list,
    node_index int
  )
  is
    node bb_bom_node;
  begin
    node := node_list(node_index);
    dbms_output.put_line('index=' || node.node_index || ', item=' || node.item_id);

    for i in 1..node.child_list.count loop
      debug_dump_bom_tree(node_list, node.child_list(i));
    end loop;
  end;

  procedure test_get_bom_tree
  is
    context  bb_context;
    manufact item_manufacturing_master%rowtype;
    node_list bb_bom_node_list;
    c int;
  begin
    context.batch_date := '2020-09-15';
    context.factory_id := 1;
    c := 0;
    for row in (select * from item_manufacturing_master where im_f_id = context.factory_id and context.batch_date between im_effective_date and im_expired_date) loop
      manufact := row;

      dbms_output.put_line('im_i_id=' || manufact.im_i_id);

      node_list := bb_get_bom_tree(context, manufact);

      dbms_output.put_line('node_list.count=' || node_list.count);
      debug_dump_bom_tree(node_list, 1);

      c := c + 1;
      exit when c=3;
    end loop;
  end;

  procedure test
  is
  begin
    --test_get_factory_list;
    test_get_bom_tree;
  end;
