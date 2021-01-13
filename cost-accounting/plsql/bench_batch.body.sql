create or replace package body bench_batch
is

  type bb_context is record (
    batch_date date,
    factory_id factory_master.f_id%type
  );

  type array_int is table of int;

  type bb_bom_node is record (
    node_index int, -- index of node_list
    item_id        item_master.i_id%type,
    parent_item_id item_master.i_id%type,
    manufact  item_manufacturing_master%rowtype,
    construct item_construction_master%rowtype,
    item      item_master%rowtype,
    child_list array_int, -- index list of node_list

    weight       bb_measurement_value.bb_measurement_value,
    weight_total bb_measurement_value.bb_measurement_value,
    weight_ratio number,

    standard_quantity bb_measurement_value.bb_measurement_value,
    required_quantity bb_measurement_value.bb_measurement_value,

    unit_cost                number,
    total_unit_cost          number,
    manufacturing_cost       number,
    total_manufacturing_cost number
  );
  type bb_bom_node_list is table of bb_bom_node;

  -- factory list
  type bb_factory_id_list is table of factory_master.f_id%type;

  function bb_get_factory_list(factories varchar2) return bb_factory_id_list
  is
    id_list bb_factory_id_list;

    start_pos pls_integer;
    s varchar2(1);
  begin
    id_list := bb_factory_id_list();

    if nvl(length(factories), 0) = 0 then
      for row in (select f_id from factory_master order by f_id) loop
        id_list.extend();
        id_list(id_list.last) := row.f_id;
      end loop;
    else
      start_pos := 1;
      for i in 1..length(factories) + 1 loop
        s := substr(factories, i, 1);
        if s = ',' or s is null then
          if i - start_pos >= 1 then
            id_list.extend();
            id_list(id_list.last) := substr(factories, start_pos, i - start_pos);
          end if;
          start_pos := i + 1;
        end if;
      end loop;
    end if;

    return id_list;
  end;

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

  --bom tree
  procedure bb_select_bom_tree_r(
    context bb_context,
    node_list in out nocopy bb_bom_node_list,
    parent_index int
  )
  is
    pnode bb_bom_node;
    node  bb_bom_node;
  begin
    pnode := node_list(parent_index);

    for row in (select * from item_construction_master where ic_parent_i_id = pnode.item_id and context.batch_date between ic_effective_date and ic_expired_date order by ic_i_id) loop
      node_list.extend();
      node.node_index := node_list.last;
      node.item_id        := row.ic_i_id;
      node.parent_item_id := pnode.item_id;
      node.construct := row;
      node.child_list := array_int();
      node_list(node.node_index) := node;

      pnode.child_list.extend();
      pnode.child_list(pnode.child_list.last) := node.node_index;

      bb_select_bom_tree_r(context, node_list, node.node_index);
    end loop;

    node_list(parent_index) := pnode;
  end;

  function bb_get_bom_tree(
    context bb_context,
    manufact item_manufacturing_master%rowtype
  ) return bb_bom_node_list
  is
    root      bb_bom_node;
    node_list bb_bom_node_list := bb_bom_node_list();
  begin
    node_list.extend();
    root.node_index := node_list.last;
    root.item_id        := manufact.im_i_id;
    root.parent_item_id := 0;
    root.manufact := manufact;
    root.child_list := array_int();
    node_list(root.node_index) := root;

    bb_select_bom_tree_r(context, node_list, root.node_index);

    return node_list;
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

  --test
  procedure test
  is
  begin
    --test_get_factory_list;
    test_get_bom_tree;
  end;
end;
/
