create or replace package body bench_batch
is
  type bb_factory_id_list is table of factory_master.f_id%type;

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

  type bb_ratio is record (
    numerator   number,
    denominator number
  );

  -- factory list
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

  -- calculate weight
  procedure bb_calculate_weight(
    context bb_context,
    node_list in out nocopy bb_bom_node_list,
    node_index int
  )
  is
    node bb_bom_node;
    quantity number;
    weight_total bb_measurement_value.bb_measurement_value;
    child_index int;
  begin
    node := node_list(node_index);
    --dbms_output.put_line('bb_calculate_weight[' || node.node_index || '] item=' || node.item_id);

    if node.construct.ic_material_quantity is null then
      node.weight := bb_measurement_value.create_value('mg', 0);
    else
      if bb_measurement.is_weight(node.construct.ic_material_unit) then
        node.weight := bb_measurement_value.create_value(node.construct.ic_material_unit, node.construct.ic_material_quantity);
      else
        if node.item.i_id is null then
          select i.* into node.item from item_master i where i_id = node.item_id and context.batch_date between i_effective_date and i_expired_date;
        end if;
        quantity := bb_measurement.convert_unit(node.construct.ic_material_quantity, node.construct.ic_material_unit, node.item.i_unit);
        node.weight := bb_measurement_value.create_value(node.item.i_weight_unit, quantity * node.item.i_weight_ratio);
      end if;
    end if;
    --dbms_output.put_line('node[' || node.node_index || '].weight=' || bb_measurement_value.dump(node.weight));

    weight_total := node.weight;
    for i in 1..node.child_list.count loop
      child_index := node.child_list(i);
      bb_calculate_weight(context, node_list, child_index);

      weight_total := bb_measurement_value.add(weight_total, node_list(child_index).weight_total);
    end loop;
    node.weight_total := weight_total;
    --dbms_output.put_line('node[' || node.node_index || '].weight_total=' || bb_measurement_value.dump(node.weight_total));

    node_list(node_index) := node;
  end;

  procedure bb_calculate_weight_ratio(
    context bb_context,
    node_list in out nocopy bb_bom_node_list,
    node_index int,
    root_weight_total bb_measurement_value.bb_measurement_value
  )
  is
    node bb_bom_node;
    quantity number;
    weight_total bb_measurement_value.bb_measurement_value;
    child_index int;
  begin
    node := node_list(node_index);
    --dbms_output.put_line('bb_calculate_weight_ratio[' || node.node_index || '] item=' || node.item_id);

    if node.weight_total.value = 0 then
      node.weight_ratio := 0;
    else
      node.weight_ratio := bb_measurement_value.divide(node.weight_total, root_weight_total) * 100;
    end if;
    --dbms_output.put_line('node[' || node.node_index || '].weight_ratio=' || node.weight_ratio);

    node_list(node_index) := node;

    for i in 1..node.child_list.count loop
      child_index := node.child_list(i);
      bb_calculate_weight_ratio(context, node_list, child_index, root_weight_total);
    end loop;
  end;

  -- calculate required quantity
  procedure bb_calculate_required_quantity(
    context bb_context,
    node_list in out nocopy bb_bom_node_list,
    node_index int,
    parent_ratio bb_ratio,
    manufacturing_quantity number
  )
  is
    node bb_bom_node;
    construct item_construction_master%rowtype;
    ratio bb_ratio;
    required_unit measurement_master.m_unit%type;
    child_index int;
  begin
    node := node_list(node_index);
    --dbms_output.put_line('bb_calculate_required_quantity[' || node.node_index || '] item=' || node.item_id);

    construct := node.construct;
    if construct.ic_loss_ratio is not null then
      ratio.numerator   := parent_ratio.numerator   *  100;
      ratio.denominator := parent_ratio.denominator * (100 - construct.ic_loss_ratio);  
    else
      ratio := parent_ratio;
    end if;
    --dbms_output.put_line('node[' || node.node_index || '].ratio=' || ratio.numerator || '/' || ratio.denominator || ' ' || ratio.numerator / ratio.denominator);

    if construct.ic_material_quantity is null then
      if node.item.i_id is null then
        select i.* into node.item from item_master i where i_id = node.item_id and context.batch_date between i_effective_date and i_expired_date;
      end if;
      node.standard_quantity := bb_measurement_value.create_value(node.item.i_unit, 0);
    else
      node.standard_quantity := bb_measurement_value.create_value(
        construct.ic_material_unit,
        construct.ic_material_quantity * ratio.numerator / ratio.denominator
      );
    end if;
    --dbms_output.put_line('node[' || node.node_index || '].standard_quantity=' || bb_measurement_value.dump(node.standard_quantity));

    required_unit := case bb_measurement.get_type(node.standard_quantity.unit)
      when 'capacity' then 'L'
      when 'weight'   then 'kg'
      else node.standard_quantity.unit
    end;
    node.required_quantity := bb_measurement_value.convert_unit(
      bb_measurement_value.multiply(node.standard_quantity, manufacturing_quantity),
      required_unit
    );
    --dbms_output.put_line('node[' || node.node_index || '].required_quantity=' || bb_measurement_value.dump(node.required_quantity));

    node_list(node_index) := node;

    for i in 1..node.child_list.count loop
      child_index := node.child_list(i);
      bb_calculate_required_quantity(context, node_list, child_index, ratio, manufacturing_quantity);
    end loop;
  end;

  -- calculate cost
  procedure bb_calculate_cost(
    context bb_context,
    node_list in out nocopy bb_bom_node_list,
    node_index int,
    manufacturing_quantity number
  )
  is
    node bb_bom_node;
    cost cost_master%rowtype;
    c bb_measurement_value.bb_value_pair;
    common_unit measurement_master.m_unit%type;
    total_manufacturing_cost number;
    child_index int;
  begin
    node := node_list(node_index);
    --dbms_output.put_line('bb_calculate_cost[' || node.node_index || '] item=' || node.item_id);

    begin
      select * into cost from cost_master where c_f_id = context.factory_id and c_i_id = node.item_id;
      c := bb_measurement_value.get_common_unit_value(
        bb_measurement_value.create_value(cost.c_stock_unit, cost.c_stock_quantity),
        node.required_quantity
      );
      node.total_unit_cost := cost.c_stock_amount * c.value2 / c.value1;
    exception
    when NO_DATA_FOUND then
      if node.item.i_id is null then
        select i.* into node.item from item_master i where i_id = node.item_id and context.batch_date between i_effective_date and i_expired_date;
      end if;
      if node.item.i_price is null then
        node.total_unit_cost := 0;
      else
        common_unit := bb_measurement.get_common_unit(node.item.i_price_unit, node.required_quantity.unit);
        node.total_unit_cost := bb_measurement.convert_price_unit(node.item.i_price, node.item.i_price_unit, common_unit)
          * bb_measurement.convert_unit(node.required_quantity.value, node.required_quantity.unit, common_unit);
      end if;
    end;
    --dbms_output.put_line('node[' || node.node_index || '].total_unit_cost=' || node.total_unit_cost);

    node.unit_cost := node.total_unit_cost / manufacturing_quantity;
    --dbms_output.put_line('node[' || node.node_index || '].unit_cost=' || node.unit_cost);

    total_manufacturing_cost := node.total_unit_cost;
    for i in 1..node.child_list.count loop
      child_index := node.child_list(i);
      bb_calculate_cost(context, node_list, child_index, manufacturing_quantity);

      total_manufacturing_cost := total_manufacturing_cost + node_list(child_index).total_manufacturing_cost;
    end loop;
    node.total_manufacturing_cost := total_manufacturing_cost;
    --dbms_output.put_line('node[' || node.node_index || '].total_manufacturing_cost=' || node.total_manufacturing_cost);

    node.manufacturing_cost := total_manufacturing_cost / manufacturing_quantity;
    --dbms_output.put_line('node[' || node.node_index || '].manufacturing_cost=' || node.manufacturing_cost);

    node_list(node_index) := node;
  end;

  --execute
  procedure bb_item_execute(
    context bb_context,
    manufact item_manufacturing_master%rowtype
  )
  is
    node_list bb_bom_node_list;
    node_list_size int;
    node      bb_bom_node;
    ratio bb_ratio;
    i int;
    j int;
    result result_table%rowtype;
  begin
    if manufact.im_manufacturing_quantity = 0 then
      return;
    end if;

    node_list := bb_get_bom_tree(context, manufact);

    bb_calculate_weight(context, node_list, 1);
    bb_calculate_weight_ratio(context, node_list, 1, node_list(1).weight_total);

    ratio.numerator   := 1;
    ratio.denominator := 1;
    bb_calculate_required_quantity(context, node_list, 1, ratio, manufact.im_manufacturing_quantity);

    bb_calculate_cost(context, node_list, 1, manufact.im_manufacturing_quantity);

    -- insert result
    node_list_size := node_list.count;
    for i in 1..node_list_size loop
      continue when not node_list.exists(i);
      node := node_list(i);
      for j in i + 1..node_list_size loop
        if node_list(j).item_id = node.item_id and node_list(j).parent_item_id = node.parent_item_id then
          node.weight       := bb_measurement_value.add(node.weight, node_list(j).weight);
          node.weight_total := bb_measurement_value.add(node.weight_total, node_list(j).weight_total);
          node.weight_ratio := node.weight_ratio + node_list(j).weight_ratio;
          node.standard_quantity := bb_measurement_value.add(node.standard_quantity, node_list(j).standard_quantity);
          node.required_quantity := bb_measurement_value.add(node.required_quantity, node_list(j).required_quantity);
          node.unit_cost                := node.unit_cost + node_list(j).unit_cost;
          node.total_unit_cost          := node.total_unit_cost + node_list(j).total_unit_cost;
          node.manufacturing_cost       := node.manufacturing_cost + node_list(j).manufacturing_cost;
          node.total_manufacturing_cost := node.total_manufacturing_cost + node_list(j).total_manufacturing_cost;
          node_list.delete(j);
        end if;
      end loop;
      --node_list(i) := node;

      result.r_f_id := context.factory_id;
      result.r_manufacturing_date := context.batch_date;
      result.r_product_i_id := manufact.im_i_id;
      result.r_parent_i_id := node.parent_item_id;
      result.r_i_id := node.item_id;

      result.r_manufacturing_quantity := manufact.im_manufacturing_quantity;

      result.r_weight_unit := node.weight.unit;
      result.r_weight      := node.weight.value;
      result.r_weight_total_unit := node.weight_total.unit;
      result.r_weight_total      := node.weight_total.value;
      result.r_weight_ratio := node.weight_ratio;

      result.r_standard_quantity_unit := node.standard_quantity.unit;
      result.r_standard_quantity      := node.standard_quantity.value;
      result.r_required_quantity_unit := node.required_quantity.unit;
      result.r_required_quantity      := node.required_quantity.value;

      result.r_unit_cost                := node.unit_cost;
      result.r_total_unit_cost          := node.total_unit_cost;
      result.r_manufacturing_cost       := node.manufacturing_cost;
      result.r_total_manufacturing_cost := node.total_manufacturing_cost;

      insert into result_table values result;
    end loop;
  end;

  function bb_execute(context bb_context) return number
  is
    result number;
  begin
    dbms_output.put_line('bb_execute start ' || context.batch_date || ',' || context.factory_id);

    delete from result_table where r_f_id = context.factory_id and r_manufacturing_date = context.batch_date;

    result := 0;
    -- TODO parallel
    for manufact in (select * from item_manufacturing_master where im_f_id = context.factory_id and context.batch_date between im_effective_date and im_expired_date) loop
      --dbms_output.put_line('bb_item_execute start ' || manufact.im_i_id);
      bb_item_execute(context, manufact);
      --dbms_output.put_line('bb_item_execute end   ' || manufact.im_i_id);

      result := result + 1;
    end loop;

    dbms_output.put_line('bb_execute end   ' || context.batch_date || ',' || context.factory_id);
    return result;
  end;

  procedure bench_batch(
    batch_date date,
    factories varchar2 := '',
    commit_ratio integer := 100
  )
  is
    factory_list bb_factory_id_list;
    context bb_context;
    c number;
  begin
    dbms_output.put_line('batch_date = ' || batch_date);

    factory_list := bb_get_factory_list(factories);
    dbms_output.put('factory_list =');
    for i in 1..factory_list.count loop
      dbms_output.put(' ' || factory_list(i));
    end loop;
    dbms_output.put_line('');

    dbms_output.put_line('commit_ratio = ' || commit_ratio);

    -- TODO parallel
    for i in 1..factory_list.count loop
      context.batch_date := batch_date;
      context.factory_id := factory_list(i);
      c := bb_execute(context);

      if dbms_random.value(0, 99) < commit_ratio then
        commit;
        dbms_output.put_line('commit ' || context.batch_date || ',' || context.factory_id || ' count=' || c);
      else
        rollback;
        dbms_output.put_line('rollback ' || context.batch_date || ',' || context.factory_id || ' count=' || c);
      end if;
    end loop;
  end;
end;
/
