create type bb_context as
(
  batch_date date,
  factory_id int
);

create type bb_bom_node as
(
  node_index int, -- index of node_list
  item_id        int,
  parent_item_id int,
  manufact  item_manufacturing_master,
  construct item_construction_master,
  item      item_master,
  child_list int[], -- index list of node_list

  weight       bb_measurement_value,
  weight_total bb_measurement_value,
  weight_ratio numeric,

  standard_quantity bb_measurement_value,
  required_quantity bb_measurement_value,

  unit_cost                numeric,
  total_unit_cost          numeric,
  manufacturing_cost       numeric,
  total_manufacturing_cost numeric
);

-- factory list
create or replace function bb_get_factory_list(factories text) returns int[]
language plpgsql
as $$
declare
  i int;
  s text;
  n int;
  r int[];
begin
  if factories is null or length(factories) = 0 then
    return array(select f_id from factory_master order by f_id);
  else
    i := 0;
    foreach s in array string_to_array(factories, ',') loop
      n := strpos(s, '-');
      if n > 0 then
        for j in substring(s for n - 1)..substring(s from n + 1) loop
          i := i + 1;
          r[i] := j;
        end loop;
      else
        i := i + 1;
        r[i] := s;
      end if;
    end loop;
    return r;
  end if;
end
$$;

-- bom tree
create or replace procedure bb_select_bom_tree_r(
  context bb_context,
  inout node_list bb_bom_node[],
  parent_index int
)
language plpgsql
as $$
declare
  construct item_construction_master;
  pnode bb_bom_node;
  node  bb_bom_node;
  i int;
begin
  pnode := node_list[parent_index];

  for construct in select * from item_construction_master where ic_parent_i_id = pnode.item_id and context.batch_date between ic_effective_date and ic_expired_date order by ic_i_id loop
    --raise debug 'bb_select_bom_tree_r = %', construct;

    node.node_index := array_length(node_list, 1) + 1;
    node.item_id        := construct.ic_i_id;
    node.parent_item_id := pnode.item_id;
    node.construct := construct;
    node.child_list := array[]::int[];
    node_list[node.node_index] := node;

    i := coalesce(array_length(pnode.child_list, 1), 0) + 1;
    pnode.child_list[i] := node.node_index;

    call bb_select_bom_tree_r(context, node_list, node.node_index);
  end loop;

  node_list[parent_index] := pnode;
end
$$;

create or replace function bb_get_bom_tree(
  context bb_context,
  manufact item_manufacturing_master
) returns bb_bom_node[]
language plpgsql
as $$
declare
  root      bb_bom_node;
  node_list bb_bom_node[];
--  node bb_bom_node;
begin
  root.node_index := 1;
  root.item_id        := manufact.im_i_id;
  root.parent_item_id := 0;
  root.manufact := manufact;
  root.child_list := array[]::int[];
  node_list[root.node_index] := root;

  call bb_select_bom_tree_r(context, node_list, root.node_index);

/*  --debug dump
  foreach node in array node_list loop
    raise debug 'node = %', node;
  end loop;
*/
  return node_list;
end
$$;

-- calculate weight
create or replace procedure bb_calculate_weight(
  context bb_context,
  inout node_list bb_bom_node[],
  node_index int
)
language plpgsql
as $$
declare
  node bb_bom_node;
  quantity numeric;
  weight_total bb_measurement_value;
  child_index int;
begin
  node := node_list[node_index];
--  raise debug 'bb_calculate_weight[%] item=%', node.node_index, node.item_id;

  if (node.construct).ic_material_quantity is null then
    node.weight := bb_measurement_value_create('mg', 0);
  else
    if bb_measurement_is_weight((node.construct).ic_material_unit) then
      node.weight := bb_measurement_value_create((node.construct).ic_material_unit, (node.construct).ic_material_quantity);
    else
      if node.item is null then
        select row(i.*) into node.item from item_master i where i_id = node.item_id and context.batch_date between i_effective_date and i_expired_date;
      end if;
      quantity := bb_measurement_convert_unit((node.construct).ic_material_quantity, (node.construct).ic_material_unit, (node.item).i_unit);
      node.weight := bb_measurement_value_create((node.item).i_weight_unit, quantity * (node.item).i_weight_ratio);
    end if;
  end if;
--  raise debug 'node[%].weight=%', node.node_index, node.weight;

  weight_total := node.weight;
  foreach child_index in array node.child_list loop
    call bb_calculate_weight(context, node_list, child_index);

    weight_total := bb_measurement_value_add(weight_total, node_list[child_index].weight_total);
  end loop;
  node.weight_total := weight_total;
--  raise debug 'node[%].weight_total=%', node.node_index, node.weight_total;

  node_list[node_index] := node;
end
$$;

create or replace procedure bb_calculate_weight_ratio(
  context bb_context,
  inout node_list bb_bom_node[],
  node_index int,
  root_weight_total bb_measurement_value
)
language plpgsql
as $$
declare
  node bb_bom_node;
  quantity numeric;
  weight_total bb_measurement_value;
  child_index int;
begin
  node := node_list[node_index];
--  raise debug 'bb_calculate_weight_ratio[%] item=%', node.node_index, node.item_id;

  if (node.weight_total).value = 0 then
    node.weight_ratio := 0;
  else
    node.weight_ratio := bb_measurement_value_divide(node.weight_total, root_weight_total) * 100;
  end if;
--  raise debug 'node[%].weight_ratio=%', node.node_index, node.weight_ratio;

  node_list[node_index] := node;

  foreach child_index in array node.child_list loop
    call bb_calculate_weight_ratio(context, node_list, child_index, root_weight_total);
  end loop;
end
$$;

-- calculate required quantity
create type bb_ratio as
(
  numerator   numeric,
  denominator numeric
);

create or replace procedure bb_calculate_required_quantity(
  context bb_context,
  inout node_list bb_bom_node[],
  node_index int,
  parent_ratio bb_ratio,
  manufacturing_quantity numeric
)
language plpgsql
as $$
declare
  node bb_bom_node;
  construct item_construction_master;
  ratio bb_ratio;
  required_unit text;
  child_index int;
begin
  node := node_list[node_index];
--  raise debug 'bb_calculate_required_quantity[%] item=%', node.node_index, node.item_id;

  construct := node.construct;
  if construct.ic_loss_ratio is not null then
    ratio.numerator   := parent_ratio.numerator   *  100;
    ratio.denominator := parent_ratio.denominator * (100 - construct.ic_loss_ratio);  
  else
    ratio := parent_ratio;
  end if;
--  raise debug 'node[%].ratio=% %', node.node_index, ratio, ratio.numerator / ratio.denominator;

  if construct.ic_material_quantity is null then
    if node.item is null then
      select row(i.*) into node.item from item_master i where i_id = node.item_id and context.batch_date between i_effective_date and i_expired_date;
    end if;
    node.standard_quantity := bb_measurement_value_create((node.item).i_unit, 0);
  else
    node.standard_quantity := bb_measurement_value_create(
      construct.ic_material_unit,
      construct.ic_material_quantity * ratio.numerator / ratio.denominator
    );
  end if;
--  raise debug 'node[%].standard_quantity=%', node.node_index, node.standard_quantity;

  required_unit := case bb_measurement_get_type((node.standard_quantity).unit)
    when 'capacity' then 'L'
    when 'weight'   then 'kg'
    else (node.standard_quantity).unit
  end;
  node.required_quantity := bb_measurement_value_convert_unit(
    bb_measurement_value_multiply(node.standard_quantity, manufacturing_quantity),
    required_unit
  );
--  raise debug 'node[%].required_quantity=%', node.node_index, node.required_quantity;

  node_list[node_index] := node;

  foreach child_index in array node.child_list loop
    call bb_calculate_required_quantity(context, node_list, child_index, ratio, manufacturing_quantity);
  end loop;
end
$$;

-- calculate cost
create or replace procedure bb_calculate_cost(
  context bb_context,
  inout node_list bb_bom_node[],
  node_index int,
  manufacturing_quantity numeric
)
language plpgsql
as $$
declare
  node bb_bom_node;
  cost cost_master;
  c bb_value_pair;
  common_unit text;
  total_manufacturing_cost numeric;
  child_index int;
begin
  node := node_list[node_index];
--  raise debug 'bb_calculate_cost[%] item=%', node.node_index, node.item_id;

  select * into cost from cost_master where c_f_id = context.factory_id and c_i_id = node.item_id;
  if not cost is null then
    c := bb_measurement_get_common_unit_value(
      bb_measurement_value_create(cost.c_stock_unit, cost.c_stock_quantity),
      node.required_quantity
    );
    node.total_unit_cost := cost.c_stock_amount * c.value2 / c.value1;
  else
    if node.item is null then
      select row(i.*) into node.item from item_master i where i_id = node.item_id and context.batch_date between i_effective_date and i_expired_date;
    end if;
    if (node.item).i_price is null then
      node.total_unit_cost := 0;
    else
      common_unit := bb_measurement_get_common_unit((node.item).i_price_unit, (node.required_quantity).unit);
      node.total_unit_cost := bb_measurement_convert_price_unit((node.item).i_price, (node.item).i_price_unit, common_unit)
        * bb_measurement_convert_unit((node.required_quantity).value, (node.required_quantity).unit, common_unit);
    end if;
  end if;
--  raise debug 'node[%].total_unit_cost=%', node.node_index, node.total_unit_cost;

  node.unit_cost := node.total_unit_cost / manufacturing_quantity;
--  raise debug 'node[%].unit_cost=%', node.node_index, node.unit_cost;

  total_manufacturing_cost := node.total_unit_cost;
  foreach child_index in array node.child_list loop
    call bb_calculate_cost(context, node_list, child_index, manufacturing_quantity);

    total_manufacturing_cost := total_manufacturing_cost + node_list[child_index].total_manufacturing_cost;
  end loop;
  node.total_manufacturing_cost := total_manufacturing_cost;
--  raise debug 'node[%].total_manufacturing_cost=%', node.node_index, node.total_manufacturing_cost;

  node.manufacturing_cost := total_manufacturing_cost / manufacturing_quantity;
--  raise debug 'node[%].manufacturing_cost=%', node.node_index, node.manufacturing_cost;

  node_list[node_index] := node;
end
$$;

-- execute
create or replace procedure bb_item_execute(
  context bb_context,
  manufact item_manufacturing_master
)
language plpgsql
as $$
declare
  node_list bb_bom_node[];
  node_list_size int;
  node      bb_bom_node;
  i int;
  j int;
  result result_table;
begin
  if manufact.im_manufacturing_quantity = 0 then
    return;
  end if;

  node_list := bb_get_bom_tree(context, manufact);

  call bb_calculate_weight(context, node_list, 1);
  call bb_calculate_weight_ratio(context, node_list, 1, node_list[1].weight_total);

  call bb_calculate_required_quantity(context, node_list, 1, (1,1), manufact.im_manufacturing_quantity);

  call bb_calculate_cost(context, node_list, 1, manufact.im_manufacturing_quantity);

  -- insert result
  node_list_size := array_length(node_list, 1);
  for i in 1..node_list_size loop
    node := node_list[i];
    continue when node is null;
    for j in i + 1..node_list_size loop
      if node_list[j].item_id = node.item_id and node_list[j].parent_item_id = node.parent_item_id then
        node.weight       := bb_measurement_value_add(node.weight, node_list[j].weight);
        node.weight_total := bb_measurement_value_add(node.weight_total, node_list[j].weight_total);
        node.weight_ratio := node.weight_ratio + node_list[j].weight_ratio;
        node.standard_quantity := bb_measurement_value_add(node.standard_quantity, node_list[j].standard_quantity);
        node.required_quantity := bb_measurement_value_add(node.required_quantity, node_list[j].required_quantity);
        node.unit_cost                := node.unit_cost + node_list[j].unit_cost;
        node.total_unit_cost          := node.total_unit_cost + node_list[j].total_unit_cost;
        node.manufacturing_cost       := node.manufacturing_cost + node_list[j].manufacturing_cost;
        node.total_manufacturing_cost := node.total_manufacturing_cost + node_list[j].total_manufacturing_cost;
        node_list[j] := null;
      end if;
    end loop;
    --node_list[i] := node;

    result.r_f_id := context.factory_id;
    result.r_manufacturing_date := context.batch_date;
    result.r_product_i_id := manufact.im_i_id;
    result.r_parent_i_id := node.parent_item_id;
    result.r_i_id := node.item_id;

    result.r_manufacturing_quantity := manufact.im_manufacturing_quantity;

    result.r_weight_unit := (node.weight).unit;
    result.r_weight      := (node.weight).value;
    result.r_weight_total_unit := (node.weight_total).unit;
    result.r_weight_total      := (node.weight_total).value;
    result.r_weight_ratio := node.weight_ratio;

    result.r_standard_quantity_unit := (node.standard_quantity).unit;
    result.r_standard_quantity      := (node.standard_quantity).value;
    result.r_required_quantity_unit := (node.required_quantity).unit;
    result.r_required_quantity      := (node.required_quantity).value;

    result.r_unit_cost                := node.unit_cost;
    result.r_total_unit_cost          := node.total_unit_cost;
    result.r_manufacturing_cost       := node.manufacturing_cost;
    result.r_total_manufacturing_cost := node.total_manufacturing_cost;

    insert into result_table values(result.*);
  end loop;
end
$$;

create or replace function bb_execute(context bb_context) returns numeric
language plpgsql
as $$
declare
  manufact item_manufacturing_master;
  result numeric;
begin
  raise log 'bb_execute start %', context;

  delete from result_table where r_f_id = context.factory_id and r_manufacturing_date = context.batch_date;

  result := 0;
  -- TODO parallel
  for manufact in select * from item_manufacturing_master where im_f_id = context.factory_id and context.batch_date between im_effective_date and im_expired_date loop
    --raise log 'bb_item_execute start %, %', context, manufact.im_i_id;
    call bb_item_execute(context, manufact);
    --raise log 'bb_item_execute end   %, %', context, manufact.im_i_id;

    result := result + 1;
  end loop;

  raise log 'bb_execute end   %', context;
  return result;
end
$$;

create or replace function bb_list_to_string(list int[]) returns text
language plpgsql
as $$
declare
  s text;
  block_end   int;
  block_count int;
  comma text;
  list_size int;
  n int;
  EMPTY constant int := -1;
begin
  s := '[';

  block_end := 0;
  block_count := 0;
  comma := '';
  list_size := array_length(list, 1);
  for i in 1..list_size + 1 loop
    n := EMPTY;
    if i <= list_size then
      n := list[i];
      if i > 1 and n = list[i - 1] + 1 then
        block_end := n;
        block_count := block_count + 1;
        continue;
      end if;
    end if;

    if block_end > 0 then
      if block_count > 2 then
        s := s || '-';
      else
        s := s || ', ';
      end if;
      s := s || block_end;
      block_end := 0;
    end if;

    if n <> EMPTY then
      s := s || comma || n;
      comma := ', ';
      block_count := 1;
    end if;
  end loop;

  return s || ']';
end
$$;

create or replace procedure bench_batch(
  batch_date date,
  factories text = '',
  commit_ratio integer = 100
)
language plpgsql
as $$
declare
  factory_list int[];
  factory_id   int;
  context bb_context;
  c numeric;
begin
  raise info 'batch_date = %', batch_date;

  factory_list := bb_get_factory_list(factories);
  raise info 'factory_list = %', bb_list_to_string(factory_list);

  raise info 'commit_ratio = %', commit_ratio;

  -- TODO parallel
  foreach factory_id in array factory_list loop
    context.batch_date := batch_date;
    context.factory_id := factory_id;
    c := bb_execute(context);

    if random() * 100 < commit_ratio then
      commit;
      raise info 'commit %, count=%', context, c;
    else
      rollback;
      raise info 'rollback %, count=%', context, c;
    end if;
  end loop;
end
$$;
