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

end;
/
