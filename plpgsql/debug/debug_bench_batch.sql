-- bom tree
create or replace procedure bb_debug_select_bom_tree_r(
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
  raise info '% bb_debug_select_bom_tree_r % start', clock_timestamp(), parent_index;
  pnode := node_list[parent_index];

  raise info '% bb_debug_select_bom_tree_r % loop start', clock_timestamp(), parent_index;
  for construct in select * from item_construction_master where ic_parent_i_id = pnode.item_id and context.batch_date between ic_effective_date and ic_expired_date order by ic_i_id loop
    raise info '% bb_debug_select_bom_tree_r % nodeA', clock_timestamp(), parent_index;

    node.index := array_length(node_list, 1) + 1;
    node.item_id := construct.ic_i_id;
    node.parent_item_id := pnode.item_id;
    node.construct := construct;
    node.child_list := array[]::int[];
    node_list[node.index] := node;

    i := coalesce(array_length(pnode.child_list, 1), 0) + 1;
    pnode.child_list[i] := node.index;

    raise info '% bb_debug_select_bom_tree_r % nodeB', clock_timestamp(), parent_index;
    call bb_debug_select_bom_tree_r(context, node_list, node.index);
    raise info '% bb_debug_select_bom_tree_r % nodeC', clock_timestamp(), parent_index;
  end loop;
  raise info '% bb_debug_select_bom_tree_r % loop end', clock_timestamp(), parent_index;

  node_list[parent_index] := pnode;
  raise info '% bb_debug_select_bom_tree_r % end', clock_timestamp(), parent_index;
end
$$;

create or replace function bb_debug_get_bom_tree(
  context bb_context,
  manufact item_manufacturing_master
) returns bb_bom_node[]
language plpgsql
as $$
declare
  root      bb_bom_node;
  node_list bb_bom_node[];
begin
  raise info '% bb_debug_get_bom_tree start', clock_timestamp();
  root.index := 1;
  root.item_id := manufact.im_i_id;
  root.parent_item_id := 0;
  root.manufact := manufact;
  root.child_list := array[]::int[];
  node_list[root.index] := root;

  raise info '% bb_debug_get_bom_tree1', clock_timestamp();
  call bb_debug_select_bom_tree_r(context, node_list, root.index);

  raise info '% bb_debug_get_bom_tree end', clock_timestamp();
  return node_list;
end
$$;

--select bb_debug_get_bom_tree(('2020-09-15',1),(1,67586,null,null,42000));
