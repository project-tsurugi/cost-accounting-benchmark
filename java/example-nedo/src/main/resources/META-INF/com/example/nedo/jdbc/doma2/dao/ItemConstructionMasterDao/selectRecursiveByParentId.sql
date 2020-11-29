with recursive
ic as (
  select * from item_construction_master
  where /* date */'2020-09-23' between ic_effective_date and ic_expired_date
),
r as (
  select * from ic
  where ic_parent_i_id=/* parentId */1
  union all
  select ic.* from ic, r
  where ic.ic_parent_i_id = r.ic_i_id
)
select * from r
order by ic_parent_i_id, ic_i_id;
