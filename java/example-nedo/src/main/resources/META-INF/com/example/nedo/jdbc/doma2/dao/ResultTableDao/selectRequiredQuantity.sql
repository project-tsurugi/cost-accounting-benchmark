select
  r_f_id,
  r_manufacturing_date,
  r_i_id,
  sum(r_required_quantity) r_required_quantity,
  max(r_required_quantity_unit) r_required_quantity_unit
from result_table r
left join item_master m on m.i_id=r.r_i_id
where r_f_id=/*factoryId*/1 and r_manufacturing_date=/*date*/'2020-09-23' and m.i_type='raw_material'
group by r_f_id, r_manufacturing_date, r_i_id
order by r_i_id
;
