select ic.*
from item_construction_master ic
left join item_master i on i_id=ic_i_id and /*date*/'2020-09-23' between i_effective_date and i_expired_date
where /*date*/'2020-09-23' between ic_effective_date and ic_expired_date
and i_type in/*typeList*/('raw_material')
;
