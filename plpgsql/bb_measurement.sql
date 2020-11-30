create or replace function bb_measurement_convert_unit(
  value numeric,
  src_unit text,
  dst_unit text
) returns numeric
language plpgsql
as $$
declare
  src measurement_master;
  dst measurement_master;
begin
  if dst_unit = src_unit then
    return value;
  end if;

  select * into src from measurement_master where m_unit = src_unit;
  select * into dst from measurement_master where m_unit = dst_unit;
  if src is null or dst is null then
    return value;
  end if;
  if src.m_type <> dst.m_type then
    raise exception 'src=%, dst=%', src, dst;
  end if;

  return value * src.m_scale / dst.m_scale;
end
$$;

create or replace function bb_measurement_convert_price_unit(
  value numeric,
  src_unit text,
  dst_unit text
) returns numeric
language plpgsql
as $$
begin
  return bb_measurement_convert_unit(value, dst_unit, src_unit);
end
$$;

create or replace function bb_measurement_get_type(unit text) returns text
language plpgsql
as $$
declare
  entity measurement_master;
begin
  select * into entity from measurement_master where m_unit = unit;
  if entity is null then
    return 'other';
  end if;
  return entity.m_type;
end
$$;

create or replace function bb_measurement_get_common_unit(unit1 text, unit2 text) returns text
language plpgsql
as $$
declare
  entity1 measurement_master;
  entity2 measurement_master;
begin
  if unit1 = unit2 then
    return unit1;
  end if;

  select * into entity1 from measurement_master where m_unit = unit1;
  select * into entity2 from measurement_master where m_unit = unit2;
  if entity1.m_type <> entity2.m_type then
    raise exception 'entity1=%, entity2=%', entity1, entity2;
  end if;

  if entity1.m_scale <= entity2.m_scale then
    return unit1;
  else
    return unit2;
  end if;
end
$$;

create type bb_value_pair as
(
  unit text,
  value1 numeric,
  value2 numeric
);

create or replace function bb_measurement_get_common_unit_value(
  value1 bb_measurement_value,
  value2 bb_measurement_value
) returns bb_value_pair
language plpgsql
as $$
declare
  result bb_value_pair;
begin
  result.unit := bb_measurement_get_common_unit(value1.unit, value2.unit);
  result.value1 := bb_measurement_convert_unit(value1.value, value1.unit, result.unit);
  result.value2 := bb_measurement_convert_unit(value2.value, value2.unit, result.unit);
  return result;
end
$$;

create or replace function bb_measurement_is_weight(unit text) returns boolean
language plpgsql
as $$
declare
  entity measurement_master;
begin
  select * into entity from measurement_master where m_unit = unit;
  if entity is null then
    return false;
  end if;
  return entity.m_type = 'weight';
end
$$;
