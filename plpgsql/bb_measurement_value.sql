create type bb_measurement_value as (
  value numeric,
  unit text
);

create or replace function bb_measurement_value_create(unit text, value numeric) returns bb_measurement_value
language plpgsql
as $$
declare
  v bb_measurement_value;
begin
  v.unit  := unit;
  v.value := value;
  return v;
end
$$;

create or replace function bb_measurement_value_add(
  value1 bb_measurement_value,
  value2 bb_measurement_value
) returns bb_measurement_value
language plpgsql
as $$
declare
  common_unit text;
  v1 numeric;
  v2 numeric;
begin
  if value1 is null then
    return value2;
  end if;
  if value1.unit = value2.unit then
    return bb_measurement_value_create(value1.unit, value1.value + value2.value);
  end if;

  common_unit := bb_measurement_get_common_unit(value1.unit, value2.unit);
  v1 := bb_measurement_convert_unit(value1.value, value1.unit, common_unit);
  v2 := bb_measurement_convert_unit(value2.value, value2.unit, common_unit);
  return bb_measurement_value_create(common_unit, v1 + v2);
end
$$;

create or replace function bb_measurement_value_multiply(
  value1 bb_measurement_value,
  value2 numeric
) returns bb_measurement_value
language plpgsql
as $$
begin
  return bb_measurement_value_create(value1.unit, value1.value * value2);
end
$$;

create or replace function bb_measurement_value_divide(
  value1 bb_measurement_value,
  value2 bb_measurement_value
) returns numeric
language plpgsql
as $$
declare
  common_unit text;
  v1 numeric;
  v2 numeric;
begin
  common_unit := bb_measurement_get_common_unit(value1.unit, value2.unit);
  v1 := bb_measurement_convert_unit(value1.value, value1.unit, common_unit);
  v2 := bb_measurement_convert_unit(value2.value, value2.unit, common_unit);
  return v1 / v2;
end
$$;

create or replace function bb_measurement_value_convert_unit(
  value    bb_measurement_value,
  dst_unit text
) returns bb_measurement_value
language plpgsql
as $$
begin
  if value.unit = dst_unit then
    return value;
  end if;
  return bb_measurement_value_create(
    dst_unit,
    bb_measurement_convert_unit(value.value, value.unit, dst_unit)
  );
end
$$;
