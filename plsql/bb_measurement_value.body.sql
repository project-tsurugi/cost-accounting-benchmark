create or replace package body bb_measurement_value
is

  function create_value(unit varchar2, value number) return bb_measurement_value
  is
    v bb_measurement_value;
  begin
    v.unit  := unit;
    v.value := value;
    return v;
  end;

  function add(
    value1 bb_measurement_value,
    value2 bb_measurement_value
  ) return bb_measurement_value
  is
    common_unit measurement_master.m_unit%type;
    v1 number;
    v2 number;
  begin
    if value1.value is null then
      return value2;
    end if;
    if value1.unit = value2.unit then
      return create_value(value1.unit, value1.value + value2.value);
    end if;

    common_unit := bb_measurement.get_common_unit(value1.unit, value2.unit);
    v1 := bb_measurement.convert_unit(value1.value, value1.unit, common_unit);
    v2 := bb_measurement.convert_unit(value2.value, value2.unit, common_unit);
    return create_value(common_unit, v1 + v2);
  end;

  function multiply(
    value1 bb_measurement_value,
    value2 number
  ) return bb_measurement_value
  is
  begin
    return create_value(value1.unit, value1.value * value2);
  end;

  function divide(
    value1 bb_measurement_value,
    value2 bb_measurement_value
  ) return number
  is
    common_unit measurement_master.m_unit%type;
    v1 number;
    v2 number;
  begin
    common_unit := bb_measurement.get_common_unit(value1.unit, value2.unit);
    v1 := bb_measurement.convert_unit(value1.value, value1.unit, common_unit);
    v2 := bb_measurement.convert_unit(value2.value, value2.unit, common_unit);
    return v1 / v2;
  end;

  function convert_unit(
    value    bb_measurement_value,
    dst_unit varchar2
  ) return bb_measurement_value
  is
  begin
    if value.unit = dst_unit then
      return value;
    end if;
    return create_value(
      dst_unit,
      bb_measurement.convert_unit(value.value, value.unit, dst_unit)
    );
  end;

  function get_common_unit_value(
    value1 bb_measurement_value,
    value2 bb_measurement_value
  ) return bb_value_pair
  is
    result bb_value_pair;
  begin
    result.unit := bb_measurement.get_common_unit(value1.unit, value2.unit);
    result.value1 := bb_measurement.convert_unit(value1.value, value1.unit, result.unit);
    result.value2 := bb_measurement.convert_unit(value2.value, value2.unit, result.unit);
    return result;
  end;

  --debug
  function dump(value bb_measurement_value) return varchar2
  is
  begin
    return value.value || value.unit;
  end;
end;
/
