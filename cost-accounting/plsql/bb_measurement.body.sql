create or replace package body bb_measurement
is
  unmatch_m_type_exception exception;

  function get_measurement_master(unit varchar2) return measurement_master%rowtype
  result_cache
  is
    entity measurement_master%rowtype;
  begin
    select * into entity from measurement_master where m_unit = unit;
    return entity;
  exception
    when NO_DATA_FOUND then
      return null;
  end;

  function convert_unit(
    value number,
    src_unit varchar2,
    dst_unit varchar2
  ) return number
  is
    src measurement_master%rowtype;
    dst measurement_master%rowtype;
  begin
    if dst_unit = src_unit then
      return value;
    end if;

    src := get_measurement_master(src_unit);
    dst := get_measurement_master(dst_unit);
    if src.m_type is null or dst.m_type is null then
      return value;
    end if;
    if src.m_type <> dst.m_type then
      dbms_output.put_line('src='||src.m_type||', dst='||dst.m_type);
      raise unmatch_m_type_exception;
    end if;

    return value * src.m_scale / dst.m_scale;
  end;

  function convert_price_unit(
    value number,
    src_unit varchar2,
    dst_unit varchar2
  ) return number
  is
  begin
    return convert_unit(value, dst_unit, src_unit);
  end;

  function get_type(unit varchar2) return varchar2
  is
    entity measurement_master%rowtype;
  begin
    entity := get_measurement_master(unit);
    if entity.m_type is null then
      return 'other';
    end if;
    return entity.m_type;
  end;

  function get_common_unit(
    unit1 varchar2,
    unit2 varchar2
  ) return varchar2
  is
    entity1 measurement_master%rowtype;
    entity2 measurement_master%rowtype;
  begin
    entity1 := get_measurement_master(unit1);
    entity2 := get_measurement_master(unit2);
    if entity1.m_type <> entity2.m_type then
      dbms_output.put_line('entity1='||entity1.m_type||', entity2='||entity2.m_type);
      raise unmatch_m_type_exception;
    end if;

    if entity1.m_scale <= entity2.m_scale then
      return unit1;
    else
      return unit2;
    end if;
  end;

  function get_common_unit_value(
    value1 varchar2,
    unit1 varchar2,
    unit2 varchar2
  ) return varchar2
  is
    entity1 measurement_master%rowtype;
    entity2 measurement_master%rowtype;
  begin
    entity1 := get_measurement_master(unit1);
    entity2 := get_measurement_master(unit2);
    if entity1.m_type <> entity2.m_type then
      dbms_output.put_line('entity1='||entity1.m_type||', entity2='||entity2.m_type);
      raise unmatch_m_type_exception;
    end if;

    if entity1.m_scale <= entity2.m_scale then
      return unit1;
    else
      return unit2;
    end if;
  end;

  function is_weight(unit varchar2) return boolean
  is
  begin
    return get_type(unit) = 'weight';
  end;

end;
/
