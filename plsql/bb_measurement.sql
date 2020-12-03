create or replace package bb_measurement
is

  function convert_unit(
    value number,
    src_unit varchar2,
    dst_unit varchar2
  ) return number;

end;
/

create or replace package body bb_measurement
is
  unmatch_m_type_exception exception;

  function get_measurement_master(unit varchar2) return measurement_master%rowtype
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

end;
/
