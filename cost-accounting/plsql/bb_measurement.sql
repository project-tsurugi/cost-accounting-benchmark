create or replace package bb_measurement
is

  function convert_unit(
    value number,
    src_unit varchar2,
    dst_unit varchar2
  ) return number;

  function convert_price_unit(
    value number,
    src_unit varchar2,
    dst_unit varchar2
  ) return number;

  function get_type(unit varchar2) return varchar2;

  function get_common_unit(
    unit1 varchar2,
    unit2 varchar2
  ) return varchar2;

  function is_weight(unit varchar2) return boolean;

end;
/
