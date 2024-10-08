--
-- Copyright 2023-2024 Project Tsurugi.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--
create or replace package bb_measurement_value
is

  type bb_measurement_value is record (
    value number,
    unit measurement_master.m_unit%type
  );

  function create_value(unit varchar2, value number) return bb_measurement_value;

  function add(
    value1 bb_measurement_value,
    value2 bb_measurement_value
  ) return bb_measurement_value;

  function multiply(
    value1 bb_measurement_value,
    value2 number
  ) return bb_measurement_value;

  function divide(
    value1 bb_measurement_value,
    value2 bb_measurement_value
  ) return number;

  function convert_unit(
    value    bb_measurement_value,
    dst_unit varchar2
  ) return bb_measurement_value;

  type bb_value_pair is record (
    unit measurement_master.m_unit%type,
    value1 number,
    value2 number
  );

  function get_common_unit_value(
    value1 bb_measurement_value,
    value2 bb_measurement_value
  ) return bb_value_pair;

  function dump(value bb_measurement_value) return varchar2;
end;
/
