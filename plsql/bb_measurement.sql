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
