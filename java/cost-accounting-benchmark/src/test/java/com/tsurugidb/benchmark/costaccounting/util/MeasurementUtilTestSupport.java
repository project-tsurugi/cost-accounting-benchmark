/*
 * Copyright 2023-2025 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.benchmark.costaccounting.util;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import com.tsurugidb.benchmark.costaccounting.db.domain.MeasurementType;
import com.tsurugidb.benchmark.costaccounting.db.entity.MeasurementMaster;

public class MeasurementUtilTestSupport {

    public static void initializeForTest() {
        if (MeasurementUtil.MAP != null) {
            return;
        }

        Map<String, MeasurementMaster> map = new HashMap<>();
        String type, standardUnit;

        type = "length";
        standardUnit = "m";
        put(map, "um", type, standardUnit, "1E-6");
        put(map, "mm", type, standardUnit, "1E-3");
        put(map, "cm", type, standardUnit, "1E-2");
        put(map, "dm", type, standardUnit, "1E-1");
        put(map, "m", type, standardUnit, "1");
        put(map, "dam", type, standardUnit, "1E+1");
        put(map, "hm", type, standardUnit, "1E+2");
        put(map, "km", type, standardUnit, "1E+3");

        type = "capacity";
        standardUnit = "mL";
        put(map, "mL", type, standardUnit, "1");
        put(map, "cL", type, standardUnit, "1E+1");
        put(map, "dL", type, standardUnit, "1E+2");
        put(map, "L", type, standardUnit, "1E+3");
        put(map, "daL", type, standardUnit, "1E+4");
        put(map, "hL", type, standardUnit, "1E+5");
        put(map, "kL", type, standardUnit, "1E+6");

        type = "weight";
        standardUnit = "g";
        put(map, "mg", type, standardUnit, "1E-3");
        put(map, "cg", type, standardUnit, "1E-2");
        put(map, "dg", type, standardUnit, "1E-1");
        put(map, "g", type, standardUnit, "1");
        put(map, "dag", type, standardUnit, "1E+1");
        put(map, "hg", type, standardUnit, "1E+2");
        put(map, "kg", type, standardUnit, "1E+3");
        put(map, "t", type, standardUnit, "1E+6");

        MeasurementUtil.MAP = map;
    }

    private static void put(Map<String, MeasurementMaster> map, String unit, String type, String standardUnit, String scale) {
        MeasurementMaster entity = new MeasurementMaster();
        entity.setMUnit(unit);
        entity.setMType(MeasurementType.of(type));
        entity.setMDefaultUnit(standardUnit);
        entity.setMScale(new BigDecimal(scale));

        map.put(unit, entity);
    }
}
