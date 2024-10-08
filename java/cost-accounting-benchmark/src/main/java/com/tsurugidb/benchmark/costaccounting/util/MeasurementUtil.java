/*
 * Copyright 2023-2024 Project Tsurugi.
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
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.tsurugidb.benchmark.costaccounting.db.dao.MeasurementMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.domain.MeasurementType;
import com.tsurugidb.benchmark.costaccounting.db.entity.MeasurementMaster;

public class MeasurementUtil {

    private static MeasurementMasterDao DAO;
    volatile static Map<String, MeasurementMaster> MAP;

    public static void initialize(MeasurementMasterDao dao) {
        MeasurementUtil.DAO = dao;
    }

    private static MeasurementMaster getMeasurementMaster(String unit) {
        if (MAP == null) {
            synchronized (MeasurementUtil.class) {
                Map<String, MeasurementMaster> map = new HashMap<>();

                List<MeasurementMaster> list = DAO.selectAll();
                for (MeasurementMaster entity : list) {
                    map.put(entity.getMUnit(), entity);
                }

                MAP = map;
            }
        }
        return MAP.get(unit);
    }

    public static BigDecimal convertUnit(BigDecimal value, String srcUnit, String dstUnit) {
        if (Objects.equals(srcUnit, dstUnit)) {
            return value;
        }

        MeasurementMaster src = getMeasurementMaster(srcUnit);
        MeasurementMaster dst = getMeasurementMaster(dstUnit);
        if (!equalsMType(src, dst)) {
            throw new RuntimeException("src=" + src + " dst=" + dst);
        }

        BigDecimal s = value.multiply(src.getMScale());
        BigDecimal d = s.divide(dst.getMScale(), BenchConst.DECIMAL_SCALE, RoundingMode.HALF_UP);
        return d;
    }

    public static BigDecimal convertPriceUnit(BigDecimal value, String srcUnit, String dstUnit) {
        return convertUnit(value, dstUnit, srcUnit); // 例えば 円/mg→円/g なので、逆変換
    }

    public static MeasurementType getType(String unit) {
        MeasurementMaster entity = getMeasurementMaster(unit);
        if (entity == null) {
            return MeasurementType.OTHER;
        }
        return entity.getMType();
    }

    public static String toDefaultUnit(String unit) {
        MeasurementMaster entity = getMeasurementMaster(unit);
        if (entity == null) {
            return unit;
        }
        return entity.getMDefaultUnit();
    }

    public static String getCommonUnit(String unit1, String unit2) {
        if (Objects.equals(unit1, unit2)) {
            return unit1;
        }

        MeasurementMaster entity1 = getMeasurementMaster(unit1);
        MeasurementMaster entity2 = getMeasurementMaster(unit2);
        if (!equalsMType(entity1, entity2)) {
            throw new RuntimeException("entity1=" + entity1 + " entity2=" + entity2);
        }

        if (entity1.getMScale().compareTo(entity2.getMScale()) <= 0) {
            return unit1;
        } else {
            return unit2;
        }
    }

    private static boolean equalsMType(MeasurementMaster entity1, MeasurementMaster entity2) {
        if (entity1 == null || entity2 == null) {
            return entity1 == null && entity2 == null;
        }
        return Objects.equals(entity1.getMType(), entity2.getMType());
    }

    public static class ValuePair {
        public final String unit;
        public final BigDecimal value1;
        public final BigDecimal value2;

        public ValuePair(String unit, BigDecimal value1, BigDecimal value2) {
            this.unit = unit;
            this.value1 = value1;
            this.value2 = value2;
        }
    }

    public static ValuePair getCommonUnitValue(MeasurementValue value1, MeasurementValue value2) {
        String commonUnit = getCommonUnit(value1.unit, value2.unit);
        BigDecimal v1 = convertUnit(value1.value, value1.unit, commonUnit);
        BigDecimal v2 = convertUnit(value2.value, value2.unit, commonUnit);
        return new ValuePair(commonUnit, v1, v2);
    }

    public static boolean isWeight(String unit) {
        MeasurementMaster entity = getMeasurementMaster(unit);
        if (entity == null) {
            return false;
        }

        return entity.getMType() == MeasurementType.WEIGHT;
    }
}
