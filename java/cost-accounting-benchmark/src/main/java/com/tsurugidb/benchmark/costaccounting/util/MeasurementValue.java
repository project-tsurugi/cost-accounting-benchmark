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
import java.math.RoundingMode;
import java.util.Objects;

public class MeasurementValue {
    public final String unit;
    public final BigDecimal value;

    public MeasurementValue(String unit, BigDecimal value) {
        this.unit = unit;
        this.value = value;
    }

    public MeasurementValue add(MeasurementValue value) {
        if (Objects.equals(unit, value.unit)) {
            return new MeasurementValue(unit, this.value.add(value.value));
        }

        String commonUnit = MeasurementUtil.getCommonUnit(this.unit, value.unit);
        BigDecimal value1 = MeasurementUtil.convertUnit(this.value, this.unit, commonUnit);
        BigDecimal value2 = MeasurementUtil.convertUnit(value.value, value.unit, commonUnit);
        return new MeasurementValue(commonUnit, value1.add(value2));
    }

    public MeasurementValue multiply(BigDecimal value) {
        return new MeasurementValue(unit, this.value.multiply(value));
    }

    public BigDecimal divide(MeasurementValue value, RoundingMode roundingMode) {
        String commonUnit = MeasurementUtil.getCommonUnit(this.unit, value.unit);
        BigDecimal value1 = MeasurementUtil.convertUnit(this.value, this.unit, commonUnit);
        BigDecimal value2 = MeasurementUtil.convertUnit(value.value, value.unit, commonUnit);
        return value1.divide(value2, BenchConst.DECIMAL_SCALE, roundingMode);
    }

    public MeasurementValue convertUnit(String dstUnit) {
        if (Objects.equals(this.unit, dstUnit)) {
            return this;
        }

        BigDecimal dstValue = MeasurementUtil.convertUnit(this.value, this.unit, dstUnit);
        return new MeasurementValue(dstUnit, dstValue);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof MeasurementValue)) {
            return false;
        }

        MeasurementValue that = (MeasurementValue) obj;
        return this.unit.equals(that.unit) && this.value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(unit, value);
    }

    @Override
    public String toString() {
        return value + " " + unit;
    }
}
