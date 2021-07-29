package com.example.nedo.init;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;

import com.example.nedo.BenchConst;

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
