package com.tsurugidb.benchmark.costaccounting.db.entity;

import com.tsurugidb.benchmark.costaccounting.db.domain.MeasurementType;
import java.math.BigDecimal;

/**
 * measurement_master
 */
public class MeasurementMaster implements Cloneable {

    /** m_scale unsigned numeric(13, 6) */
    public static final int M_SCALE_SCALE = 6;

    /** m_unit variable text(5) */
    private String mUnit;

    /** m_name variable text(20) */
    private String mName;

    /** m_type variable text(10) */
    private MeasurementType mType;

    /** m_default_unit variable text(5) */
    private String mDefaultUnit;

    /** m_scale unsigned numeric(13, 6) */
    private BigDecimal mScale;

    public void setMUnit(String value) {
        this.mUnit = value;
    }

    public String getMUnit() {
        return this.mUnit;
    }

    public void setMName(String value) {
        this.mName = value;
    }

    public String getMName() {
        return this.mName;
    }

    public void setMType(MeasurementType value) {
        this.mType = value;
    }

    public MeasurementType getMType() {
        return this.mType;
    }

    public void setMDefaultUnit(String value) {
        this.mDefaultUnit = value;
    }

    public String getMDefaultUnit() {
        return this.mDefaultUnit;
    }

    public void setMScale(BigDecimal value) {
        this.mScale = value;
    }

    public BigDecimal getMScale() {
        return this.mScale;
    }

    @Override
    public MeasurementMaster clone() {
        try {
            return (MeasurementMaster) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new InternalError(e);
        }
    }

    public String toCsv(String suffix) {
        return mUnit + "," + mName + "," + mType + "," + mDefaultUnit + "," + mScale + suffix;
    }

    @Override
    public String toString() {
        return "MeasurementMaster(m_unit=" + mUnit + ", m_name=" + mName + ", m_type=" + mType + ", m_default_unit=" + mDefaultUnit + ", m_scale=" + mScale + ")";
    }
}
