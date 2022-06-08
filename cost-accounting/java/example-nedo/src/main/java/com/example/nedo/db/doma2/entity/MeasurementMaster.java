package com.example.nedo.db.doma2.entity;

import java.math.BigDecimal;
import org.seasar.doma.Column;
import org.seasar.doma.Entity;
import org.seasar.doma.Id;
import org.seasar.doma.Table;

import com.example.nedo.db.doma2.domain.MeasurementType;

@Entity
@Table(name = "measurement_master")
public class MeasurementMaster implements Cloneable {

    /** variable text(5) */
    @Column(name = "m_unit")
    @Id
    String mUnit;

    /** variable text(20) */
    @Column(name = "m_name")
    String mName;

    /** variable text(10) */
    @Column(name = "m_type")
    MeasurementType mType;

    /** variable text(5) */
    @Column(name = "m_default_unit")
    String mDefaultUnit;

    /** unsigned numeric(13, 6) */
    @Column(name = "m_scale")
    BigDecimal mScale;

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

    @Override
    public String toString() {
        return "MeasurementMaster(m_unit=" + mUnit + ", m_name=" + mName + ", m_type=" + mType + ", m_default_unit=" + mDefaultUnit + ", m_scale=" + mScale + ")";
    }
}
