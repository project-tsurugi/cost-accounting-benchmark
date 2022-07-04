package com.tsurugidb.benchmark.costaccounting.db.doma2.entity;

import java.math.BigDecimal;
import java.time.LocalDate;

import org.seasar.doma.Column;
import org.seasar.doma.Entity;

@Entity
public class ResultRequiredQuantity implements Cloneable {

    /** unique ID(4) */
    @Column(name = "r_f_id")
    Integer rFId;

    /** date(8) */
    @Column(name = "r_manufacturing_date")
    LocalDate rManufacturingDate;

    /** unique ID(9) */
    @Column(name = "r_i_id")
    Integer rIId;

    /** variable text(5) */
    @Column(name = "r_required_quantity_unit")
    String rRequiredQuantityUnit;

    /** unsigned numeric(15, 4) */
    @Column(name = "r_required_quantity")
    BigDecimal rRequiredQuantity;

    public void setRFId(Integer value) {
        this.rFId = value;
    }

    public Integer getRFId() {
        return this.rFId;
    }

    public void setRManufacturingDate(LocalDate value) {
        this.rManufacturingDate = value;
    }

    public LocalDate getRManufacturingDate() {
        return this.rManufacturingDate;
    }

    public void setRIId(Integer value) {
        this.rIId = value;
    }

    public Integer getRIId() {
        return this.rIId;
    }

    public void setRRequiredQuantityUnit(String value) {
        this.rRequiredQuantityUnit = value;
    }

    public String getRRequiredQuantityUnit() {
        return this.rRequiredQuantityUnit;
    }

    public void setRRequiredQuantity(BigDecimal value) {
        this.rRequiredQuantity = value;
    }

    public BigDecimal getRRequiredQuantity() {
        return this.rRequiredQuantity;
    }

    @Override
    public ResultRequiredQuantity clone() {
        try {
            return (ResultRequiredQuantity) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new InternalError(e);
        }
    }
}
