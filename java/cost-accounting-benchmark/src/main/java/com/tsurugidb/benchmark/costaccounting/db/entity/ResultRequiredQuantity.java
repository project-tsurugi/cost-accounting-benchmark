package com.tsurugidb.benchmark.costaccounting.db.entity;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * required_quantity
 */
public class ResultRequiredQuantity implements Cloneable {

    /** r_f_id unique ID(4) */
    private Integer rFId;

    /** r_manufacturing_date date(8) */
    private LocalDate rManufacturingDate;

    /** r_i_id unique ID(9) */
    private Integer rIId;

    /** r_required_quantity_unit variable text(5) */
    private String rRequiredQuantityUnit;

    /** r_required_quantity unsigned numeric(15, 4) */
    private BigDecimal rRequiredQuantity;

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
