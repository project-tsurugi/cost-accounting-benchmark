package com.tsurugidb.benchmark.costaccounting.db.entity;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * item_construction_master
 */
public class ItemConstructionMaster implements Cloneable, HasDateRange {

    /** ic_parent_i_id unique ID(9) */
    private Integer icParentIId;

    /** ic_i_id unique ID(9) */
    private Integer icIId;

    /** ic_effective_date date(8) */
    private LocalDate icEffectiveDate;

    /** ic_expired_date date(8) */
    private LocalDate icExpiredDate;

    /** ic_material_unit variable text(5) */
    private String icMaterialUnit;

    /** ic_material_quantity unsigned numeric(15, 4) */
    private BigDecimal icMaterialQuantity;

    /** ic_loss_ratio unsigned numeric(5, 2) */
    private BigDecimal icLossRatio;

    public void setIcParentIId(Integer value) {
        this.icParentIId = value;
    }

    public Integer getIcParentIId() {
        return this.icParentIId;
    }

    public void setIcIId(Integer value) {
        this.icIId = value;
    }

    public Integer getIcIId() {
        return this.icIId;
    }

    public void setIcEffectiveDate(LocalDate value) {
        this.icEffectiveDate = value;
    }

    public LocalDate getIcEffectiveDate() {
        return this.icEffectiveDate;
    }

    public void setIcExpiredDate(LocalDate value) {
        this.icExpiredDate = value;
    }

    public LocalDate getIcExpiredDate() {
        return this.icExpiredDate;
    }

    public void setIcMaterialUnit(String value) {
        this.icMaterialUnit = value;
    }

    public String getIcMaterialUnit() {
        return this.icMaterialUnit;
    }

    public void setIcMaterialQuantity(BigDecimal value) {
        this.icMaterialQuantity = value;
    }

    public BigDecimal getIcMaterialQuantity() {
        return this.icMaterialQuantity;
    }

    public void setIcLossRatio(BigDecimal value) {
        this.icLossRatio = value;
    }

    public BigDecimal getIcLossRatio() {
        return this.icLossRatio;
    }

    @Override
    public ItemConstructionMaster clone() {
        try {
            return (ItemConstructionMaster) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new InternalError(e);
        }
    }

    @Override
    public LocalDate getEffectiveDate() {
        return getIcEffectiveDate();
    }

    @Override
    public void setEffectiveDate(LocalDate value) {
        setIcEffectiveDate(value);
    }

    @Override
    public LocalDate getExpiredDate() {
        return getIcExpiredDate();
    }

    @Override
    public void setExpiredDate(LocalDate value) {
        setIcExpiredDate(value);
    }

    @Override
    public String toString() {
        return "ItemConstructionMaster(ic_parent_i_id=" + icParentIId + ", ic_i_id=" + icIId + ", ic_effective_date=" + icEffectiveDate + ", ic_expired_date=" + icExpiredDate + ", ic_material_unit=" + icMaterialUnit + ", ic_material_quantity=" + icMaterialQuantity + ", ic_loss_ratio=" + icLossRatio + ")";
    }
}
