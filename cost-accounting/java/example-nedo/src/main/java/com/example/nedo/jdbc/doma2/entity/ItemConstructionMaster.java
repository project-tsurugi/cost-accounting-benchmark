package com.example.nedo.jdbc.doma2.entity;

import java.math.BigDecimal;
import java.time.LocalDate;
import org.seasar.doma.Column;
import org.seasar.doma.Entity;
import org.seasar.doma.Id;
import org.seasar.doma.Table;

@Entity
@Table(name = "item_construction_master")
public class ItemConstructionMaster implements Cloneable, HasDateRange {

    /** unique ID(9) */
    @Column(name = "ic_parent_i_id")
    @Id
    Integer icParentIId;

    /** unique ID(9) */
    @Column(name = "ic_i_id")
    @Id
    Integer icIId;

    /** date(8) */
    @Column(name = "ic_effective_date")
    @Id
    LocalDate icEffectiveDate;

    /** date(8) */
    @Column(name = "ic_expired_date")
    LocalDate icExpiredDate;

    /** variable text(5) */
    @Column(name = "ic_material_unit")
    String icMaterialUnit;

    /** unsigned numeric(15, 4) */
    @Column(name = "ic_material_quantity")
    BigDecimal icMaterialQuantity;

    /** unsigned numeric(5, 2) */
    @Column(name = "ic_loss_ratio")
    BigDecimal icLossRatio;

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
