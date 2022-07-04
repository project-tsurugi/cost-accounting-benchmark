package com.tsurugidb.benchmark.costaccounting.db.doma2.entity;

import java.math.BigDecimal;
import java.time.LocalDate;
import org.seasar.doma.Column;
import org.seasar.doma.Entity;
import org.seasar.doma.Id;
import org.seasar.doma.Table;

import com.tsurugidb.benchmark.costaccounting.db.doma2.domain.ItemType;

@Entity
@Table(name = "item_master")
public class ItemMaster implements Cloneable, HasDateRange {

    /** unique ID(9) */
    @Column(name = "i_id")
    @Id
    Integer iId;

    /** date(8) */
    @Column(name = "i_effective_date")
    @Id
    LocalDate iEffectiveDate;

    /** date(8) */
    @Column(name = "i_expired_date")
    LocalDate iExpiredDate;

    /** variable text(20) */
    @Column(name = "i_name")
    String iName;

    /** variable text(20) */
    @Column(name = "i_type")
    ItemType iType;

    /** variable text(5) */
    @Column(name = "i_unit")
    String iUnit;

    /** unsigned numeric(9, 2) */
    @Column(name = "i_weight_ratio")
    BigDecimal iWeightRatio;

    /** variable text(5) */
    @Column(name = "i_weight_unit")
    String iWeightUnit;

    /** unsigned numeric(11, 2) */
    @Column(name = "i_price")
    BigDecimal iPrice;

    /** variable text(5) */
    @Column(name = "i_price_unit")
    String iPriceUnit;

    public void setIId(Integer value) {
        this.iId = value;
    }

    public Integer getIId() {
        return this.iId;
    }

    public void setIEffectiveDate(LocalDate value) {
        this.iEffectiveDate = value;
    }

    public LocalDate getIEffectiveDate() {
        return this.iEffectiveDate;
    }

    public void setIExpiredDate(LocalDate value) {
        this.iExpiredDate = value;
    }

    public LocalDate getIExpiredDate() {
        return this.iExpiredDate;
    }

    public void setIName(String value) {
        this.iName = value;
    }

    public String getIName() {
        return this.iName;
    }

    public void setIType(ItemType value) {
        this.iType = value;
    }

    public ItemType getIType() {
        return this.iType;
    }

    public void setIUnit(String value) {
        this.iUnit = value;
    }

    public String getIUnit() {
        return this.iUnit;
    }

    public void setIWeightRatio(BigDecimal value) {
        this.iWeightRatio = value;
    }

    public BigDecimal getIWeightRatio() {
        return this.iWeightRatio;
    }

    public void setIWeightUnit(String value) {
        this.iWeightUnit = value;
    }

    public String getIWeightUnit() {
        return this.iWeightUnit;
    }

    public void setIPrice(BigDecimal value) {
        this.iPrice = value;
    }

    public BigDecimal getIPrice() {
        return this.iPrice;
    }

    public void setIPriceUnit(String value) {
        this.iPriceUnit = value;
    }

    public String getIPriceUnit() {
        return this.iPriceUnit;
    }

    @Override
    public ItemMaster clone() {
        try {
            return (ItemMaster) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new InternalError(e);
        }
    }

    @Override
    public LocalDate getEffectiveDate() {
        return getIEffectiveDate();
    }

    @Override
    public void setEffectiveDate(LocalDate value) {
        setIEffectiveDate(value);
    }

    @Override
    public LocalDate getExpiredDate() {
        return getIExpiredDate();
    }

    @Override
    public void setExpiredDate(LocalDate value) {
        setIExpiredDate(value);
    }

    @Override
    public String toString() {
        return "ItemMaster(i_id=" + iId + ", i_effective_date=" + iEffectiveDate + ", i_expired_date=" + iExpiredDate + ", i_name=" + iName + ", i_type=" + iType + ", i_unit=" + iUnit
                + ", i_weight_ratio=" + iWeightRatio + ", i_weight_unit=" + iWeightUnit + ", i_price=" + iPrice + ", i_price_unit=" + iPriceUnit + ")";
    }
}
