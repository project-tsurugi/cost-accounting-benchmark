package com.tsurugidb.benchmark.costaccounting.db.entity;

import com.tsurugidb.benchmark.costaccounting.db.domain.ItemType;
import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * item_master
 */
public class ItemMaster implements Cloneable, HasDateRange {

    /** i_weight_ratio unsigned numeric(9, 2) */
    public static final int I_WEIGHT_RATIO_SCALE = 2;

    /** i_price unsigned numeric(11, 2) */
    public static final int I_PRICE_SCALE = 2;

    /** i_id unique ID(9) */
    private Integer iId;

    /** i_effective_date date(8) */
    private LocalDate iEffectiveDate;

    /** i_expired_date date(8) */
    private LocalDate iExpiredDate;

    /** i_name variable text(20) */
    private String iName;

    /** i_type variable text(20) */
    private ItemType iType;

    /** i_unit variable text(5) */
    private String iUnit;

    /** i_weight_ratio unsigned numeric(9, 2) */
    private BigDecimal iWeightRatio;

    /** i_weight_unit variable text(5) */
    private String iWeightUnit;

    /** i_price unsigned numeric(11, 2) */
    private BigDecimal iPrice;

    /** i_price_unit variable text(5) */
    private String iPriceUnit;

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

    public String toCsv(String suffix) {
        return iId + "," + iEffectiveDate + "," + iExpiredDate + "," + iName + "," + iType + "," + iUnit + "," + iWeightRatio + "," + iWeightUnit + "," + iPrice + "," + iPriceUnit + suffix;
    }

    @Override
    public String toString() {
        return "ItemMaster(i_id=" + iId + ", i_effective_date=" + iEffectiveDate + ", i_expired_date=" + iExpiredDate + ", i_name=" + iName + ", i_type=" + iType + ", i_unit=" + iUnit + ", i_weight_ratio=" + iWeightRatio + ", i_weight_unit=" + iWeightUnit + ", i_price=" + iPrice + ", i_price_unit=" + iPriceUnit + ")";
    }
}
