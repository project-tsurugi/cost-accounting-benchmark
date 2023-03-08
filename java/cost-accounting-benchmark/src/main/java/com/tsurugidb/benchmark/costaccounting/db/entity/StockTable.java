package com.tsurugidb.benchmark.costaccounting.db.entity;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * stock_table
 */
public class StockTable implements Cloneable {

    /** s_stock_quantity unsigned numeric(15, 4) */
    public static final int S_STOCK_QUANTITY_SCALE = 4;

    /** s_stock_amount unsigned numeric(13, 2) */
    public static final int S_STOCK_AMOUNT_SCALE = 2;

    /** s_date date(8) */
    private LocalDate sDate;

    /** s_i_id unique ID(9) */
    private Integer sIId;

    /** s_stock_unit variable text(5) */
    private String sStockUnit;

    /** s_stock_quantity unsigned numeric(15, 4) */
    private BigDecimal sStockQuantity;

    /** s_stock_amount unsigned numeric(13, 2) */
    private BigDecimal sStockAmount;

    public void setSDate(LocalDate value) {
        this.sDate = value;
    }

    public LocalDate getSDate() {
        return this.sDate;
    }

    public void setSIId(Integer value) {
        this.sIId = value;
    }

    public Integer getSIId() {
        return this.sIId;
    }

    public void setSStockUnit(String value) {
        this.sStockUnit = value;
    }

    public String getSStockUnit() {
        return this.sStockUnit;
    }

    public void setSStockQuantity(BigDecimal value) {
        this.sStockQuantity = value;
    }

    public BigDecimal getSStockQuantity() {
        return this.sStockQuantity;
    }

    public void setSStockAmount(BigDecimal value) {
        this.sStockAmount = value;
    }

    public BigDecimal getSStockAmount() {
        return this.sStockAmount;
    }

    @Override
    public StockTable clone() {
        try {
            return (StockTable) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new InternalError(e);
        }
    }

    public String toCsv(String suffix) {
        return sDate + "," + sIId + "," + sStockUnit + "," + sStockQuantity + "," + sStockAmount + suffix;
    }

    @Override
    public String toString() {
        return "StockTable(s_date=" + sDate + ", s_i_id=" + sIId + ", s_stock_unit=" + sStockUnit + ", s_stock_quantity=" + sStockQuantity + ", s_stock_amount=" + sStockAmount + ")";
    }
}
