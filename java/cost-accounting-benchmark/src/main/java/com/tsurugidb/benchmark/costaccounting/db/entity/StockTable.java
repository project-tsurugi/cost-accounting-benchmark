package com.tsurugidb.benchmark.costaccounting.db.entity;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * stock_table
 */
public class StockTable implements Cloneable {

    /** s_stock_amount unsigned numeric(13, 2) */
    public static final int S_STOCK_AMOUNT_SCALE = 2;

    /** s_date date(8) */
    private LocalDate sDate;

    /** s_f_id unique ID(4) */
    private Integer sFId;

    /** s_stock_amount unsigned numeric(13, 2) */
    private BigDecimal sStockAmount;

    public void setSDate(LocalDate value) {
        this.sDate = value;
    }

    public LocalDate getSDate() {
        return this.sDate;
    }

    public void setSFId(Integer value) {
        this.sFId = value;
    }

    public Integer getSFId() {
        return this.sFId;
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
        return sDate + "," + sFId + "," + sStockAmount + suffix;
    }

    @Override
    public String toString() {
        return "StockTable(s_date=" + sDate + ", s_f_id=" + sFId + ", s_stock_amount=" + sStockAmount + ")";
    }
}
