package com.tsurugidb.benchmark.costaccounting.db.entity;

import java.math.BigDecimal;

/**
 * cost_master
 */
public class CostMaster implements Cloneable {

    /** c_f_id unique ID(4) */
    private Integer cFId;

    /** c_i_id unique ID(9) */
    private Integer cIId;

    /** c_stock_unit variable text(5) */
    private String cStockUnit;

    /** c_stock_quantity unsigned numeric(15, 4) */
    private BigDecimal cStockQuantity;

    /** c_stock_amount unsigned numeric(13, 2) */
    private BigDecimal cStockAmount;

    public void setCFId(Integer value) {
        this.cFId = value;
    }

    public Integer getCFId() {
        return this.cFId;
    }

    public void setCIId(Integer value) {
        this.cIId = value;
    }

    public Integer getCIId() {
        return this.cIId;
    }

    public void setCStockUnit(String value) {
        this.cStockUnit = value;
    }

    public String getCStockUnit() {
        return this.cStockUnit;
    }

    public void setCStockQuantity(BigDecimal value) {
        this.cStockQuantity = value;
    }

    public BigDecimal getCStockQuantity() {
        return this.cStockQuantity;
    }

    public void setCStockAmount(BigDecimal value) {
        this.cStockAmount = value;
    }

    public BigDecimal getCStockAmount() {
        return this.cStockAmount;
    }

    @Override
    public CostMaster clone() {
        try {
            return (CostMaster) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new InternalError(e);
        }
    }

    @Override
    public String toString() {
        return "CostMaster(c_f_id=" + cFId + ", c_i_id=" + cIId + ", c_stock_unit=" + cStockUnit + ", c_stock_quantity=" + cStockQuantity + ", c_stock_amount=" + cStockAmount + ")";
    }
}
