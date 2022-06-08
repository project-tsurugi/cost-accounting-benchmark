package com.example.nedo.db.doma2.entity;

import java.math.BigDecimal;
import org.seasar.doma.Column;
import org.seasar.doma.Entity;
import org.seasar.doma.Id;
import org.seasar.doma.Table;

@Entity
@Table(name = "cost_master")
public class CostMaster implements Cloneable {

    /** unique ID(4) */
    @Column(name = "c_f_id")
    @Id
    Integer cFId;

    /** unique ID(9) */
    @Column(name = "c_i_id")
    @Id
    Integer cIId;

    /** variable text(5) */
    @Column(name = "c_stock_unit")
    String cStockUnit;

    /** unsigned numeric(15, 4) */
    @Column(name = "c_stock_quantity")
    BigDecimal cStockQuantity;

    /** unsigned numeric(13, 2) */
    @Column(name = "c_stock_amount")
    BigDecimal cStockAmount;

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
