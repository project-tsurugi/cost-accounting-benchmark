/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.benchmark.costaccounting.db.entity;

import java.math.BigDecimal;

/**
 * cost_master
 */
public class CostMaster implements Cloneable {

    /** c_stock_quantity unsigned numeric(15, 4) */
    public static final int C_STOCK_QUANTITY_SCALE = 4;

    /** c_stock_amount unsigned numeric(13, 2) */
    public static final int C_STOCK_AMOUNT_SCALE = 2;

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

    public String toCsv(String suffix) {
        return cFId + "," + cIId + "," + cStockUnit + "," + cStockQuantity + "," + cStockAmount + suffix;
    }

    @Override
    public String toString() {
        return "CostMaster(c_f_id=" + cFId + ", c_i_id=" + cIId + ", c_stock_unit=" + cStockUnit + ", c_stock_quantity=" + cStockQuantity + ", c_stock_amount=" + cStockAmount + ")";
    }
}
