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
import java.time.LocalDate;
import java.time.LocalTime;

/**
 * stock_history
 */
public class StockHistory implements Cloneable {

    /** s_stock_quantity unsigned numeric(15, 4) */
    public static final int S_STOCK_QUANTITY_SCALE = 4;

    /** s_stock_amount unsigned numeric(13, 2) */
    public static final int S_STOCK_AMOUNT_SCALE = 2;

    /** s_date date(8) */
    private LocalDate sDate;

    /** s_time time(6) */
    private LocalTime sTime;

    /** s_f_id unique ID(4) */
    private Integer sFId;

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

    public void setSTime(LocalTime value) {
        this.sTime = value;
    }

    public LocalTime getSTime() {
        return this.sTime;
    }

    public void setSFId(Integer value) {
        this.sFId = value;
    }

    public Integer getSFId() {
        return this.sFId;
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
    public StockHistory clone() {
        try {
            return (StockHistory) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new InternalError(e);
        }
    }

    public String toCsv(String suffix) {
        return sDate + "," + sTime + "," + sFId + "," + sIId + "," + sStockUnit + "," + sStockQuantity + "," + sStockAmount + suffix;
    }

    @Override
    public String toString() {
        return "StockHistory(s_date=" + sDate + ", s_time=" + sTime + ", s_f_id=" + sFId + ", s_i_id=" + sIId + ", s_stock_unit=" + sStockUnit + ", s_stock_quantity=" + sStockQuantity + ", s_stock_amount=" + sStockAmount + ")";
    }
}
