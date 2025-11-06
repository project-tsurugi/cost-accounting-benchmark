/*
 * Copyright 2023-2025 Project Tsurugi.
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

import java.math.BigInteger;
import java.time.LocalDate;

/**
 * item_manufacturing_master
 */
public class ItemManufacturingMaster implements Cloneable, HasDateRange {

    /** im_f_id unique ID(4) */
    private Integer imFId;

    /** im_i_id unique ID(9) */
    private Integer imIId;

    /** im_effective_date date(8) */
    private LocalDate imEffectiveDate;

    /** im_expired_date date(8) */
    private LocalDate imExpiredDate;

    /** im_manufacturing_quantity unsigned numeric(6) */
    private BigInteger imManufacturingQuantity;

    public void setImFId(Integer value) {
        this.imFId = value;
    }

    public Integer getImFId() {
        return this.imFId;
    }

    public void setImIId(Integer value) {
        this.imIId = value;
    }

    public Integer getImIId() {
        return this.imIId;
    }

    public void setImEffectiveDate(LocalDate value) {
        this.imEffectiveDate = value;
    }

    public LocalDate getImEffectiveDate() {
        return this.imEffectiveDate;
    }

    public void setImExpiredDate(LocalDate value) {
        this.imExpiredDate = value;
    }

    public LocalDate getImExpiredDate() {
        return this.imExpiredDate;
    }

    public void setImManufacturingQuantity(BigInteger value) {
        this.imManufacturingQuantity = value;
    }

    public BigInteger getImManufacturingQuantity() {
        return this.imManufacturingQuantity;
    }

    @Override
    public ItemManufacturingMaster clone() {
        try {
            return (ItemManufacturingMaster) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new InternalError(e);
        }
    }

    @Override
    public LocalDate getEffectiveDate() {
        return getImEffectiveDate();
    }

    @Override
    public void setEffectiveDate(LocalDate value) {
        setImEffectiveDate(value);
    }

    @Override
    public LocalDate getExpiredDate() {
        return getImExpiredDate();
    }

    @Override
    public void setExpiredDate(LocalDate value) {
        setImExpiredDate(value);
    }

    public String toCsv(String suffix) {
        return imFId + "," + imIId + "," + imEffectiveDate + "," + imExpiredDate + "," + imManufacturingQuantity + suffix;
    }

    @Override
    public String toString() {
        return "ItemManufacturingMaster(im_f_id=" + imFId + ", im_i_id=" + imIId + ", im_effective_date=" + imEffectiveDate + ", im_expired_date=" + imExpiredDate + ", im_manufacturing_quantity=" + imManufacturingQuantity + ")";
    }
}
