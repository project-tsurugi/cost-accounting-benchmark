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
import java.math.BigInteger;
import java.time.LocalDate;

/**
 * result_table
 */
public class ResultTable implements Cloneable {

    /** r_weight unsigned numeric(9, 2) */
    public static final int R_WEIGHT_SCALE = 2;

    /** r_weight_total unsigned numeric(12, 2) */
    public static final int R_WEIGHT_TOTAL_SCALE = 2;

    /** r_weight_ratio unsigned numeric(7, 3) */
    public static final int R_WEIGHT_RATIO_SCALE = 3;

    /** r_standard_quantity unsigned numeric(15, 4) */
    public static final int R_STANDARD_QUANTITY_SCALE = 4;

    /** r_required_quantity unsigned numeric(15, 4) */
    public static final int R_REQUIRED_QUANTITY_SCALE = 4;

    /** r_unit_cost unsigned numeric(11, 2) */
    public static final int R_UNIT_COST_SCALE = 2;

    /** r_total_unit_cost unsigned numeric(13, 2) */
    public static final int R_TOTAL_UNIT_COST_SCALE = 2;

    /** r_manufacturing_cost unsigned numeric(11, 2) */
    public static final int R_MANUFACTURING_COST_SCALE = 2;

    /** r_total_manufacturing_cost unsigned numeric(13, 2) */
    public static final int R_TOTAL_MANUFACTURING_COST_SCALE = 2;

    /** r_f_id unique ID(4) */
    private Integer rFId;

    /** r_manufacturing_date date(8) */
    private LocalDate rManufacturingDate;

    /** r_product_i_id unique ID(9) */
    private Integer rProductIId;

    /** r_parent_i_id unique ID(9) */
    private Integer rParentIId;

    /** r_i_id unique ID(9) */
    private Integer rIId;

    /** r_manufacturing_quantity unsigned numeric(6) */
    private BigInteger rManufacturingQuantity;

    /** r_weight_unit variable text(5) */
    private String rWeightUnit;

    /** r_weight unsigned numeric(9, 2) */
    private BigDecimal rWeight;

    /** r_weight_total_unit variable text(5) */
    private String rWeightTotalUnit;

    /** r_weight_total unsigned numeric(12, 2) */
    private BigDecimal rWeightTotal;

    /** r_weight_ratio unsigned numeric(7, 3) */
    private BigDecimal rWeightRatio;

    /** r_standard_quantity_unit variable text(5) */
    private String rStandardQuantityUnit;

    /** r_standard_quantity unsigned numeric(15, 4) */
    private BigDecimal rStandardQuantity;

    /** r_required_quantity_unit variable text(5) */
    private String rRequiredQuantityUnit;

    /** r_required_quantity unsigned numeric(15, 4) */
    private BigDecimal rRequiredQuantity;

    /** r_unit_cost unsigned numeric(11, 2) */
    private BigDecimal rUnitCost;

    /** r_total_unit_cost unsigned numeric(13, 2) */
    private BigDecimal rTotalUnitCost;

    /** r_manufacturing_cost unsigned numeric(11, 2) */
    private BigDecimal rManufacturingCost;

    /** r_total_manufacturing_cost unsigned numeric(13, 2) */
    private BigDecimal rTotalManufacturingCost;

    public void setRFId(Integer value) {
        this.rFId = value;
    }

    public Integer getRFId() {
        return this.rFId;
    }

    public void setRManufacturingDate(LocalDate value) {
        this.rManufacturingDate = value;
    }

    public LocalDate getRManufacturingDate() {
        return this.rManufacturingDate;
    }

    public void setRProductIId(Integer value) {
        this.rProductIId = value;
    }

    public Integer getRProductIId() {
        return this.rProductIId;
    }

    public void setRParentIId(Integer value) {
        this.rParentIId = value;
    }

    public Integer getRParentIId() {
        return this.rParentIId;
    }

    public void setRIId(Integer value) {
        this.rIId = value;
    }

    public Integer getRIId() {
        return this.rIId;
    }

    public void setRManufacturingQuantity(BigInteger value) {
        this.rManufacturingQuantity = value;
    }

    public BigInteger getRManufacturingQuantity() {
        return this.rManufacturingQuantity;
    }

    public void setRWeightUnit(String value) {
        this.rWeightUnit = value;
    }

    public String getRWeightUnit() {
        return this.rWeightUnit;
    }

    public void setRWeight(BigDecimal value) {
        this.rWeight = value;
    }

    public BigDecimal getRWeight() {
        return this.rWeight;
    }

    public void setRWeightTotalUnit(String value) {
        this.rWeightTotalUnit = value;
    }

    public String getRWeightTotalUnit() {
        return this.rWeightTotalUnit;
    }

    public void setRWeightTotal(BigDecimal value) {
        this.rWeightTotal = value;
    }

    public BigDecimal getRWeightTotal() {
        return this.rWeightTotal;
    }

    public void setRWeightRatio(BigDecimal value) {
        this.rWeightRatio = value;
    }

    public BigDecimal getRWeightRatio() {
        return this.rWeightRatio;
    }

    public void setRStandardQuantityUnit(String value) {
        this.rStandardQuantityUnit = value;
    }

    public String getRStandardQuantityUnit() {
        return this.rStandardQuantityUnit;
    }

    public void setRStandardQuantity(BigDecimal value) {
        this.rStandardQuantity = value;
    }

    public BigDecimal getRStandardQuantity() {
        return this.rStandardQuantity;
    }

    public void setRRequiredQuantityUnit(String value) {
        this.rRequiredQuantityUnit = value;
    }

    public String getRRequiredQuantityUnit() {
        return this.rRequiredQuantityUnit;
    }

    public void setRRequiredQuantity(BigDecimal value) {
        this.rRequiredQuantity = value;
    }

    public BigDecimal getRRequiredQuantity() {
        return this.rRequiredQuantity;
    }

    public void setRUnitCost(BigDecimal value) {
        this.rUnitCost = value;
    }

    public BigDecimal getRUnitCost() {
        return this.rUnitCost;
    }

    public void setRTotalUnitCost(BigDecimal value) {
        this.rTotalUnitCost = value;
    }

    public BigDecimal getRTotalUnitCost() {
        return this.rTotalUnitCost;
    }

    public void setRManufacturingCost(BigDecimal value) {
        this.rManufacturingCost = value;
    }

    public BigDecimal getRManufacturingCost() {
        return this.rManufacturingCost;
    }

    public void setRTotalManufacturingCost(BigDecimal value) {
        this.rTotalManufacturingCost = value;
    }

    public BigDecimal getRTotalManufacturingCost() {
        return this.rTotalManufacturingCost;
    }

    @Override
    public ResultTable clone() {
        try {
            return (ResultTable) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new InternalError(e);
        }
    }

    public String toCsv(String suffix) {
        return rFId + "," + rManufacturingDate + "," + rProductIId + "," + rParentIId + "," + rIId + "," + rManufacturingQuantity + "," + rWeightUnit + "," + rWeight + "," + rWeightTotalUnit + "," + rWeightTotal + "," + rWeightRatio + "," + rStandardQuantityUnit + "," + rStandardQuantity + "," + rRequiredQuantityUnit + "," + rRequiredQuantity + "," + rUnitCost + "," + rTotalUnitCost + "," + rManufacturingCost + "," + rTotalManufacturingCost + suffix;
    }

    @Override
    public String toString() {
        return "ResultTable(r_f_id=" + rFId + ", r_manufacturing_date=" + rManufacturingDate + ", r_product_i_id=" + rProductIId + ", r_parent_i_id=" + rParentIId + ", r_i_id=" + rIId + ", r_manufacturing_quantity=" + rManufacturingQuantity + ", r_weight_unit=" + rWeightUnit + ", r_weight=" + rWeight + ", r_weight_total_unit=" + rWeightTotalUnit + ", r_weight_total=" + rWeightTotal + ", r_weight_ratio=" + rWeightRatio + ", r_standard_quantity_unit=" + rStandardQuantityUnit + ", r_standard_quantity=" + rStandardQuantity + ", r_required_quantity_unit=" + rRequiredQuantityUnit + ", r_required_quantity=" + rRequiredQuantity + ", r_unit_cost=" + rUnitCost + ", r_total_unit_cost=" + rTotalUnitCost + ", r_manufacturing_cost=" + rManufacturingCost + ", r_total_manufacturing_cost=" + rTotalManufacturingCost + ")";
    }
}
