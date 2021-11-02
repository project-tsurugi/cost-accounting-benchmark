package com.example.nedo.jdbc.doma2.entity;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import org.seasar.doma.Column;
import org.seasar.doma.Entity;
import org.seasar.doma.Id;
import org.seasar.doma.Table;

@Entity
@Table(name = "result_table")
public class ResultTable implements Cloneable {

    /** unique ID(4) */
    @Column(name = "r_f_id")
    @Id
    Integer rFId;

    /** date(8) */
    @Column(name = "r_manufacturing_date")
    @Id
    LocalDate rManufacturingDate;

    /** unique ID(9) */
    @Column(name = "r_product_i_id")
    @Id
    Integer rProductIId;

    /** unique ID(9) */
    @Column(name = "r_parent_i_id")
    @Id
    Integer rParentIId;

    /** unique ID(9) */
    @Column(name = "r_i_id")
    @Id
    Integer rIId;

    /** unsigned numeric(6) */
    @Column(name = "r_manufacturing_quantity")
    BigInteger rManufacturingQuantity;

    /** variable text(5) */
    @Column(name = "r_weight_unit")
    String rWeightUnit;

    /** unsigned numeric(9, 2) */
    @Column(name = "r_weight")
    BigDecimal rWeight;

    /** variable text(5) */
    @Column(name = "r_weight_total_unit")
    String rWeightTotalUnit;

    /** unsigned numeric(11, 2) */
    @Column(name = "r_weight_total")
    BigDecimal rWeightTotal;

    /** unsigned numeric(7, 3) */
    @Column(name = "r_weight_ratio")
    BigDecimal rWeightRatio;

    /** variable text(5) */
    @Column(name = "r_standard_quantity_unit")
    String rStandardQuantityUnit;

    /** unsigned numeric(15, 4) */
    @Column(name = "r_standard_quantity")
    BigDecimal rStandardQuantity;

    /** variable text(5) */
    @Column(name = "r_required_quantity_unit")
    String rRequiredQuantityUnit;

    /** unsigned numeric(15, 4) */
    @Column(name = "r_required_quantity")
    BigDecimal rRequiredQuantity;

    /** unsigned numeric(11, 2) */
    @Column(name = "r_unit_cost")
    BigDecimal rUnitCost;

    /** unsigned numeric(11, 2) */
    @Column(name = "r_total_unit_cost")
    BigDecimal rTotalUnitCost;

    /** unsigned numeric(11, 2) */
    @Column(name = "r_manufacturing_cost")
    BigDecimal rManufacturingCost;

    /** unsigned numeric(11, 2) */
    @Column(name = "r_total_manufacturing_cost")
    BigDecimal rTotalManufacturingCost;

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

    @Override
    public String toString() {
        return "ResultTable(r_f_id=" + rFId + ", r_manufacturing_date=" + rManufacturingDate + ", r_product_i_id=" + rProductIId + ", r_parent_i_id=" + rParentIId + ", r_i_id=" + rIId + ", r_manufacturing_quantity=" + rManufacturingQuantity + ", r_weight_unit=" + rWeightUnit + ", r_weight=" + rWeight + ", r_weight_total_unit=" + rWeightTotalUnit + ", r_weight_total=" + rWeightTotal + ", r_weight_ratio=" + rWeightRatio + ", r_standard_quantity_unit=" + rStandardQuantityUnit + ", r_standard_quantity=" + rStandardQuantity + ", r_required_quantity_unit=" + rRequiredQuantityUnit + ", r_required_quantity=" + rRequiredQuantity + ", r_unit_cost=" + rUnitCost + ", r_total_unit_cost=" + rTotalUnitCost + ", r_manufacturing_cost=" + rManufacturingCost + ", r_total_manufacturing_cost=" + rTotalManufacturingCost + ")";
    }
}
