package com.tsurugidb.benchmark.costaccounting.db.doma2.entity;

import java.math.BigInteger;
import java.time.LocalDate;
import org.seasar.doma.Column;
import org.seasar.doma.Entity;
import org.seasar.doma.Id;
import org.seasar.doma.Table;

@Entity
@Table(name = "item_manufacturing_master")
public class ItemManufacturingMaster implements Cloneable, HasDateRange {

    /** unique ID(4) */
    @Column(name = "im_f_id")
    @Id
    Integer imFId;

    /** unique ID(9) */
    @Column(name = "im_i_id")
    @Id
    Integer imIId;

    /** date(8) */
    @Column(name = "im_effective_date")
    @Id
    LocalDate imEffectiveDate;

    /** date(8) */
    @Column(name = "im_expired_date")
    LocalDate imExpiredDate;

    /** unsigned numeric(6) */
    @Column(name = "im_manufacturing_quantity")
    BigInteger imManufacturingQuantity;

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

    @Override
    public String toString() {
        return "ItemManufacturingMaster(im_f_id=" + imFId + ", im_i_id=" + imIId + ", im_effective_date=" + imEffectiveDate + ", im_expired_date=" + imExpiredDate + ", im_manufacturing_quantity="
                + imManufacturingQuantity + ")";
    }
}
