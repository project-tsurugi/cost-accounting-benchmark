package com.example.nedo.jdbc.doma2.entity;

import org.seasar.doma.Column;
import org.seasar.doma.Entity;
import org.seasar.doma.Id;
import org.seasar.doma.Table;

@Entity
@Table(name = "item_manufacturing_master")
public class ItemManufacturingMasterIds {

    /** unique ID(4) */
    @Column(name = "im_f_id")
    @Id
    Integer imFId;

    /** unique ID(9) */
    @Column(name = "im_i_id")
    @Id
    Integer imIId;

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

    @Override
    public String toString() {
        return "ItemManufacturingMasterIds(im_f_id=" + imFId + ", im_i_id=" + imIId + ")";
    }
}
