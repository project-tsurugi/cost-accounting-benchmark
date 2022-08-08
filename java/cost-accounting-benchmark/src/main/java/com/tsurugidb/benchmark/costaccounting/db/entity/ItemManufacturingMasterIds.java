package com.tsurugidb.benchmark.costaccounting.db.entity;

/**
 * ids for item_manufacturing_master
 */
public class ItemManufacturingMasterIds {

    /** im_f_id unique ID(4) */
    private Integer imFId;

    /** im_i_id unique ID(9) */
    private Integer imIId;

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
