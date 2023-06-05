package com.tsurugidb.benchmark.costaccounting.db.entity;

/**
 * count for item_manufacturing_master
 */
public class ItemManufacturingMasterCount {

    /** im_f_id unique ID(4) */
    private int imFId;

    private int count;

    public void setImFId(int value) {
        this.imFId = value;
    }

    public int getImFId() {
        return this.imFId;
    }

    public void setCount(int value) {
        this.count = value;
    }

    public int getCount() {
        return this.count;
    }

    @Override
    public String toString() {
        return "ItemManufacturingMasterCount(im_f_id=" + imFId + ", count=" + count + ")";
    }
}
