package com.tsurugidb.benchmark.costaccounting.db.entity;

/**
 * ids for item_construction_master
 */
public class ItemConstructionMasterIds {

    /** ic_parent_i_id unique ID(9) */
    private Integer icParentIId;

    /** ic_i_id unique ID(9) */
    private Integer icIId;

    public void setIcParentIId(Integer value) {
        this.icParentIId = value;
    }

    public Integer getIcParentIId() {
        return this.icParentIId;
    }

    public void setIcIId(Integer value) {
        this.icIId = value;
    }

    public Integer getIcIId() {
        return this.icIId;
    }

    @Override
    public String toString() {
        return "ItemConstructionMasterIds(ic_parent_i_id=" + icParentIId + ", ic_i_id=" + icIId + ")";
    }
}
