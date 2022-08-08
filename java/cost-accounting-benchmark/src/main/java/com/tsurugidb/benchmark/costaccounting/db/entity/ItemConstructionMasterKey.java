package com.tsurugidb.benchmark.costaccounting.db.entity;

import java.time.LocalDate;
import java.util.Objects;

/**
 * primary key for item_construction_master
 */
public class ItemConstructionMasterKey {

    /** ic_parent_i_id unique ID(9) */
    private Integer icParentIId;

    /** ic_i_id unique ID(9) */
    private Integer icIId;

    /** ic_effective_date date(8) */
    private LocalDate icEffectiveDate;

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

    public void setIcEffectiveDate(LocalDate value) {
        this.icEffectiveDate = value;
    }

    public LocalDate getIcEffectiveDate() {
        return this.icEffectiveDate;
    }

    @Override
    public int hashCode() {
        return Objects.hash(icParentIId, icIId, icEffectiveDate);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof ItemConstructionMasterKey)) {
            return false;
        }

        ItemConstructionMasterKey other = (ItemConstructionMasterKey) obj;
        return Objects.equals(icParentIId, other.icParentIId) && Objects.equals(icIId, other.icIId) && Objects.equals(icEffectiveDate, other.icEffectiveDate);
    }

    @Override
    public String toString() {
        return "ItemConstructionMaster.Key(ic_parent_i_id=" + icParentIId + ", ic_i_id=" + icIId + ", ic_effective_date=" + icEffectiveDate + ")";
    }
}
