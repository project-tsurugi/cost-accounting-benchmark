package com.tsurugidb.benchmark.costaccounting.db.doma2.entity;

import java.time.LocalDate;
import java.util.Objects;

import org.seasar.doma.Column;
import org.seasar.doma.Entity;
import org.seasar.doma.Id;
import org.seasar.doma.Table;

@Entity
@Table(name = "item_construction_master")
public class ItemConstructionMasterKey {

    /** unique ID(9) */
    @Column(name = "ic_parent_i_id")
    @Id
    Integer icParentIId;

    /** unique ID(9) */
    @Column(name = "ic_i_id")
    @Id
    Integer icIId;

    /** date(8) */
    @Column(name = "ic_effective_date")
    @Id
    LocalDate icEffectiveDate;

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
