package com.example.nedo.jdbc.doma2.entity;

import org.seasar.doma.Column;
import org.seasar.doma.Entity;
import org.seasar.doma.Id;
import org.seasar.doma.Table;

@Entity
@Table(name = "item_construction_master")
public class ItemConstructionMasterIds {

	/** unique ID(9) */
	@Column(name = "ic_parent_i_id")
	@Id
	Integer icParentIId;

	/** unique ID(9) */
	@Column(name = "ic_i_id")
	@Id
	Integer icIId;

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
