/*
 * Copyright 2023-2025 Project Tsurugi.
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
