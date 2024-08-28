/*
 * Copyright 2023-2024 Project Tsurugi.
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
