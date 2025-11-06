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
 * factory_master
 */
public class FactoryMaster implements Cloneable {

    /** f_id unique ID(4) */
    private Integer fId;

    /** f_name variable text(20) */
    private String fName;

    public void setFId(Integer value) {
        this.fId = value;
    }

    public Integer getFId() {
        return this.fId;
    }

    public void setFName(String value) {
        this.fName = value;
    }

    public String getFName() {
        return this.fName;
    }

    @Override
    public FactoryMaster clone() {
        try {
            return (FactoryMaster) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new InternalError(e);
        }
    }

    public String toCsv(String suffix) {
        return fId + "," + fName + suffix;
    }

    @Override
    public String toString() {
        return "FactoryMaster(f_id=" + fId + ", f_name=" + fName + ")";
    }
}
