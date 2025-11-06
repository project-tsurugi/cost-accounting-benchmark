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
package com.tsurugidb.benchmark.costaccounting.db.dao;

import java.util.List;
import java.util.function.Consumer;

import com.tsurugidb.benchmark.costaccounting.db.entity.MeasurementMaster;

/**
 * 度量衡マスターDAO
 */
public interface MeasurementMasterDao {

    public static final String TABLE_NAME = "measurement_master";

    /**
     * <pre>
     * truncate table measurement_master
     * </pre>
     */
    void truncate();

    /**
     * <pre>
     * delete from measurement_master
     * </pre>
     */
    int deleteAll();

    /**
     * <pre>
     * insert into measurement_master
     * values(:entity)
     * </pre>
     */
    int insert(MeasurementMaster entity);

    /**
     * <pre>
     * select * from measurement_master
     * </pre>
     */
    List<MeasurementMaster> selectAll();

    void forEach(Consumer<MeasurementMaster> entityConsumer);
}
