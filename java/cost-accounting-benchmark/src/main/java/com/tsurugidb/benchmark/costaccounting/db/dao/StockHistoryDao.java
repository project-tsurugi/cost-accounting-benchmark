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
package com.tsurugidb.benchmark.costaccounting.db.dao;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import com.tsurugidb.benchmark.costaccounting.db.entity.StockHistory;
import com.tsurugidb.benchmark.costaccounting.db.entity.StockHistoryDateTime;

/**
 * 在庫履歴DAO
 */
public interface StockHistoryDao {

    public static final String TABLE_NAME = "stock_history";

    /**
     * <pre>
     * truncate table stock_history
     * </pre>
     */
    void truncate();

    /**
     * <pre>
     * delete from stock_history
     * </pre>
     */
    int deleteAll();

    /**
     * <pre>
     * insert into stock_history
     * values(:entity)
     * </pre>
     */
    int insert(StockHistory entity);

    int[] insertBatch(Collection<StockHistory> entityList);

    /**
     * <pre>
     * select s_date, s_time from stock_history
     * group by s_date, s_time
     * order by s_date, s_time
     * </pre>
     */
    List<StockHistoryDateTime> selectGroupByDateTime();

    /**
     * <pre>
     * select distinct s_date, s_time from stock_history
     * order by s_date, s_time
     * </pre>
     */
    List<StockHistoryDateTime> selectDistinctDateTime();

    /**
     * <pre>
     * delete from stock_history
     * where s_date = :date and s_time = :time
     * </pre>
     */
    int deleteByDateTime(LocalDate date, LocalTime time);

    /**
     * <pre>
     * delete from stock_history
     * where s_date = :date and s_time = :time and s_f_id = :factoryId
     * </pre>
     */
    int deleteByDateTime(LocalDate date, LocalTime time, int factoryId);

    /**
     * <pre>
     * delete from stock_history
     * where (s_date < :date) or (s_date = :date and s_time <= :time)
     * </pre>
     */
    int deleteOldDateTime(LocalDate date, LocalTime time);

    /**
     * <pre>
     * delete from stock_history
     * where ((s_date < :date) or (s_date = :date and s_time <= :time) and s_f_id = :factoryId
     * </pre>
     */
    int deleteOldDateTime(LocalDate date, LocalTime time, int factoryId);

    /**
     * <pre>
     * insert into stock_history
     * select ... from cost_master
     * </pre>
     */
    void insertSelectFromCostMaster(LocalDate date, LocalTime time);

    /**
     * <pre>
     * insert into stock_history
     * select ... from cost_master where c_f_id = :factoryId
     * </pre>
     */
    void insertSelectFromCostMaster(LocalDate date, LocalTime time, int factoryId);

    void forEach(Consumer<StockHistory> entityConsumer);
}
