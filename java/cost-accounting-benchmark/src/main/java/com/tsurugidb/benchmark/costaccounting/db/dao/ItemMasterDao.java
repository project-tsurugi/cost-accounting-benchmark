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
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.tsurugidb.benchmark.costaccounting.db.domain.ItemType;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemMaster;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao.SqlBetween;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain.BenchVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;

/**
 * 品目マスターDAO
 */
public interface ItemMasterDao {

    public static final String TABLE_NAME = "item_master";

    public static final String PS_COND_DATE = "? between i_effective_date and i_expired_date";

    public static final TgBindVariable<LocalDate> vDate = BenchVariable.ofDate("date");
    public static final String TG_COND_DATE = new SqlBetween(vDate, "i_effective_date", "i_expired_date").toString();

    /**
     * <pre>
     * truncate table item_master
     * </pre>
     */
    void truncate();

    /**
     * <pre>
     * delete from item_master
     * </pre>
     */
    int deleteAll();

    /**
     * <pre>
     * insert (or replace) into item_master
     * values(:entity)
     * </pre>
     */
    int insert(ItemMaster entity);

    /**
     * <pre>
     * insert into item_master
     * values(:entity)
     * </pre>
     */
    int insertOnly(ItemMaster entity);

    int[] insertBatch(Collection<ItemMaster> entityList);

    /**
     * <pre>
     * select * from item_master
     * where i_id in (:ids)
     *   and :date between i_effective_date and i_expired_date
     * order by i_id
     * </pre>
     */
    List<ItemMaster> selectByIds(Iterable<Integer> ids, LocalDate date);

    /**
     * <pre>
     * select * from item_master
     * where :date between i_effective_date and i_expired_date
     *   and i_type = :type
     * </pre>
     */
    Stream<ItemMaster> selectByType(LocalDate date, ItemType type);

    /**
     * <pre>
     * select i_id from item_master
     * where :date between i_effective_date and i_expired_date
     *   and i_type = :type
     * </pre>
     */
    List<Integer> selectIdByType(LocalDate date, ItemType type);

    /**
     * <pre>
     * select * from item_master
     * </pre>
     */
    List<ItemMaster> selectAll();

    /**
     * <pre>
     * select max(i_id) + 1 from item_master
     * </pre>
     */
    Integer selectMaxId();

    /**
     * <pre>
     * select * from item_master
     * where i_id = :id
     *   and i_effective_date = :date
     * </pre>
     */
    ItemMaster selectByKey(int id, LocalDate date);

    /**
     * <pre>
     * select * from item_master
     * where i_id = :id
     *   and :date between i_effective_date and i_expired_date
     * </pre>
     */
    ItemMaster selectById(int id, LocalDate date);

    void forEach(Consumer<ItemMaster> entityConsumer);
}
