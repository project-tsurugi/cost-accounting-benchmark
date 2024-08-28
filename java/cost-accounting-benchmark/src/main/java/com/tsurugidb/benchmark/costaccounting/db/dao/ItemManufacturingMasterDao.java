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

import com.tsurugidb.benchmark.costaccounting.db.entity.ItemManufacturingMaster;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemManufacturingMasterCount;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemManufacturingMasterIds;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao.SqlBetween;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain.BenchVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;

/**
 * 製造品目マスターDAO
 */
public interface ItemManufacturingMasterDao {

    public static final String TABLE_NAME = "item_manufacturing_master";

    public static final String PS_COND_DATE = "? between im_effective_date and im_expired_date";

    public static final TgBindVariable<LocalDate> vDate = BenchVariable.ofDate("date");
    static final String TG_COND_DATE = new SqlBetween(vDate, "im_effective_date", "im_expired_date").toString();

    /**
     * <pre>
     * truncate table item_manufacturing_master
     * </pre>
     */
    void truncate();

    /**
     * <pre>
     * delete from item_manufacturing_master
     * </pre>
     */
    int deleteAll();

    /**
     * <pre>
     * insert into item_manufacturing_master
     * values(:entity)
     * </pre>
     */
    int insert(ItemManufacturingMaster entity);

    int[] insertBatch(Collection<ItemManufacturingMaster> entityList);

    /**
     * <pre>
     * select * from item_manufacturing_master
     * where :date between im_effective_date and im_expired_date
     * order by im_f_id, im_i_id
     * </pre>
     */
    List<ItemManufacturingMaster> selectAll(LocalDate date);

    /**
     * <pre>
     * select * from item_manufacturing_master
     * </pre>
     */
    List<ItemManufacturingMaster> selectAll();

    /**
     * <pre>
     * select im_f_id, im_i_id from item_manufacturing_master
     * where :date between im_effective_date and im_expired_date
     * order by im_f_id, im_i_id
     * </pre>
     */
    List<ItemManufacturingMasterIds> selectIds(LocalDate date);

    /**
     * <pre>
     * select * from item_manufacturing_master
     * where im_f_id = :factoryId
     *   and :date between im_effective_date and im_expired_date
     * </pre>
     */
    Stream<ItemManufacturingMaster> selectByFactory(int factoryId, LocalDate date);

    /**
     * <pre>
     * select * from item_manufacturing_master
     * where im_f_id in (:factoryIdList)
     *   and :date between im_effective_date and im_expired_date
     * </pre>
     */
    Stream<ItemManufacturingMaster> selectByFactories(List<Integer> factoryIdList, LocalDate date);

    /**
     * <pre>
     * select im_f_id, count(*) from item_manufacturing_master
     * where :date between im_effective_date and im_expired_date
     * group by im_f_id
     * </pre>
     */
    List<ItemManufacturingMasterCount> selectCount(LocalDate date);

    /**
     * <pre>
     * select * from item_manufacturing_master
     * where im_f_id = :factoryId
     *   and im_i_id = :itemId
     *   and :date between im_effective_date and im_expired_date
     * </pre>
     */
    ItemManufacturingMaster selectById(int factoryId, int itemId, LocalDate date);

    /**
     * <pre>
     * select * from item_manufacturing_master
     * where im_f_id = :factoryId
     *   and im_i_id = :itemId
     *   and :date between im_effective_date and im_expired_date
     * for update
     * </pre>
     */
    ItemManufacturingMaster selectByIdForUpdate(int factoryId, int itemId, LocalDate date);

    /**
     * <pre>
     * select * from item_manufacturing_master
     * where im_f_id = :factoryId
     *   and im_i_id = :itemId
     *   and :date < im_effective_date
     * order by im_effective_date
     * </pre>
     */
    List<ItemManufacturingMaster> selectByIdFuture(int factoryId, int itemId, LocalDate date);

    /**
     * <pre>
     * update item_manufacturing_master
     * set :entity
     * </pre>
     */
    int update(ItemManufacturingMaster entity);

    /**
     * <pre>
     * select im_i_id from item_manufacturing_master
     * where im_f_id = :factoryId
     *   and :date between im_effective_date and im_expired_date
     * </pre>
     */
    List<Integer> selectIdByFactory(int factoryId, LocalDate date);

    void forEach(Consumer<ItemManufacturingMaster> entityConsumer);
}
