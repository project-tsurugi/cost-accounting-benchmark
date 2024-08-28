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
package com.tsurugidb.benchmark.costaccounting.db.tsubakuro.dao;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.tsurugidb.benchmark.costaccounting.db.dao.ItemManufacturingMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemManufacturingMaster;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemManufacturingMasterCount;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemManufacturingMasterIds;
import com.tsurugidb.benchmark.costaccounting.db.tsubakuro.CostBenchDbManagerTsubakuro;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;

public class ItemManufacturingMasterDaoTsubakuro extends TsubakuroDao<ItemManufacturingMaster> implements ItemManufacturingMasterDao {

    private static final List<TsubakuroColumn<ItemManufacturingMaster, ?>> COLUMN_LIST;
    static {
        List<TsubakuroColumn<ItemManufacturingMaster, ?>> list = new ArrayList<>();
        add(list, "im_f_id", AtomType.INT4, ItemManufacturingMaster::setImFId, ItemManufacturingMaster::getImFId, TsubakuroUtil::getParameter, TsubakuroUtil::getInt, true);
        add(list, "im_i_id", AtomType.INT4, ItemManufacturingMaster::setImIId, ItemManufacturingMaster::getImIId, TsubakuroUtil::getParameter, TsubakuroUtil::getInt, true);
        add(list, "im_effective_date", AtomType.DATE, ItemManufacturingMaster::setImEffectiveDate, ItemManufacturingMaster::getImEffectiveDate, TsubakuroUtil::getParameter, TsubakuroUtil::getDate,
                true);
        add(list, "im_expired_date", AtomType.DATE, ItemManufacturingMaster::setImExpiredDate, ItemManufacturingMaster::getImExpiredDate, TsubakuroUtil::getParameter, TsubakuroUtil::getDate);
        add(list, "im_manufacturing_quantity", AtomType.DECIMAL, ItemManufacturingMaster::setImManufacturingQuantity, ItemManufacturingMaster::getImManufacturingQuantity, TsubakuroUtil::getParameter,
                TsubakuroUtil::getBigInt);
        COLUMN_LIST = list;
    }

    public ItemManufacturingMasterDaoTsubakuro(CostBenchDbManagerTsubakuro dbManager) {
        super(dbManager, TABLE_NAME, COLUMN_LIST, ItemManufacturingMaster::new);
    }

    @Override
    public void truncate() {
        doTruncate();
    }

    @Override
    public int deleteAll() {
        return doDeleteAll();
    }

    @Override
    public int insert(ItemManufacturingMaster entity) {
        return doInsert(entity, false);
    }

    @Override
    public int[] insertBatch(Collection<ItemManufacturingMaster> entityList) {
        return doInsert(entityList, false);
    }

    @Override
    public List<ItemManufacturingMaster> selectAll(LocalDate date) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public List<ItemManufacturingMaster> selectAll() {
        return doSelectAll();
    }

    @Override
    public List<ItemManufacturingMasterIds> selectIds(LocalDate date) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public Stream<ItemManufacturingMaster> selectByFactory(int factoryId, LocalDate date) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public Stream<ItemManufacturingMaster> selectByFactories(List<Integer> factoryIdList, LocalDate date) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public List<ItemManufacturingMasterCount> selectCount(LocalDate date) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public ItemManufacturingMaster selectById(int factoryId, int itemId, LocalDate date) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public ItemManufacturingMaster selectByIdForUpdate(int factoryId, int itemId, LocalDate date) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public List<ItemManufacturingMaster> selectByIdFuture(int factoryId, int itemId, LocalDate date) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public int update(ItemManufacturingMaster entity) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public List<Integer> selectIdByFactory(int factoryId, LocalDate date) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public void forEach(Consumer<ItemManufacturingMaster> entityConsumer) {
        throw new UnsupportedOperationException("not yet impl");
    }
}
