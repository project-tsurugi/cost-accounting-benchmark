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
package com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao;

import java.io.IOException;
import java.io.UncheckedIOException;
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
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.CostBenchDbManagerIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain.BenchVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable.TgBindVariableInteger;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariables;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;

public class ItemManufacturingMasterDaoIceaxe extends IceaxeDao<ItemManufacturingMaster> implements ItemManufacturingMasterDao {

    private static final TgBindVariableInteger IM_F_ID = BenchVariable.ofInt("im_f_id");
    private static final TgBindVariableInteger IM_I_ID = BenchVariable.ofInt("im_i_id");
    private static final List<IceaxeColumn<ItemManufacturingMaster, ?>> COLUMN_LIST;
    static {
        List<IceaxeColumn<ItemManufacturingMaster, ?>> list = new ArrayList<>();
        add(list, IM_F_ID, ItemManufacturingMaster::setImFId, ItemManufacturingMaster::getImFId, IceaxeRecordUtil::getInt, true);
        add(list, IM_I_ID, ItemManufacturingMaster::setImIId, ItemManufacturingMaster::getImIId, IceaxeRecordUtil::getInt, true);
        add(list, BenchVariable.ofDate("im_effective_date"), ItemManufacturingMaster::setImEffectiveDate, ItemManufacturingMaster::getImEffectiveDate, IceaxeRecordUtil::getDate, true);
        add(list, BenchVariable.ofDate("im_expired_date"), ItemManufacturingMaster::setImExpiredDate, ItemManufacturingMaster::getImExpiredDate, IceaxeRecordUtil::getDate);
        add(list, BenchVariable.ofBigInt("im_manufacturing_quantity"), ItemManufacturingMaster::setImManufacturingQuantity, ItemManufacturingMaster::getImManufacturingQuantity,
                IceaxeRecordUtil::getBigInt);
        COLUMN_LIST = list;
    }

    public ItemManufacturingMasterDaoIceaxe(CostBenchDbManagerIceaxe dbManager) {
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
        var ps = selectAllCache.get();
        var parameter = TgBindParameters.of(vDate.bind(date));
        return executeAndGetList(ps, parameter);
    }

    private final CachePreparedQuery<TgBindParameters, ItemManufacturingMaster> selectAllCache = new CachePreparedQuery<>() {
        @Override
        protected void initialize() {
            this.sql = getSelectEntitySql() + " where " + TG_COND_DATE + " order by im_f_id, im_i_id";
            this.parameterMapping = TgParameterMapping.of(vDate);
            this.resultMapping = getEntityResultMapping();
        }
    };

    @Override
    public List<ItemManufacturingMaster> selectAll() {
        return doSelectAll();
    }

    @Override
    public List<ItemManufacturingMasterIds> selectIds(LocalDate date) {
        var ps = selectIdsCache.get();
        var parameter = TgBindParameters.of(vDate.bind(date));
        return executeAndGetList(ps, parameter);
    }

    private final CachePreparedQuery<TgBindParameters, ItemManufacturingMasterIds> selectIdsCache = new CachePreparedQuery<>() {
        @Override
        protected void initialize() {
            this.sql = "select im_f_id, im_i_id from " + TABLE_NAME + " where " + TG_COND_DATE + " order by im_f_id, im_i_id";
            this.parameterMapping = TgParameterMapping.of(vDate);
            this.resultMapping = TgResultMapping.of(ItemManufacturingMasterIds::new) //
                    .addInt("im_f_id", ItemManufacturingMasterIds::setImFId) //
                    .addInt("im_i_id", ItemManufacturingMasterIds::setImIId);
        }
    };

    private static final TgBindVariableInteger vFactoryId = IM_F_ID.clone("factoryId");

    @Override
    public Stream<ItemManufacturingMaster> selectByFactory(int factoryId, LocalDate date) {
        var ps = selectByFactoryCache.get();
        var parameter = TgBindParameters.of(vFactoryId.bind(factoryId), vDate.bind(date));
        return executeAndGetStream(ps, parameter);
    }

    private final CachePreparedQuery<TgBindParameters, ItemManufacturingMaster> selectByFactoryCache = new CachePreparedQuery<>() {
        @Override
        protected void initialize() {
            this.sql = getSelectEntitySql() + " where im_f_id = " + vFactoryId + " and " + TG_COND_DATE;
            this.parameterMapping = TgParameterMapping.of(vFactoryId, vDate);
            this.resultMapping = getEntityResultMapping();
        }
    };

    @Override
    public Stream<ItemManufacturingMaster> selectByFactories(List<Integer> factoryIdList, LocalDate date) {
        var variables = TgBindVariables.of();
        var inSql = new SqlIn(IM_F_ID.name());
        var parameter = TgBindParameters.of();
        int i = 0;
        for (var factoryId : factoryIdList) {
            var variable = IM_F_ID.clone("id" + (i++));
            variables.add(variable);
            inSql.add(variable);
            parameter.add(variable.bind(factoryId));
        }
        variables.add(vDate);
        parameter.add(vDate.bind(date));

        var sql = getSelectEntitySql() + " where " + inSql + " and " + TG_COND_DATE;
        var parameterMapping = TgParameterMapping.of(variables);
        var resultMapping = getEntityResultMapping();
        var session = getSession();
        try (var ps = session.createQuery(sql, parameterMapping, resultMapping)) {
            return executeAndGetStream(ps, parameter);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<ItemManufacturingMasterCount> selectCount(LocalDate date) {
        var ps = selectCountCache.get();
        var parameter = TgBindParameters.of(vDate.bind(date));
        return executeAndGetList(ps, parameter);
    }

    private final CachePreparedQuery<TgBindParameters, ItemManufacturingMasterCount> selectCountCache = new CachePreparedQuery<>() {
        @Override
        protected void initialize() {
            this.sql = "select im_f_id, count(*) from item_manufacturing_master" //
                    + " where " + TG_COND_DATE //
                    + " group by im_f_id";
            this.parameterMapping = TgParameterMapping.of(vDate);
            this.resultMapping = TgResultMapping.of(ItemManufacturingMasterCount::new) //
                    .addInt(ItemManufacturingMasterCount::setImFId) //
                    .addInt(ItemManufacturingMasterCount::setCount);
        }
    };

    private static final TgBindVariableInteger vItemId = IM_I_ID.clone("itemId");

    @Override
    public ItemManufacturingMaster selectById(int factoryId, int itemId, LocalDate date) {
        var ps = selectByIdCache.get();
        var parameter = TgBindParameters.of(vFactoryId.bind(factoryId), vItemId.bind(itemId), vDate.bind(date));
        return executeAndGetRecord(ps, parameter);
    }

    private final CachePreparedQuery<TgBindParameters, ItemManufacturingMaster> selectByIdCache = new CachePreparedQuery<>() {
        @Override
        protected void initialize() {
            this.sql = getSelectEntitySql() + " where im_f_id = " + vFactoryId + " and im_i_id = " + vItemId + " and " + TG_COND_DATE;
            this.parameterMapping = TgParameterMapping.of(vFactoryId, vItemId, vDate);
            this.resultMapping = getEntityResultMapping();
        }
    };

    @Override
    public ItemManufacturingMaster selectByIdForUpdate(int factoryId, int itemId, LocalDate date) {
        // Tsurugiにselect for updateは無い
        return selectById(factoryId, itemId, date);
    }

    @Override
    public synchronized List<ItemManufacturingMaster> selectByIdFuture(int factoryId, int itemId, LocalDate date) {
        var ps = selectByIdFutureCache.get();
        var parameter = TgBindParameters.of(vFactoryId.bind(factoryId), vItemId.bind(itemId), vDate.bind(date));
        return executeAndGetList(ps, parameter);
    }

    private final CachePreparedQuery<TgBindParameters, ItemManufacturingMaster> selectByIdFutureCache = new CachePreparedQuery<>() {
        @Override
        protected void initialize() {
            this.sql = getSelectEntitySql() + " where im_f_id = " + vFactoryId + " and im_i_id = " + vItemId + " and " + vDate + " < im_effective_date" + " order by im_effective_date";
            this.parameterMapping = TgParameterMapping.of(vFactoryId, vItemId, vDate);
            this.resultMapping = getEntityResultMapping();
        }
    };

    @Override
    public int update(ItemManufacturingMaster entity) {
        return doUpdate(entity);
    }

    @Override
    public List<Integer> selectIdByFactory(int factoryId, LocalDate date) {
        var ps = selectIdByFactoryCache.get();
        var parameter = TgBindParameters.of(vFactoryId.bind(factoryId), vDate.bind(date));
        return executeAndGetList(ps, parameter);
    }

    private final CachePreparedQuery<TgBindParameters, Integer> selectIdByFactoryCache = new CachePreparedQuery<>() {
        @Override
        protected void initialize() {
            this.sql = "select im_i_id from " + TABLE_NAME + " where im_f_id = " + vFactoryId + " and " + TG_COND_DATE;
            this.parameterMapping = TgParameterMapping.of(vFactoryId, vDate);
            this.resultMapping = TgResultMapping.of(record -> record.nextIntOrNull());
        }
    };

    @Override
    public void forEach(Consumer<ItemManufacturingMaster> entityConsumer) {
        doForEach(entityConsumer);
    }
}
