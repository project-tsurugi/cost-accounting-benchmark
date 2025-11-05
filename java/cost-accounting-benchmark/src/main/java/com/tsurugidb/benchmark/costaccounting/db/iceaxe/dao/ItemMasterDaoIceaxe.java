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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.tsurugidb.benchmark.costaccounting.db.dao.ItemMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.domain.ItemType;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemMaster;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.CostBenchDbManagerIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain.BenchVariable;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable.TgBindVariableInteger;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariables;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;

public class ItemMasterDaoIceaxe extends IceaxeDao<ItemMaster> implements ItemMasterDao {

    private static final TgBindVariableInteger I_ID = BenchVariable.ofInt("i_id");
    private static final TgBindVariable<ItemType> I_TYPE = BenchVariable.ofItemType("i_type");
    private static final List<IceaxeColumn<ItemMaster, ?>> COLUMN_LIST;
    static {
        List<IceaxeColumn<ItemMaster, ?>> list = new ArrayList<>();
        add(list, I_ID, ItemMaster::setIId, ItemMaster::getIId, IceaxeRecordUtil::getInt, true);
        add(list, BenchVariable.ofDate("i_effective_date"), ItemMaster::setIEffectiveDate, ItemMaster::getIEffectiveDate, IceaxeRecordUtil::getDate, true);
        add(list, BenchVariable.ofDate("i_expired_date"), ItemMaster::setIExpiredDate, ItemMaster::getIExpiredDate, IceaxeRecordUtil::getDate);
        add(list, BenchVariable.ofString("i_name"), ItemMaster::setIName, ItemMaster::getIName, IceaxeRecordUtil::getString);
        add(list, I_TYPE, ItemMaster::setIType, ItemMaster::getIType, IceaxeRecordUtil::getItemType);
        add(list, BenchVariable.ofString("i_unit"), ItemMaster::setIUnit, ItemMaster::getIUnit, IceaxeRecordUtil::getString);
        add(list, BenchVariable.ofDecimal("i_weight_ratio", ItemMaster.I_WEIGHT_RATIO_SCALE), ItemMaster::setIWeightRatio, ItemMaster::getIWeightRatio, IceaxeRecordUtil::getDecimal);
        add(list, BenchVariable.ofString("i_weight_unit"), ItemMaster::setIWeightUnit, ItemMaster::getIWeightUnit, IceaxeRecordUtil::getString);
        add(list, BenchVariable.ofDecimal("i_price", ItemMaster.I_PRICE_SCALE), ItemMaster::setIPrice, ItemMaster::getIPrice, IceaxeRecordUtil::getDecimal);
        add(list, BenchVariable.ofString("i_price_unit"), ItemMaster::setIPriceUnit, ItemMaster::getIPriceUnit, IceaxeRecordUtil::getString);
        COLUMN_LIST = list;
    }

    public ItemMasterDaoIceaxe(CostBenchDbManagerIceaxe dbManager) {
        super(dbManager, TABLE_NAME, COLUMN_LIST, ItemMaster::new);
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
    public int insert(ItemMaster entity) {
        return doInsert(entity, false);
    }

    @Override
    public int insertOnly(ItemMaster entity) {
        return doInsert(entity, true);
    }

    @Override
    public int[] insertBatch(Collection<ItemMaster> entityList) {
        return doInsert(entityList, false);
    }

    @Override
    public List<ItemMaster> selectByIds(Iterable<Integer> ids, LocalDate date) {
        if (BenchConst.WORKAROUND) { // pkのinがfull scanになるため
            return selectByIdsWorkaround(ids, date);
        }

        var variables = TgBindVariables.of();
        var inSql = new SqlIn(I_ID.name());
        var parameter = TgBindParameters.of();
        int i = 0;
        for (int id : ids) {
            var variable = I_ID.clone("id" + (i++));
            variables.add(variable);
            inSql.add(variable);
            parameter.add(variable.bind(id));
        }
        variables.add(vDate);
        parameter.add(vDate.bind(date));

        var sql = getSelectEntitySql() + " where " + inSql + " and " + TG_COND_DATE + " order by i_id";
        var parameterMapping = TgParameterMapping.of(variables);
        var resultMapping = getEntityResultMapping();
        var session = getSession();
        try (var ps = session.createQuery(sql, parameterMapping, resultMapping)) {
            return executeAndGetList(ps, parameter);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private List<ItemMaster> selectByIdsWorkaround(Iterable<Integer> ids, LocalDate date) {
        var ps = selectByIdsWorkaroundCache.get();

        var result = new ArrayList<ItemMaster>();
        for (int id : ids) {
            var parameter = TgBindParameters.of(vId.bind(id), vDate.bind(date));
            var r = executeAndGetList(ps, parameter);
            result.addAll(r);
        }
        Collections.sort(result, Comparator.comparing(ItemMaster::getIId));
        return result;
    }

    private final CachePreparedQuery<TgBindParameters, ItemMaster> selectByIdsWorkaroundCache = new CachePreparedQuery<>() {
        @Override
        protected void initialize() {
            this.sql = getSelectEntitySql() + " where i_id=" + vId + " and " + TG_COND_DATE;
            this.parameterMapping = TgParameterMapping.of(vId, vDate);
            this.resultMapping = getEntityResultMapping();
        }
    };

    private static final TgBindVariable<ItemType> vType = I_TYPE.clone("type");

    @Override
    public Stream<ItemMaster> selectByType(LocalDate date, ItemType type) {
        var ps = selectByTypeCache.get();
        var parameter = TgBindParameters.of(vDate.bind(date), vType.bind(type));
        return executeAndGetStream(ps, parameter);
    }

    private final CachePreparedQuery<TgBindParameters, ItemMaster> selectByTypeCache = new CachePreparedQuery<>() {
        @Override
        protected void initialize() {
            this.sql = getSelectEntitySql() + " where " + TG_COND_DATE + " and i_type = " + vType;
            this.parameterMapping = TgParameterMapping.of(vDate, vType);
            this.resultMapping = getEntityResultMapping();
        }
    };

    @Override
    public List<Integer> selectIdByType(LocalDate date, ItemType type) {
        var ps = selectIdByTypeCache.get();
        var parameter = TgBindParameters.of(vDate.bind(date), vType.bind(type));
        return executeAndGetList(ps, parameter);
    }

    private final CachePreparedQuery<TgBindParameters, Integer> selectIdByTypeCache = new CachePreparedQuery<>() {
        @Override
        protected void initialize() {
            this.sql = "select i_id from " + TABLE_NAME + " where " + TG_COND_DATE + " and i_type = " + vType;
            this.parameterMapping = TgParameterMapping.of(vDate, vType);
            this.resultMapping = TgResultMapping.of(record -> record.nextInt());
        }
    };

    @Override
    public List<ItemMaster> selectAll() {
        return doSelectAll();
    }

    @Override
    public Integer selectMaxId() {
        var ps = selectMaxIdCache.get();
        return executeAndGetRecord(ps);
    }

    private final CacheQuery<Integer> selectMaxIdCache = new CacheQuery<>() {
        @Override
        protected void initialize() {
            this.sql = "select max(i_id) + 1 from " + TABLE_NAME;
            this.resultMapping = TgResultMapping.of(record -> record.nextInt());
        }
    };

    private static final TgBindVariableInteger vId = I_ID.clone("id");

    @Override
    public ItemMaster selectByKey(int id, LocalDate date) {
        var ps = selectByKeyCache.get();
        var parameter = TgBindParameters.of(vId.bind(id), vDate.bind(date));
        return executeAndGetRecord(ps, parameter);
    }

    private final CachePreparedQuery<TgBindParameters, ItemMaster> selectByKeyCache = new CachePreparedQuery<>() {
        @Override
        protected void initialize() {
            this.sql = getSelectEntitySql() + " where i_id = " + vId + " and i_effective_date = " + vDate;
            this.parameterMapping = TgParameterMapping.of(vId, vDate);
            this.resultMapping = getEntityResultMapping();
        }
    };

    @Override
    public ItemMaster selectById(int id, LocalDate date) {
        var ps = selectByIdCache.get();
        var parameter = TgBindParameters.of(vId.bind(id), vDate.bind(date));
        return executeAndGetRecord(ps, parameter);
    }

    private final CachePreparedQuery<TgBindParameters, ItemMaster> selectByIdCache = new CachePreparedQuery<>() {
        @Override
        protected void initialize() {
            this.sql = getSelectEntitySql() + " where i_id = " + vId + " and " + TG_COND_DATE;
            this.parameterMapping = TgParameterMapping.of(vId, vDate);
            this.resultMapping = getEntityResultMapping();
        }
    };

    @Override
    public void forEach(Consumer<ItemMaster> entityConsumer) {
        doForEach(entityConsumer);
    }
}
