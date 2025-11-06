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
package com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.tsurugidb.benchmark.costaccounting.db.dao.CostMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.CostMaster;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.CostBenchDbManagerIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain.BenchVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable.TgBindVariableInteger;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;

public class CostMasterDaoIceaxe extends IceaxeDao<CostMaster> implements CostMasterDao {

    private static final TgBindVariableInteger C_F_ID = BenchVariable.ofInt("c_f_id");
    private static final TgBindVariableInteger C_I_ID = BenchVariable.ofInt("c_i_id");
    private static final List<IceaxeColumn<CostMaster, ?>> COLUMN_LIST;
    static {
        List<IceaxeColumn<CostMaster, ?>> list = new ArrayList<>();
        add(list, C_F_ID, CostMaster::setCFId, CostMaster::getCFId, IceaxeRecordUtil::getInt, true);
        add(list, C_I_ID, CostMaster::setCIId, CostMaster::getCIId, IceaxeRecordUtil::getInt, true);
        add(list, BenchVariable.ofString("c_stock_unit"), CostMaster::setCStockUnit, CostMaster::getCStockUnit, IceaxeRecordUtil::getString);
        add(list, BenchVariable.ofDecimal("c_stock_quantity", CostMaster.C_STOCK_QUANTITY_SCALE), CostMaster::setCStockQuantity, CostMaster::getCStockQuantity, IceaxeRecordUtil::getDecimal);
        add(list, BenchVariable.ofDecimal("c_stock_amount", CostMaster.C_STOCK_AMOUNT_SCALE), CostMaster::setCStockAmount, CostMaster::getCStockAmount, IceaxeRecordUtil::getDecimal);
        COLUMN_LIST = list;
    }

    public CostMasterDaoIceaxe(CostBenchDbManagerIceaxe dbManager) {
        super(dbManager, TABLE_NAME, COLUMN_LIST, CostMaster::new);
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
    public int insert(CostMaster entity) {
        return doInsert(entity, false);
    }

    @Override
    public int[] insertBatch(Collection<CostMaster> entityList) {
        return doInsert(entityList, false);
    }

    @Override
    public Stream<CostMaster> selectAll() {
        var ps = selectAllCache.get();
        var parameter = TgBindParameters.of();
        return executeAndGetStream(ps, parameter);
    }

    private final CachePreparedQuery<TgBindParameters, CostMaster> selectAllCache = new CachePreparedQuery<>() {
        @Override
        protected void initialize() {
            this.sql = getSelectEntitySql();
            this.parameterMapping = TgParameterMapping.of();
            this.resultMapping = getEntityResultMapping();
        }
    };

    private static final TgBindVariableInteger vFactoryId = C_F_ID.clone("fId");

    @Override
    public Stream<CostMaster> selectByFactory(int fId) {
        var ps = selectByFactoryCache.get();
        var parameter = TgBindParameters.of(vFactoryId.bind(fId));
        return executeAndGetStream(ps, parameter);
    }

    private final CachePreparedQuery<TgBindParameters, CostMaster> selectByFactoryCache = new CachePreparedQuery<>() {
        @Override
        protected void initialize() {
            this.sql = getSelectEntitySql() + " where c_f_id = " + vFactoryId;
            this.parameterMapping = TgParameterMapping.of(vFactoryId);
            this.resultMapping = getEntityResultMapping();
        }
    };

    @Override
    public List<Integer> selectIdByFactory(int fId) {
        var ps = selectIdByFactoryCache.get();
        var parameter = TgBindParameters.of(vFactoryId.bind(fId));
        return executeAndGetList(ps, parameter);
    }

    private final CachePreparedQuery<TgBindParameters, Integer> selectIdByFactoryCache = new CachePreparedQuery<>() {
        @Override
        protected void initialize() {
            this.sql = "select c_i_id from " + TABLE_NAME + " where c_f_id = " + vFactoryId + " order by c_i_id";
            this.parameterMapping = TgParameterMapping.of(vFactoryId);
            this.resultMapping = TgResultMapping.ofSingle(int.class);
        }
    };

    private static final TgBindVariableInteger vItemId = C_I_ID.clone("iId");

    @Override
    public CostMaster selectById(int fId, int iId, boolean forUpdate) {
        var ps = selectByIdCache.get();
        var parameter = TgBindParameters.of(vFactoryId.bind(fId), vItemId.bind(iId));
        return executeAndGetRecord(ps, parameter);
    }

    private final CachePreparedQuery<TgBindParameters, CostMaster> selectByIdCache = new CachePreparedQuery<>() {
        @Override
        protected void initialize() {
            this.sql = getSelectEntitySql() + " where c_f_id = " + vFactoryId + " and c_i_id = " + vItemId;
            this.parameterMapping = TgParameterMapping.of(vFactoryId, vItemId);
            this.resultMapping = getEntityResultMapping();
        }
    };

    private static final TgBindVariable<BigDecimal> vQuantity = BenchVariable.ofDecimal("quantity", CostMaster.C_STOCK_QUANTITY_SCALE);
    private static final TgBindVariable<BigDecimal> vAmount = BenchVariable.ofDecimal("amount", CostMaster.C_STOCK_AMOUNT_SCALE);

    @Override
    public int updateIncrease(CostMaster entity, BigDecimal quantity, BigDecimal amount) {
        var ps = updateIncreaseCache.get();
        var parameter = TgBindParameters.of(vFactoryId.bind(entity.getCFId()), vItemId.bind(entity.getCIId()), vQuantity.bind(quantity), vAmount.bind(amount));
        return executeAndGetCount(ps, parameter);
    }

    private final CachePreparedStatement<TgBindParameters> updateIncreaseCache = new CachePreparedStatement<>() {
        @Override
        protected void initialize() {
            this.sql = "update " + TABLE_NAME + " set" //
                    + " c_stock_quantity = c_stock_quantity + " + vQuantity //
                    + ",c_stock_amount = c_stock_amount + " + vAmount //
                    + " where c_f_id=" + vFactoryId + " and c_i_id=" + vItemId;
            this.parameterMapping = TgParameterMapping.of(vFactoryId, vItemId, vQuantity, vAmount);
        }
    };

    @Override
    public int updateDecrease(CostMaster entity, BigDecimal quantity) {
        var ps = updateDecreaseCache.get();
        var parameter = TgBindParameters.of(vFactoryId.bind(entity.getCFId()), vItemId.bind(entity.getCIId()), vQuantity.bind(quantity));
        return executeAndGetCount(ps, parameter);
    }

    private final CachePreparedStatement<TgBindParameters> updateDecreaseCache = new CachePreparedStatement<>() {
        @Override
        protected void initialize() {
            String stockAmountType = "decimal(15," + CostMaster.C_STOCK_AMOUNT_SCALE + ")";
            this.sql = "update " + TABLE_NAME + " set" //
                    + " c_stock_quantity = c_stock_quantity - " + vQuantity //
                    + ",c_stock_amount = cast(c_stock_amount - c_stock_amount * " + vQuantity + " / c_stock_quantity as " + stockAmountType + ")" //
                    + " where c_f_id=" + vFactoryId + " and c_i_id=" + vItemId;
            this.parameterMapping = TgParameterMapping.of(vFactoryId, vItemId, vQuantity);
        }
    };

    @Override
    public int updateZero(CostMaster entity) {
        var ps = updateZeroCache.get();
        var parameter = TgBindParameters.of(vFactoryId.bind(entity.getCFId()), vItemId.bind(entity.getCIId()));
        return executeAndGetCount(ps, parameter);
    }

    private final CachePreparedStatement<TgBindParameters> updateZeroCache = new CachePreparedStatement<>() {
        @Override
        protected void initialize() {
            this.sql = "update " + TABLE_NAME + " set" //
                    + " c_stock_quantity = 0" //
                    + ",c_stock_amount = 0" //
                    + " where c_f_id=" + vFactoryId + " and c_i_id=" + vItemId;
            this.parameterMapping = TgParameterMapping.of(vFactoryId, vItemId);
        }
    };

    @Override
    public void forEach(Consumer<CostMaster> entityConsumer) {
        doForEach(entityConsumer);
    }
}
