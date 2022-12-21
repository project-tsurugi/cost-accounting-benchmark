package com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.tsurugidb.benchmark.costaccounting.db.dao.ItemManufacturingMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemManufacturingMaster;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemManufacturingMasterIds;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.CostBenchDbManagerIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain.BenchVariable;
import com.tsurugidb.iceaxe.result.TgResultMapping;
import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariable.TgVariableInteger;
import com.tsurugidb.iceaxe.statement.TgVariableList;

public class ItemManufacturingMasterDaoIceaxe extends IceaxeDao<ItemManufacturingMaster> implements ItemManufacturingMasterDao {

    private static final TgVariableInteger IM_F_ID = BenchVariable.ofInt("im_f_id");
    private static final TgVariableInteger IM_I_ID = BenchVariable.ofInt("im_i_id");
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
        return doInsert(entity);
    }

    @Override
    public List<ItemManufacturingMaster> selectAll(LocalDate date) {
        var ps = selectAllCache.get();
        var param = TgParameterList.of(vDate.bind(date));
        return executeAndGetList(ps, param);
    }

    private final CachePreparedQuery<TgParameterList, ItemManufacturingMaster> selectAllCache = new CachePreparedQuery<>() {
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
        var param = TgParameterList.of(vDate.bind(date));
        return executeAndGetList(ps, param);
    }

    private final CachePreparedQuery<TgParameterList, ItemManufacturingMasterIds> selectIdsCache = new CachePreparedQuery<>() {
        @Override
        protected void initialize() {
            this.sql = "select im_f_id, im_i_id from " + TABLE_NAME + " where " + TG_COND_DATE + " order by im_f_id, im_i_id";
            this.parameterMapping = TgParameterMapping.of(vDate);
            this.resultMapping = TgResultMapping.of(ItemManufacturingMasterIds::new) //
                    .int4("im_f_id", ItemManufacturingMasterIds::setImFId) //
                    .int4("im_i_id", ItemManufacturingMasterIds::setImIId);
        }
    };

    private static final TgVariableInteger vFactoryId = IM_F_ID.copy("factoryId");

    @Override
    public Stream<ItemManufacturingMaster> selectByFactory(int factoryId, LocalDate date) {
        var ps = selectByFactoryCache.get();
        var param = TgParameterList.of(vFactoryId.bind(factoryId), vDate.bind(date));
        return executeAndGetStream(ps, param);
    }

    private final CachePreparedQuery<TgParameterList, ItemManufacturingMaster> selectByFactoryCache = new CachePreparedQuery<>() {
        @Override
        protected void initialize() {
            this.sql = getSelectEntitySql() + " where im_f_id = " + vFactoryId + " and " + TG_COND_DATE;
            this.parameterMapping = TgParameterMapping.of(vFactoryId, vDate);
            this.resultMapping = getEntityResultMapping();
        }
    };

    @Override
    public Stream<ItemManufacturingMaster> selectByFactories(List<Integer> factoryIdList, LocalDate date) {
        var vlist = TgVariableList.of();
        var inSql = new SqlIn(IM_F_ID.name());
        var param = TgParameterList.of();
        int i = 0;
        for (var factoryId : factoryIdList) {
            var variable = IM_F_ID.copy("id" + (i++));
            vlist.add(variable);
            inSql.add(variable);
            param.add(variable.bind(factoryId));
        }
        vlist.add(vDate);
        param.add(vDate.bind(date));

        var sql = getSelectEntitySql() + " where " + inSql + " and " + TG_COND_DATE;
        var parameterMapping = TgParameterMapping.of(vlist);
        var resultMapping = getEntityResultMapping();
        var session = getSession();
        try (var ps = session.createPreparedQuery(sql, parameterMapping, resultMapping)) {
            return executeAndGetStream(ps, param);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static final TgVariableInteger vItemId = IM_I_ID.copy("itemId");

    @Override
    public ItemManufacturingMaster selectById(int factoryId, int itemId, LocalDate date) {
        var ps = selectByIdCache.get();
        var param = TgParameterList.of(vFactoryId.bind(factoryId), vItemId.bind(itemId), vDate.bind(date));
        return executeAndGetRecord(ps, param);
    }

    private final CachePreparedQuery<TgParameterList, ItemManufacturingMaster> selectByIdCache = new CachePreparedQuery<>() {
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
        var param = TgParameterList.of(vFactoryId.bind(factoryId), vItemId.bind(itemId), vDate.bind(date));
        return executeAndGetList(ps, param);
    }

    private final CachePreparedQuery<TgParameterList, ItemManufacturingMaster> selectByIdFutureCache = new CachePreparedQuery<>() {
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
        var param = TgParameterList.of(vFactoryId.bind(factoryId), vDate.bind(date));
        return executeAndGetList(ps, param);
    }

    private final CachePreparedQuery<TgParameterList, Integer> selectIdByFactoryCache = new CachePreparedQuery<>() {
        @Override
        protected void initialize() {
            this.sql = "select im_i_id from " + TABLE_NAME + " where im_f_id = " + vFactoryId + " and " + TG_COND_DATE;
            this.parameterMapping = TgParameterMapping.of(vFactoryId, vDate);
            this.resultMapping = TgResultMapping.of(record -> record.nextInt4OrNull());
        }
    };

    @Override
    public void forEach(Consumer<ItemManufacturingMaster> entityConsumer) {
        doForEach(entityConsumer);
    }
}
