package com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.seasar.doma.jdbc.SelectOptions;

import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.ItemManufacturingMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.doma2.entity.ItemManufacturingMaster;
import com.tsurugidb.benchmark.costaccounting.db.doma2.entity.ItemManufacturingMasterIds;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.CostBenchDbManagerIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain.BenchVariable;
import com.tsurugidb.iceaxe.result.TgResultMapping;
import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariable.TgVariableInteger;
import com.tsurugidb.iceaxe.statement.TgVariableList;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementQuery1;

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
    public int deleteAll() {
        return doDeleteAll();
    }

    @Override
    public int insert(ItemManufacturingMaster entity) {
        return doInsert(entity);
    }

    @Override
    public List<ItemManufacturingMaster> selectAll(LocalDate date) {
        var ps = getSelectAllPs();
        var param = TgParameterList.of(vDate.bind(date));
        return executeAndGetList(ps, param);
    }

    private TsurugiPreparedStatementQuery1<TgParameterList, ItemManufacturingMaster> selectAllPs;

    private synchronized TsurugiPreparedStatementQuery1<TgParameterList, ItemManufacturingMaster> getSelectAllPs() {
        if (this.selectAllPs == null) {
            var sql = getSelectEntitySql() + " where " + TG_COND_DATE + " order by im_f_id, im_i_id";
            var parameterMapping = TgParameterMapping.of(vDate);
            var resultMapping = getEntityResultMapping();
            this.selectAllPs = createPreparedQuery(sql, parameterMapping, resultMapping);
        }
        return this.selectAllPs;
    }

    @Override
    public List<ItemManufacturingMaster> selectAll() {
        return doSelectAll();
    }

    @Override
    public List<ItemManufacturingMasterIds> selectIds(LocalDate date) {
        var ps = getSelectIdsPs();
        var param = TgParameterList.of(vDate.bind(date));
        return executeAndGetList(ps, param);
    }

    private TsurugiPreparedStatementQuery1<TgParameterList, ItemManufacturingMasterIds> selectIdsPs;

    private synchronized TsurugiPreparedStatementQuery1<TgParameterList, ItemManufacturingMasterIds> getSelectIdsPs() {
        if (this.selectIdsPs == null) {
            var sql = "select im_f_id, im_i_id from " + TABLE_NAME + " where " + TG_COND_DATE + " order by im_f_id, im_i_id";
            var parameterMapping = TgParameterMapping.of(vDate);
            var resultMapping = TgResultMapping.of(ItemManufacturingMasterIds::new) //
                    .int4("im_f_id", ItemManufacturingMasterIds::setImFId) //
                    .int4("im_i_id", ItemManufacturingMasterIds::setImIId);
            this.selectIdsPs = createPreparedQuery(sql, parameterMapping, resultMapping);
        }
        return this.selectIdsPs;
    }

    private static final TgVariableInteger vFactoryId = IM_F_ID.copy("factoryId");

    @Override
    public Stream<ItemManufacturingMaster> selectByFactory(int factoryId, LocalDate date) {
        var ps = getSelectByFactoryPs();
        var param = TgParameterList.of(vFactoryId.bind(factoryId), vDate.bind(date));
        return executeAndGetStream(ps, param);
    }

    private TsurugiPreparedStatementQuery1<TgParameterList, ItemManufacturingMaster> selectByFactoryPs;

    private synchronized TsurugiPreparedStatementQuery1<TgParameterList, ItemManufacturingMaster> getSelectByFactoryPs() {
        if (this.selectByFactoryPs == null) {
            var sql = getSelectEntitySql() + " where im_f_id = " + vFactoryId + " and " + TG_COND_DATE;
            var parameterMapping = TgParameterMapping.of(vFactoryId, vDate);
            var resultMapping = getEntityResultMapping();
            this.selectByFactoryPs = createPreparedQuery(sql, parameterMapping, resultMapping);
        }
        return this.selectByFactoryPs;
    }

    @Override
    public Stream<ItemManufacturingMaster> selectByFactories(List<Integer> factoryIdList, LocalDate date) {
        var vlist = TgVariableList.of();
        var param = TgParameterList.of();
        int i = 0;
        for (var factoryId : factoryIdList) {
            var variable = IM_F_ID.copy(Integer.toString(i++));
            vlist.add(variable);
            param.add(variable.bind(factoryId));
        }
        var in = vlist.getSqlNames();
        vlist.add(vDate);
        param.add(vDate.bind(date));

        var sql = getSelectEntitySql() + " where im_f_id in (" + in + ") and " + TG_COND_DATE;
        var parameterMapping = TgParameterMapping.of(vlist);
        var resultMapping = getEntityResultMapping();
        try (var ps = createPreparedQuery(sql, parameterMapping, resultMapping)) {
            return executeAndGetStream(ps, param);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static final TgVariableInteger vItemId = IM_I_ID.copy("itemId");

    @Override
    public ItemManufacturingMaster selectById(int factoryId, int itemId, LocalDate date) {
        var ps = getSelectByIdPs();
        var param = TgParameterList.of(vFactoryId.bind(factoryId), vItemId.bind(itemId), vDate.bind(date));
        return executeAndGetRecord(ps, param);
    }

    private TsurugiPreparedStatementQuery1<TgParameterList, ItemManufacturingMaster> selectByIdPs;

    private synchronized TsurugiPreparedStatementQuery1<TgParameterList, ItemManufacturingMaster> getSelectByIdPs() {
        if (this.selectByIdPs == null) {
            var sql = getSelectEntitySql() + " where im_f_id = " + vFactoryId + " and im_i_id = " + vItemId + " and " + TG_COND_DATE;
            var parameterMapping = TgParameterMapping.of(vFactoryId, vItemId, vDate);
            var resultMapping = getEntityResultMapping();
            this.selectByIdPs = createPreparedQuery(sql, parameterMapping, resultMapping);
        }
        return this.selectByIdPs;
    }

    @Override
    public ItemManufacturingMaster selectByIdOptions(int factoryId, int itemId, LocalDate date, SelectOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ItemManufacturingMaster selectByIdForUpdate(int factoryId, int itemId, LocalDate date) {
        // Tsurugiにselect for updateは無い
        return selectById(factoryId, itemId, date);
    }

    @Override
    public List<ItemManufacturingMaster> selectByIdFuture(int factoryId, int itemId, LocalDate date) {
        var ps = getSelectByIdFuturePs();
        var param = TgParameterList.of(vFactoryId.bind(factoryId), vItemId.bind(itemId), vDate.bind(date));
        return executeAndGetList(ps, param);
    }

    private TsurugiPreparedStatementQuery1<TgParameterList, ItemManufacturingMaster> selectByIdFuturePs;

    private synchronized TsurugiPreparedStatementQuery1<TgParameterList, ItemManufacturingMaster> getSelectByIdFuturePs() {
        if (this.selectByIdFuturePs == null) {
            var sql = getSelectEntitySql() + " where im_f_id = " + vFactoryId + " and im_i_id = " + vItemId + " and " + vDate + " < im_effective_date" + " order by im_effective_date";
            var parameterMapping = TgParameterMapping.of(vFactoryId, vItemId, vDate);
            var resultMapping = getEntityResultMapping();
            this.selectByIdFuturePs = createPreparedQuery(sql, parameterMapping, resultMapping);
        }
        return this.selectByIdFuturePs;
    }

    @Override
    public int update(ItemManufacturingMaster entity) {
        return doUpdate(entity);
    }

    @Override
    public List<Integer> selectIdByFactory(int factoryId, LocalDate date) {
        var ps = getSelectIdByFactoryPs();
        var param = TgParameterList.of(vFactoryId.bind(factoryId), vDate.bind(date));
        return executeAndGetList(ps, param);
    }

    private TsurugiPreparedStatementQuery1<TgParameterList, Integer> selectIdByFactoryPs;

    private synchronized TsurugiPreparedStatementQuery1<TgParameterList, Integer> getSelectIdByFactoryPs() {
        if (this.selectIdByFactoryPs == null) {
            var sql = "select im_i_id from " + TABLE_NAME + " where im_f_id = " + vFactoryId + " and " + TG_COND_DATE;
            var parameterMapping = TgParameterMapping.of(vFactoryId, vDate);
            var resultMapping = TgResultMapping.of(record -> record.nextInt4OrNull());
            this.selectIdByFactoryPs = createPreparedQuery(sql, parameterMapping, resultMapping);
        }
        return this.selectIdByFactoryPs;
    }
}
