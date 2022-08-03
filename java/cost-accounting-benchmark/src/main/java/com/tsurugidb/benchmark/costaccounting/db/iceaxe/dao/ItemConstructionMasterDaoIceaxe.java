package com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.ItemConstructionMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.ItemMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.doma2.domain.ItemType;
import com.tsurugidb.benchmark.costaccounting.db.doma2.entity.ItemConstructionMaster;
import com.tsurugidb.benchmark.costaccounting.db.doma2.entity.ItemConstructionMasterIds;
import com.tsurugidb.benchmark.costaccounting.db.doma2.entity.ItemConstructionMasterKey;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.CostBenchDbManagerIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain.BenchVariable;
import com.tsurugidb.iceaxe.result.TgResultMapping;
import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariable.TgVariableInteger;
import com.tsurugidb.iceaxe.statement.TgVariableList;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementQuery1;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementUpdate1;

public class ItemConstructionMasterDaoIceaxe extends IceaxeDao<ItemConstructionMaster> implements ItemConstructionMasterDao {

    private static final TgVariableInteger IC_PARENT_I_ID = BenchVariable.ofInt("ic_parent_i_id");
    private static final TgVariableInteger IC_I_ID = BenchVariable.ofInt("ic_i_id");
    private static final List<IceaxeColumn<ItemConstructionMaster, ?>> COLUMN_LIST;
    static {
        List<IceaxeColumn<ItemConstructionMaster, ?>> list = new ArrayList<>();
        add(list, IC_PARENT_I_ID, ItemConstructionMaster::setIcParentIId, ItemConstructionMaster::getIcParentIId, IceaxeRecordUtil::getInt, true);
        add(list, IC_I_ID, ItemConstructionMaster::setIcIId, ItemConstructionMaster::getIcIId, IceaxeRecordUtil::getInt, true);
        add(list, BenchVariable.ofDate("ic_effective_date"), ItemConstructionMaster::setIcEffectiveDate, ItemConstructionMaster::getIcEffectiveDate, IceaxeRecordUtil::getDate, true);
        add(list, BenchVariable.ofDate("ic_expired_date"), ItemConstructionMaster::setIcExpiredDate, ItemConstructionMaster::getIcExpiredDate, IceaxeRecordUtil::getDate);
        add(list, BenchVariable.ofString("ic_material_unit"), ItemConstructionMaster::setIcMaterialUnit, ItemConstructionMaster::getIcMaterialUnit, IceaxeRecordUtil::getString);
        add(list, BenchVariable.ofDecimal("ic_material_quantity"), ItemConstructionMaster::setIcMaterialQuantity, ItemConstructionMaster::getIcMaterialQuantity, IceaxeRecordUtil::getDecimal);
        add(list, BenchVariable.ofDecimal("ic_loss_ratio"), ItemConstructionMaster::setIcLossRatio, ItemConstructionMaster::getIcLossRatio, IceaxeRecordUtil::getDecimal);
        COLUMN_LIST = list;
    }

    public ItemConstructionMasterDaoIceaxe(CostBenchDbManagerIceaxe dbManager) {
        super(dbManager, TABLE_NAME, COLUMN_LIST, ItemConstructionMaster::new);
    }

    @Override
    public int deleteAll() {
        return doDeleteAll();
    }

    @Override
    public int insert(ItemConstructionMaster entity) {
        return doInsert(entity);
    }

    @Override
    public List<ItemConstructionMaster> selectAll() {
        return doSelectAll();
    }

    @Override
    public List<ItemConstructionMaster> selectAll(LocalDate date) {
        var ps = getSelectAllPs();
        var param = TgParameterList.of(vDate.bind(date));
        return executeAndGetList(ps, param);
    }

    private TsurugiPreparedStatementQuery1<TgParameterList, ItemConstructionMaster> selectAllPs;

    private synchronized TsurugiPreparedStatementQuery1<TgParameterList, ItemConstructionMaster> getSelectAllPs() {
        if (this.selectAllPs == null) {
            var sql = getSelectEntitySql() + " where " + TG_COND_DATE + " order by ic_parent_i_id, ic_i_id";
            var parameterMapping = TgParameterMapping.of(vDate);
            var resultMapping = getEntityResultMapping();
            this.selectAllPs = createPreparedQuery(sql, parameterMapping, resultMapping);
        }
        return this.selectAllPs;
    }

    @Override
    public List<ItemConstructionMasterIds> selectIds(LocalDate date) {
        var ps = getSelectIdsPs();
        var param = TgParameterList.of(vDate.bind(date));
        return executeAndGetList(ps, param);
    }

    private TsurugiPreparedStatementQuery1<TgParameterList, ItemConstructionMasterIds> selectIdsPs;

    private synchronized TsurugiPreparedStatementQuery1<TgParameterList, ItemConstructionMasterIds> getSelectIdsPs() {
        if (this.selectIdsPs == null) {
            var sql = "select ic_parent_i_id, ic_i_id from " + TABLE_NAME + " where " + TG_COND_DATE + " order by ic_parent_i_id, ic_i_id";
            var parameterMapping = TgParameterMapping.of(vDate);
            var resultMapping = TgResultMapping.of(ItemConstructionMasterIds::new) //
                    .int4(ItemConstructionMasterIds::setIcParentIId) //
                    .int4(ItemConstructionMasterIds::setIcIId);
            this.selectIdsPs = createPreparedQuery(sql, parameterMapping, resultMapping);
        }
        return this.selectIdsPs;
    }

    private static final TgVariableInteger vParentId = IC_PARENT_I_ID.copy("parentId");

    @Override
    public List<ItemConstructionMaster> selectByParentId(int parentId, LocalDate date) {
        var ps = getSelectByParentId();
        var param = TgParameterList.of(vParentId.bind(parentId), vDate.bind(date));
        return executeAndGetList(ps, param);
    }

    private TsurugiPreparedStatementQuery1<TgParameterList, ItemConstructionMaster> selectByParentIdPs;

    private synchronized TsurugiPreparedStatementQuery1<TgParameterList, ItemConstructionMaster> getSelectByParentId() {
        if (this.selectByParentIdPs == null) {
            var sql = getSelectEntitySql() + " where ic_parent_i_id = " + vParentId + " and " + TG_COND_DATE + " order by ic_i_id";
            var parameterMapping = TgParameterMapping.of(vParentId, vDate);
            var resultMapping = getEntityResultMapping();
            this.selectByParentIdPs = createPreparedQuery(sql, parameterMapping, resultMapping);
        }
        return this.selectByParentIdPs;
    }

    private static final TgVariableInteger vItemId = IC_I_ID.copy("itemId");

    @Override
    public ItemConstructionMaster selectById(int parentId, int itemId, LocalDate date) {
        var ps = getSelectByIdPs();
        var param = TgParameterList.of(vParentId.bind(parentId), vItemId.bind(itemId), vDate.bind(date));
        return executeAndGetRecord(ps, param);
    }

    private TsurugiPreparedStatementQuery1<TgParameterList, ItemConstructionMaster> selectByIdPs;

    private synchronized TsurugiPreparedStatementQuery1<TgParameterList, ItemConstructionMaster> getSelectByIdPs() {
        if (this.selectByIdPs == null) {
            var sql = getSelectEntitySql() + " where ic_parent_i_id = " + vParentId + " and ic_i_id = " + vItemId + " and " + TG_COND_DATE;
            var parameterMapping = TgParameterMapping.of(vParentId, vItemId, vDate);
            var resultMapping = getEntityResultMapping();
            this.selectByIdPs = createPreparedQuery(sql, parameterMapping, resultMapping);
        }
        return this.selectByIdPs;
    }

    @Override
    public Stream<ItemConstructionMaster> selectRecursiveByParentId(int parentId, LocalDate date) {
        throw new UnsupportedOperationException("impossible with-recursive");
    }

    @Override
    public List<ItemConstructionMasterKey> selectByItemType(LocalDate date, List<ItemType> typeList) {
        var vlist = TgVariableList.of();
        vlist.add(vDate);
        var param = TgParameterList.of();
        param.add(vDate.bind(date));

        var inSql = new SqlIn("i_type");
        int i = 0;
        for (var type : typeList) {
            var variable = BenchVariable.ofItemType("t" + (i++));
            vlist.add(variable);
            inSql.add(variable);
            param.add(variable.bind(type));
        }

        var sql = "select ic_parent_i_id, ic_i_id, ic_effective_date" //
                + " from " + TABLE_NAME + " ic" //
                + " left join " + ItemMasterDao.TABLE_NAME + " i on i_id=ic_i_id and " + ItemMasterDaoIceaxe.TG_COND_DATE //
                + " where " + TG_COND_DATE //
                + " and " + inSql //
        ;
        var parameterMapping = TgParameterMapping.of(vlist);
        var resultMapping = TgResultMapping.of(ItemConstructionMasterKey::new) //
                .int4(ItemConstructionMasterKey::setIcParentIId) //
                .int4(ItemConstructionMasterKey::setIcIId) //
                .date(ItemConstructionMasterKey::setIcEffectiveDate);
        try (var ps = createPreparedQuery(sql, parameterMapping, resultMapping)) {
            return executeAndGetList(ps, param);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public ItemConstructionMaster lock(ItemConstructionMaster in) {
        // Tsurugiにselect for updateは無い
        return in;
    }

    @Override
    public int delete(ItemConstructionMasterKey key) {
        var ps = getDeletePs();
        return executeAndGetCount(ps, key);
    }

    private TsurugiPreparedStatementUpdate1<ItemConstructionMasterKey> deletePs;

    private synchronized TsurugiPreparedStatementUpdate1<ItemConstructionMasterKey> getDeletePs() {
        if (this.deletePs == null) {
            var sql = "delete from " + TABLE_NAME + " where ic_i_id = " + vItemId + " and ic_parent_i_id = " + vParentId + " and ic_effective_date = " + vDate;
            var parameterMapping = TgParameterMapping.of(ItemConstructionMasterKey.class) //
                    .add(vItemId, ItemConstructionMasterKey::getIcIId) //
                    .add(vParentId, ItemConstructionMasterKey::getIcParentIId) //
                    .add(vDate, ItemConstructionMasterKey::getIcEffectiveDate);
            this.deletePs = createPreparedStatement(sql, parameterMapping);
        }
        return this.deletePs;
    }
}
