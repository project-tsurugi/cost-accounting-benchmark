package com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.tsurugidb.benchmark.costaccounting.db.dao.ItemConstructionMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.domain.ItemType;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemConstructionMaster;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemConstructionMasterIds;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemConstructionMasterKey;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.CostBenchDbManagerIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain.BenchVariable;
import com.tsurugidb.iceaxe.result.TgResultMapping;
import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariable.TgVariableInteger;
import com.tsurugidb.iceaxe.statement.TgVariableList;

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
        add(list, BenchVariable.ofDecimal("ic_material_quantity", ItemConstructionMaster.IC_MATERIAL_QUANTITY_SCALE), ItemConstructionMaster::setIcMaterialQuantity,
                ItemConstructionMaster::getIcMaterialQuantity, IceaxeRecordUtil::getDecimal);
        add(list, BenchVariable.ofDecimal("ic_loss_ratio", ItemConstructionMaster.IC_LOSS_RATIO_SCALE), ItemConstructionMaster::setIcLossRatio, ItemConstructionMaster::getIcLossRatio,
                IceaxeRecordUtil::getDecimal);
        COLUMN_LIST = list;
    }

    public ItemConstructionMasterDaoIceaxe(CostBenchDbManagerIceaxe dbManager) {
        super(dbManager, TABLE_NAME, COLUMN_LIST, ItemConstructionMaster::new);
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
    public int insert(ItemConstructionMaster entity) {
        return doInsert(entity);
    }

    @Override
    public List<ItemConstructionMaster> selectAll() {
        return doSelectAll();
    }

    @Override
    public List<ItemConstructionMaster> selectAll(LocalDate date) {
        var ps = selectAllCache.get();
        var param = TgParameterList.of(vDate.bind(date));
        return executeAndGetList(ps, param);
    }

    private final CachePreparedQuery<TgParameterList, ItemConstructionMaster> selectAllCache = new CachePreparedQuery<>() {
        @Override
        protected void initialize() {
            this.sql = getSelectEntitySql() + " where " + TG_COND_DATE + " order by ic_parent_i_id, ic_i_id";
            this.parameterMapping = TgParameterMapping.of(vDate);
            this.resultMapping = getEntityResultMapping();
        }
    };

    @Override
    public List<ItemConstructionMasterIds> selectIds(LocalDate date) {
        var ps = selectIdsCache.get();
        var param = TgParameterList.of(vDate.bind(date));
        return executeAndGetList(ps, param);
    }

    private final CachePreparedQuery<TgParameterList, ItemConstructionMasterIds> selectIdsCache = new CachePreparedQuery<>() {
        @Override
        protected void initialize() {
            this.sql = "select ic_parent_i_id, ic_i_id from " + TABLE_NAME + " where " + TG_COND_DATE + " order by ic_parent_i_id, ic_i_id";
            this.parameterMapping = TgParameterMapping.of(vDate);
            this.resultMapping = TgResultMapping.of(ItemConstructionMasterIds::new) //
                    .int4(ItemConstructionMasterIds::setIcParentIId) //
                    .int4(ItemConstructionMasterIds::setIcIId);
        }
    };

    private static final TgVariableInteger vParentId = IC_PARENT_I_ID.copy("parentId");

    @Override
    public List<ItemConstructionMaster> selectByParentId(int parentId, LocalDate date) {
        var ps = selectByParentIdCache.get();
        var param = TgParameterList.of(vParentId.bind(parentId), vDate.bind(date));
        return executeAndGetList(ps, param);
    }

    private final CachePreparedQuery<TgParameterList, ItemConstructionMaster> selectByParentIdCache = new CachePreparedQuery<>() {
        @Override
        protected void initialize() {
            this.sql = getSelectEntitySql() + " where ic_parent_i_id = " + vParentId + " and " + TG_COND_DATE + " order by ic_i_id";
            this.parameterMapping = TgParameterMapping.of(vParentId, vDate);
            this.resultMapping = getEntityResultMapping();
        }
    };

    private static final TgVariableInteger vItemId = IC_I_ID.copy("itemId");

    @Override
    public ItemConstructionMaster selectById(int parentId, int itemId, LocalDate date) {
        var ps = selectByIdCache.get();
        var param = TgParameterList.of(vParentId.bind(parentId), vItemId.bind(itemId), vDate.bind(date));
        return executeAndGetRecord(ps, param);
    }

    private final CachePreparedQuery<TgParameterList, ItemConstructionMaster> selectByIdCache = new CachePreparedQuery<>() {
        @Override
        protected void initialize() {
            this.sql = getSelectEntitySql() + " where ic_parent_i_id = " + vParentId + " and ic_i_id = " + vItemId + " and " + TG_COND_DATE;
            this.parameterMapping = TgParameterMapping.of(vParentId, vItemId, vDate);
            this.resultMapping = getEntityResultMapping();
        }
    };

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
//              + " left join " + ItemMasterDao.TABLE_NAME + " i on i_id=ic_i_id and " + ItemMasterDaoIceaxe.TG_COND_DATE //
                + " inner join " + ItemMasterDao.TABLE_NAME + " i on i_id=ic_i_id and " + ItemMasterDaoIceaxe.TG_COND_DATE //
                + " where " + TG_COND_DATE //
                + " and " + inSql //
        ;
        LOG.info("selectByItemType: {}", sql);
        var parameterMapping = TgParameterMapping.of(vlist);
        var resultMapping = TgResultMapping.of(ItemConstructionMasterKey::new) //
                .int4(ItemConstructionMasterKey::setIcParentIId) //
                .int4(ItemConstructionMasterKey::setIcIId) //
                .date(ItemConstructionMasterKey::setIcEffectiveDate);
        var session = getSession();
        try (var ps = session.createPreparedQuery(sql, parameterMapping, resultMapping)) {
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
        var ps = deleteCache.get();
        return executeAndGetCount(ps, key);
    }

    private final CachePreparedStatement<ItemConstructionMasterKey> deleteCache = new CachePreparedStatement<>() {
        @Override
        protected void initialize() {
            this.sql = "delete from " + TABLE_NAME + " where ic_i_id = " + vItemId + " and ic_parent_i_id = " + vParentId + " and ic_effective_date = " + vDate;
            this.parameterMapping = TgParameterMapping.of(ItemConstructionMasterKey.class) //
                    .add(vItemId, ItemConstructionMasterKey::getIcIId) //
                    .add(vParentId, ItemConstructionMasterKey::getIcParentIId) //
                    .add(vDate, ItemConstructionMasterKey::getIcEffectiveDate);
        }
    };

    @Override
    public void forEach(Consumer<ItemConstructionMaster> entityConsumer) {
        doForEach(entityConsumer);
    }
}
