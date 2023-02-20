package com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;

import com.tsurugidb.benchmark.costaccounting.db.dao.ItemMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.domain.ItemType;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemMaster;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.CostBenchDbManagerIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain.BenchVariable;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.iceaxe.result.TgResultMapping;
import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariable;
import com.tsurugidb.iceaxe.statement.TgVariable.TgVariableInteger;
import com.tsurugidb.iceaxe.statement.TgVariableList;

public class ItemMasterDaoIceaxe extends IceaxeDao<ItemMaster> implements ItemMasterDao {

    private static final TgVariableInteger I_ID = BenchVariable.ofInt("i_id");
    private static final TgVariable<ItemType> I_TYPE = BenchVariable.ofItemType("i_type");
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
        return doInsert(entity);
    }

    @Override
    public List<ItemMaster> selectByIds(Iterable<Integer> ids, LocalDate date) {
        if (BenchConst.WORKAROUND) {
            return selectByIdsWorkaround(ids, date);
        }

        var vlist = TgVariableList.of();
        var inSql = new SqlIn(I_ID.name());
        var param = TgParameterList.of();
        int i = 0;
        for (int id : ids) {
            var variable = I_ID.copy("id" + (i++));
            vlist.add(variable);
            inSql.add(variable);
            param.add(variable.bind(id));
        }
        vlist.add(vDate);
        param.add(vDate.bind(date));

        var sql = getSelectEntitySql() + " where " + inSql + " and " + TG_COND_DATE + " order by i_id";
        var parameterMapping = TgParameterMapping.of(vlist);
        var resultMapping = getEntityResultMapping();
        var session = getSession();
        try (var ps = session.createPreparedQuery(sql, parameterMapping, resultMapping)) {
            return executeAndGetList(ps, param);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private List<ItemMaster> selectByIdsWorkaround(Iterable<Integer> ids, LocalDate date) {
        var ps = selectByIdsWorkaroundCache.get();

        var result = new ArrayList<ItemMaster>();
        for (int id : ids) {
            var param = TgParameterList.of(vId.bind(id), vDate.bind(date));
            var r = executeAndGetList(ps, param);
            result.addAll(r);
        }
        Collections.sort(result, Comparator.comparing(ItemMaster::getIId));
        return result;
    }

    private final CachePreparedQuery<TgParameterList, ItemMaster> selectByIdsWorkaroundCache = new CachePreparedQuery<>() {
        @Override
        protected void initialize() {
            this.sql = getSelectEntitySql() + " where i_id=" + vId + " and " + TG_COND_DATE;
            this.parameterMapping = TgParameterMapping.of(vId, vDate);
            this.resultMapping = getEntityResultMapping();
        }
    };

    private static final TgVariable<ItemType> vType = I_TYPE.copy("type");

    @Override
    public List<Integer> selectIdByType(LocalDate date, ItemType type) {
        var ps = selectIdByTypeCache.get();
        var param = TgParameterList.of(vDate.bind(date), vType.bind(type));
        return executeAndGetList(ps, param);
    }

    private final CachePreparedQuery<TgParameterList, Integer> selectIdByTypeCache = new CachePreparedQuery<>() {
        @Override
        protected void initialize() {
            this.sql = "select i_id from " + TABLE_NAME + " where " + TG_COND_DATE + " and i_type = " + vType;
            this.parameterMapping = TgParameterMapping.of(vDate, vType);
            this.resultMapping = TgResultMapping.of(record -> record.nextInt4());
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
            this.resultMapping = TgResultMapping.of(record -> record.nextInt4());
        }
    };

    private static final TgVariableInteger vId = I_ID.copy("id");

    @Override
    public ItemMaster selectByKey(int id, LocalDate date) {
        var ps = selectByKeyCache.get();
        var param = TgParameterList.of(vId.bind(id), vDate.bind(date));
        return executeAndGetRecord(ps, param);
    }

    private final CachePreparedQuery<TgParameterList, ItemMaster> selectByKeyCache = new CachePreparedQuery<>() {
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
        var param = TgParameterList.of(vId.bind(id), vDate.bind(date));
        return executeAndGetRecord(ps, param);
    }

    private final CachePreparedQuery<TgParameterList, ItemMaster> selectByIdCache = new CachePreparedQuery<>() {
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
