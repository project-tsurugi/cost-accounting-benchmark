package com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import com.tsurugidb.benchmark.costaccounting.db.dao.FactoryMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.FactoryMaster;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.CostBenchDbManagerIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain.BenchVariable;
import com.tsurugidb.iceaxe.result.TgResultMapping;
import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariable;

public class FactoryMasterDaoIceaxe extends IceaxeDao<FactoryMaster> implements FactoryMasterDao {

    private static final List<IceaxeColumn<FactoryMaster, ?>> COLUMN_LIST;
    static {
        List<IceaxeColumn<FactoryMaster, ?>> list = new ArrayList<>();
        add(list, BenchVariable.ofInt("f_id"), FactoryMaster::setFId, FactoryMaster::getFId, IceaxeRecordUtil::getInt, true);
        add(list, BenchVariable.ofString("f_name"), FactoryMaster::setFName, FactoryMaster::getFName, IceaxeRecordUtil::getString);
        COLUMN_LIST = list;
    }

    public FactoryMasterDaoIceaxe(CostBenchDbManagerIceaxe dbManager) {
        super(dbManager, TABLE_NAME, COLUMN_LIST, FactoryMaster::new);
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
    public int insert(FactoryMaster entity) {
        return doInsert(entity);
    }

    @Override
    public List<Integer> selectAllId() {
        var ps = selectAllIdCache.get();
        return executeAndGetList(ps);
    }

    private final CacheQuery<Integer> selectAllIdCache = new CacheQuery<>() {
        @Override
        protected void initialize() {
            this.sql = "select f_id from " + TABLE_NAME;
            this.resultMapping = TgResultMapping.of(record -> record.nextInt4OrNull());
        }
    };

    private final TgVariable<Integer> vFactoryId = BenchVariable.ofInt("factoryId");

    @Override
    public FactoryMaster selectById(int factoryId) {
        var ps = selectByIdCache.get();
        var param = TgParameterList.of(vFactoryId.bind(factoryId));
        return executeAndGetRecord(ps, param);
    }

    private final CachePreparedQuery<TgParameterList, FactoryMaster> selectByIdCache = new CachePreparedQuery<>() {
        @Override
        protected void initialize() {
            this.sql = getSelectEntitySql() + " where f_id = " + vFactoryId;
            this.parameterMapping = TgParameterMapping.of(vFactoryId);
            this.resultMapping = getEntityResultMapping();
        }
    };

    @Override
    public void forEach(Consumer<FactoryMaster> entityConsumer) {
        doForEach(entityConsumer);
    }
}
