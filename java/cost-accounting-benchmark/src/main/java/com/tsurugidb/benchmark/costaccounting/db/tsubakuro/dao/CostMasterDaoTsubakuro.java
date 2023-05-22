package com.tsurugidb.benchmark.costaccounting.db.tsubakuro.dao;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.tsurugidb.benchmark.costaccounting.db.dao.CostMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.CostMaster;
import com.tsurugidb.benchmark.costaccounting.db.tsubakuro.CostBenchDbManagerTsubakuro;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;
import com.tsurugidb.tsubakuro.sql.Parameters;
import com.tsurugidb.tsubakuro.sql.Placeholders;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;

public class CostMasterDaoTsubakuro extends TsubakuroDao<CostMaster> implements CostMasterDao {

    private static final List<TsubakuroColumn<CostMaster, ?>> COLUMN_LIST;
    static {
        List<TsubakuroColumn<CostMaster, ?>> list = new ArrayList<>();
        add(list, "c_f_id", AtomType.INT4, CostMaster::setCFId, CostMaster::getCFId, TsubakuroUtil::getParameter, TsubakuroUtil::getInt, true);
        add(list, "c_i_id", AtomType.INT4, CostMaster::setCIId, CostMaster::getCIId, TsubakuroUtil::getParameter, TsubakuroUtil::getInt, true);
        add(list, "c_stock_unit", AtomType.CHARACTER, CostMaster::setCStockUnit, CostMaster::getCStockUnit, TsubakuroUtil::getParameter, TsubakuroUtil::getString);
        add(list, "c_stock_quantity", AtomType.DECIMAL, CostMaster::setCStockQuantity, CostMaster::getCStockQuantity,
                (name, value) -> TsubakuroUtil.getParameter(name, value, CostMaster.C_STOCK_QUANTITY_SCALE), TsubakuroUtil::getDecimal);
        add(list, "c_stock_amount", AtomType.DECIMAL, CostMaster::setCStockAmount, CostMaster::getCStockAmount,
                (name, value) -> TsubakuroUtil.getParameter(name, value, CostMaster.C_STOCK_AMOUNT_SCALE), TsubakuroUtil::getDecimal);
        COLUMN_LIST = list;
    }

    public CostMasterDaoTsubakuro(CostBenchDbManagerTsubakuro dbManager) {
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
        return doInsert(entity);
    }

    @Override
    public int[] insertBatch(Collection<CostMaster> entityList) {
        return doInsert(entityList);
    }

    @Override
    public Stream<CostMaster> selectAll() {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public Stream<CostMaster> selectByFactory(int fId) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public List<Integer> selectIdByFactory(int fId) {
        var ps = getSelectIdByFactoryPs();
        var parameters = List.of(Parameters.of("fId", fId));
        explain(selectIdByFactorySql, ps, parameters);
        return executeAndGetList(ps, parameters, rs -> {
            if (rs.nextColumn()) {
                return rs.fetchInt4Value();
            }
            throw new AssertionError();
        });
    }

    private String selectIdByFactorySql;
    private PreparedStatement selectIdByFactoryPs;

    private synchronized PreparedStatement getSelectIdByFactoryPs() {
        if (this.selectIdByFactoryPs == null) {
            this.selectIdByFactorySql = "select c_i_id from " + TABLE_NAME + " where c_f_id = :fId order by c_i_id";
            var placeholders = List.of(Placeholders.of("fId", AtomType.INT4));
            this.selectIdByFactoryPs = createPreparedStatement(selectIdByFactorySql, placeholders);
        }
        return this.selectIdByFactoryPs;
    }

    @Override
    public CostMaster selectById(int fId, int iId, boolean forUpdate) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public int updateIncrease(CostMaster entity, BigDecimal quantity, BigDecimal amount) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public int updateDecrease(CostMaster entity, BigDecimal quantity) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public int updateZero(CostMaster entity) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public void forEach(Consumer<CostMaster> entityConsumer) {
        throw new UnsupportedOperationException("not yet impl");
    }
}
