package com.tsurugidb.benchmark.costaccounting.db.jdbc.dao;

import static com.tsurugidb.benchmark.costaccounting.db.jdbc.dao.JdbcUtil.setDecimal;
import static com.tsurugidb.benchmark.costaccounting.db.jdbc.dao.JdbcUtil.setInt;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.tsurugidb.benchmark.costaccounting.db.dao.CostMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.CostMaster;
import com.tsurugidb.benchmark.costaccounting.db.jdbc.CostBenchDbManagerJdbc;

public class CostMasterDaoJdbc extends JdbcDao<CostMaster> implements CostMasterDao {

    private static final List<JdbcColumn<CostMaster, ?>> COLUMN_LIST;
    static {
        List<JdbcColumn<CostMaster, ?>> list = new ArrayList<>();
        add(list, "c_f_id", CostMaster::setCFId, CostMaster::getCFId, JdbcUtil::setInt, JdbcUtil::getInt, true);
        add(list, "c_i_id", CostMaster::setCIId, CostMaster::getCIId, JdbcUtil::setInt, JdbcUtil::getInt, true);
        add(list, "c_stock_unit", CostMaster::setCStockUnit, CostMaster::getCStockUnit, JdbcUtil::setString, JdbcUtil::getString);
        add(list, "c_stock_quantity", CostMaster::setCStockQuantity, CostMaster::getCStockQuantity, JdbcUtil::setDecimal, JdbcUtil::getDecimal);
        add(list, "c_stock_amount", CostMaster::setCStockAmount, CostMaster::getCStockAmount, JdbcUtil::setDecimal, JdbcUtil::getDecimal);
        COLUMN_LIST = list;
    }

    public CostMasterDaoJdbc(CostBenchDbManagerJdbc dbManager) {
        super(dbManager, TABLE_NAME, COLUMN_LIST);
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
    public Stream<CostMaster> selectAll() {
        String sql = "select * from " + TABLE_NAME;
        return executeQueryStream(sql, null, this::newEntity);
    }

    @Override
    public Stream<CostMaster> selectByFactory(int fId) {
        String sql = "select * from " + TABLE_NAME + " where c_f_id = ?";
        return executeQueryStream(sql, ps -> {
            int i = 1;
            setInt(ps, i++, fId);
        }, this::newEntity);
    }

    @Override
    public List<Integer> selectIdByFactory(int fId) {
        String sql = "select c_i_id from " + TABLE_NAME + " where c_f_id = ? order by c_i_id";
        return executeQueryList(sql, ps -> {
            int i = 1;
            setInt(ps, i++, fId);
        }, rs -> rs.getInt("c_i_id"));
    }

    @Override
    public CostMaster selectById(int fId, int iId, boolean forUpdate) {
        String sql = "select * from " + TABLE_NAME + " where c_f_id = ? and c_i_id = ?";
        if (forUpdate) {
            sql += " for update";
        }
        return executeQuery1(sql, ps -> {
            int i = 1;
            setInt(ps, i++, fId);
            setInt(ps, i++, iId);
        }, this::newEntity);
    }

    @Override
    public int updateIncrease(CostMaster entity, BigDecimal quantity, BigDecimal amount) {
        String sql = "update " + TABLE_NAME + " set" //
                + " c_stock_quantity = c_stock_quantity + ?" //
                + ",c_stock_amount = c_stock_amount + ?" //
                + " where c_f_id=? and c_i_id=?";
        return executeUpdate(sql, ps -> {
            int i = 1;
            setDecimal(ps, i++, quantity);
            setDecimal(ps, i++, amount);
            setInt(ps, i++, entity.getCFId());
            setInt(ps, i++, entity.getCIId());
        });
    }

    @Override
    public int updateDecrease(CostMaster entity, BigDecimal quantity) {
        String sql = "update " + TABLE_NAME + " set" //
                + " c_stock_quantity = c_stock_quantity - ?" //
                + ",c_stock_amount = c_stock_amount - c_stock_amount * ? / c_stock_quantity" //
                + " where c_f_id=? and c_i_id=?";
        return executeUpdate(sql, ps -> {
            int i = 1;
            setDecimal(ps, i++, quantity);
            setDecimal(ps, i++, quantity);
            setInt(ps, i++, entity.getCFId());
            setInt(ps, i++, entity.getCIId());
        });
    }

    @Override
    public int updateZero(CostMaster entity) {
        String sql = "update " + TABLE_NAME + " set" //
                + " c_stock_quantity = 0" //
                + ",c_stock_amount = 0" //
                + " where c_f_id=? and c_i_id=?";
        return executeUpdate(sql, ps -> {
            int i = 1;
            setInt(ps, i++, entity.getCFId());
            setInt(ps, i++, entity.getCIId());
        });
    }

    private CostMaster newEntity(ResultSet rs) throws SQLException {
        CostMaster entity = new CostMaster();
        fillEntity(entity, rs);
        return entity;
    }

    @Override
    public void forEach(Consumer<CostMaster> entityConsumer) {
        doForEach(CostMaster::new, entityConsumer);
    }
}
