package com.tsurugidb.benchmark.costaccounting.db.jdbc.dao;

import static com.tsurugidb.benchmark.costaccounting.db.jdbc.dao.JdbcUtil.*;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

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
    public int deleteAll() {
        return doDeleteAll();
    }

    @Override
    public int insert(CostMaster entity) {
        return doInsert(entity);
    }

    @Override
    public List<CostMaster> selectByFactory(int fId) {
        String sql = "select * from " + TABLE_NAME + " where c_f_id = ?";
        return executeQueryList(sql, ps -> {
            int i = 1;
            setInt(ps, i++, fId);
        }, this::newEntity);
    }

    @Override
    public CostMaster selectById(int fId, int iId) {
        String sql = "select * from " + TABLE_NAME + " where c_f_id = ? and c_i_id = ?";
        return executeQuery1(sql, ps -> {
            int i = 1;
            setInt(ps, i++, fId);
            setInt(ps, i++, iId);
        }, this::newEntity);
    }

    @Override
    public CostMaster lock(CostMaster in) {
        String sql = "select * from " + TABLE_NAME + " where c_f_id = ? and c_i_id = ? for update";
        return executeQuery1(sql, ps -> {
            int i = 1;
            setInt(ps, i++, in.getCFId());
            setInt(ps, i++, in.getCIId());
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

    private CostMaster newEntity(ResultSet rs) throws SQLException {
        CostMaster entity = new CostMaster();
        fillEntity(entity, rs);
        return entity;
    }
}
