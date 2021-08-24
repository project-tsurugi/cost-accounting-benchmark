package com.example.nedo.jdbc.raw.dao;

import static com.example.nedo.jdbc.raw.dao.RawJdbcUtil.*;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.example.nedo.jdbc.doma2.dao.CostMasterDao;
import com.example.nedo.jdbc.doma2.entity.CostMaster;
import com.example.nedo.jdbc.raw.CostBenchDbManagerJdbc;

public class CostMasterDaoRaw extends RawJdbcDao<CostMaster> implements CostMasterDao {

	private static final List<RawJdbcColumn<CostMaster, ?>> COLUMN_LIST;
	static {
		List<RawJdbcColumn<CostMaster, ?>> list = new ArrayList<>();
		add(list, "c_f_id", CostMaster::setCFId, CostMaster::getCFId, RawJdbcUtil::setInt, RawJdbcUtil::getInt, true);
		add(list, "c_i_id", CostMaster::setCIId, CostMaster::getCIId, RawJdbcUtil::setInt, RawJdbcUtil::getInt, true);
		add(list, "c_stock_unit", CostMaster::setCStockUnit, CostMaster::getCStockUnit, RawJdbcUtil::setString,
				RawJdbcUtil::getString);
		add(list, "c_stock_quantity", CostMaster::setCStockQuantity, CostMaster::getCStockQuantity,
				RawJdbcUtil::setDecimal, RawJdbcUtil::getDecimal);
		add(list, "c_stock_amount", CostMaster::setCStockAmount, CostMaster::getCStockAmount, RawJdbcUtil::setDecimal,
				RawJdbcUtil::getDecimal);
		COLUMN_LIST = list;
	}

	public CostMasterDaoRaw(CostBenchDbManagerJdbc dbManager) {
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
