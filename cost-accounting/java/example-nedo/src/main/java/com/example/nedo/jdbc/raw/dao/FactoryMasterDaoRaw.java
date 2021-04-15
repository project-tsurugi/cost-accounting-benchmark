package com.example.nedo.jdbc.raw.dao;

import static com.example.nedo.jdbc.raw.dao.RawJdbcUtil.getInt;
import static com.example.nedo.jdbc.raw.dao.RawJdbcUtil.setInt;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.example.nedo.jdbc.doma2.dao.FactoryMasterDao;
import com.example.nedo.jdbc.doma2.entity.FactoryMaster;
import com.example.nedo.jdbc.raw.CostBenchDbManagerJdbc;

public class FactoryMasterDaoRaw extends RawJdbcDao<FactoryMaster> implements FactoryMasterDao {

	private static final List<RawJdbcColumn<FactoryMaster, ?>> COLUMN_LIST;
	static {
		List<RawJdbcColumn<FactoryMaster, ?>> list = new ArrayList<>();
		add(list, "f_id", FactoryMaster::setFId, FactoryMaster::getFId, RawJdbcUtil::setInt, RawJdbcUtil::getInt, true);
		add(list, "f_name", FactoryMaster::setFName, FactoryMaster::getFName, RawJdbcUtil::setString,
				RawJdbcUtil::getString);
		COLUMN_LIST = list;
	}

	public FactoryMasterDaoRaw(CostBenchDbManagerJdbc dbManager) {
		super(dbManager, TABLE_NAME, COLUMN_LIST);
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
		String sql = "select f_id from " + TABLE_NAME;
		return executeQueryList(sql, null, rs -> getInt(rs, "f_id"));
	}

	@Override
	public FactoryMaster selectById(int factoryId) {
		String sql = "select * from " + TABLE_NAME + " where f_id = ?";
		return executeQuery1(sql, ps -> {
			int i = 1;
			setInt(ps, i++, factoryId);
		}, this::newEntity);
	}

	private FactoryMaster newEntity(ResultSet rs) throws SQLException {
		FactoryMaster entity = new FactoryMaster();
		fillEntity(entity, rs);
		return entity;
	}
}
