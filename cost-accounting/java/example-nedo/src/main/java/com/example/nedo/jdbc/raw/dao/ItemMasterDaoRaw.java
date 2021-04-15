package com.example.nedo.jdbc.raw.dao;

import static com.example.nedo.jdbc.raw.dao.RawJdbcUtil.getInt;
import static com.example.nedo.jdbc.raw.dao.RawJdbcUtil.setDate;
import static com.example.nedo.jdbc.raw.dao.RawJdbcUtil.setInt;
import static com.example.nedo.jdbc.raw.dao.RawJdbcUtil.setItemType;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import com.example.nedo.jdbc.doma2.dao.ItemMasterDao;
import com.example.nedo.jdbc.doma2.domain.ItemType;
import com.example.nedo.jdbc.doma2.entity.ItemMaster;
import com.example.nedo.jdbc.raw.CostBenchDbManagerJdbc;

public class ItemMasterDaoRaw extends RawJdbcDao<ItemMaster> implements ItemMasterDao {

	private static final List<RawJdbcColumn<ItemMaster, ?>> COLUMN_LIST;
	static {
		List<RawJdbcColumn<ItemMaster, ?>> list = new ArrayList<>();
		add(list, "i_id", ItemMaster::setIId, ItemMaster::getIId, RawJdbcUtil::setInt, RawJdbcUtil::getInt, true);
		add(list, "i_effective_date", ItemMaster::setIEffectiveDate, ItemMaster::getIEffectiveDate,
				RawJdbcUtil::setDate, RawJdbcUtil::getDate, true);
		add(list, "i_expired_date", ItemMaster::setIExpiredDate, ItemMaster::getIExpiredDate, RawJdbcUtil::setDate,
				RawJdbcUtil::getDate);
		add(list, "i_name", ItemMaster::setIName, ItemMaster::getIName, RawJdbcUtil::setString, RawJdbcUtil::getString);
		add(list, "i_type", ItemMaster::setIType, ItemMaster::getIType, RawJdbcUtil::setItemType,
				RawJdbcUtil::getItemType);
		add(list, "i_unit", ItemMaster::setIUnit, ItemMaster::getIUnit, RawJdbcUtil::setString, RawJdbcUtil::getString);
		add(list, "i_weight_ratio", ItemMaster::setIWeightRatio, ItemMaster::getIWeightRatio, RawJdbcUtil::setDecimal,
				RawJdbcUtil::getDecimal);
		add(list, "i_weight_unit", ItemMaster::setIWeightUnit, ItemMaster::getIWeightUnit, RawJdbcUtil::setString,
				RawJdbcUtil::getString);
		add(list, "i_price", ItemMaster::setIPrice, ItemMaster::getIPrice, RawJdbcUtil::setDecimal,
				RawJdbcUtil::getDecimal);
		add(list, "i_price_unit", ItemMaster::setIPriceUnit, ItemMaster::getIPriceUnit, RawJdbcUtil::setString,
				RawJdbcUtil::getString);
		COLUMN_LIST = list;
	}

	public ItemMasterDaoRaw(CostBenchDbManagerJdbc dbManager) {
		super(dbManager, TABLE_NAME, COLUMN_LIST);
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
		StringBuilder sb = new StringBuilder();
		for (@SuppressWarnings("unused")
		int id : ids) {
			if (sb.length() != 0) {
				sb.append(',');
			}
			sb.append('?');
		}
		String sql = "select * from " + TABLE_NAME + " where i_id in (" + sb + ") and " + PS_COND_DATE;
		return executeQueryList(sql, ps -> {
			int i = 1;
			for (int id : ids) {
				setInt(ps, i++, id);
			}
			setDate(ps, i++, date);
		}, this::newEntity);
	}

	@Override
	public List<Integer> selectIdByType(LocalDate date, ItemType type) {
		String sql = "select i_id from " + TABLE_NAME + " where " + PS_COND_DATE + " and i_type = ?";
		return executeQueryList(sql, ps -> {
			int i = 1;
			setDate(ps, i++, date);
			setItemType(ps, i++, type);
		}, rs -> getInt(rs, "i_id"));
	}

	@Override
	public List<ItemMaster> selectByType(LocalDate date, ItemType type) {
		String sql = "select * from " + TABLE_NAME + " where " + PS_COND_DATE + " and i_type = ?";
		return executeQueryList(sql, ps -> {
			int i = 1;
			setDate(ps, i++, date);
			setItemType(ps, i++, type);
		}, this::newEntity);
	}

	@Override
	public List<ItemMaster> selectAll() {
		String sql = "select * from " + TABLE_NAME;
		return executeQueryList(sql, null, this::newEntity);
	}

	@Override
	public Integer selectMaxId() {
		String sql = "select max(i_id) + 1 from " + TABLE_NAME;
		return executeQuery1(sql, null, rs -> rs.getInt(1));
	}

	@Override
	public ItemMaster selectById(int id, LocalDate date) {
		String sql = "select * from " + TABLE_NAME + " where i_id = ? and " + PS_COND_DATE;
		return executeQuery1(sql, ps -> {
			int i = 1;
			setInt(ps, i++, id);
			setDate(ps, i++, date);
		}, this::newEntity);
	}

	private ItemMaster newEntity(ResultSet rs) throws SQLException {
		ItemMaster entity = new ItemMaster();
		fillEntity(entity, rs);
		return entity;
	}
}
