package com.example.nedo.jdbc.raw.dao;

import static com.example.nedo.jdbc.raw.dao.RawJdbcUtil.*;
import static com.example.nedo.jdbc.raw.dao.RawJdbcUtil.setInt;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.example.nedo.jdbc.doma2.dao.ItemConstructionMasterDao;
import com.example.nedo.jdbc.doma2.domain.ItemType;
import com.example.nedo.jdbc.doma2.entity.ItemConstructionMaster;
import com.example.nedo.jdbc.doma2.entity.ItemConstructionMasterIds;
import com.example.nedo.jdbc.doma2.entity.ItemConstructionMasterKey;
import com.example.nedo.jdbc.raw.CostBenchDbManagerJdbc;

public class ItemConstructionMasterDaoRaw extends RawJdbcDao<ItemConstructionMaster>
		implements ItemConstructionMasterDao {

	private static final List<RawJdbcColumn<ItemConstructionMaster, ?>> COLUMN_LIST;
	static {
		List<RawJdbcColumn<ItemConstructionMaster, ?>> list = new ArrayList<>();
		add(list, "ic_parent_i_id", ItemConstructionMaster::setIcParentIId, ItemConstructionMaster::getIcParentIId,
				RawJdbcUtil::setInt, RawJdbcUtil::getInt, true);
		add(list, "ic_i_id", ItemConstructionMaster::setIcIId, ItemConstructionMaster::getIcIId, RawJdbcUtil::setInt,
				RawJdbcUtil::getInt, true);
		add(list, "ic_effective_date", ItemConstructionMaster::setIcEffectiveDate,
				ItemConstructionMaster::getIcEffectiveDate, RawJdbcUtil::setDate, RawJdbcUtil::getDate, true);
		add(list, "ic_expired_date", ItemConstructionMaster::setIcExpiredDate, ItemConstructionMaster::getIcExpiredDate,
				RawJdbcUtil::setDate, RawJdbcUtil::getDate);
		add(list, "ic_material_unit", ItemConstructionMaster::setIcMaterialUnit,
				ItemConstructionMaster::getIcMaterialUnit, RawJdbcUtil::setString, RawJdbcUtil::getString);
		add(list, "ic_material_quantity", ItemConstructionMaster::setIcMaterialQuantity,
				ItemConstructionMaster::getIcMaterialQuantity, RawJdbcUtil::setDecimal, RawJdbcUtil::getDecimal);
		add(list, "ic_loss_ratio", ItemConstructionMaster::setIcLossRatio, ItemConstructionMaster::getIcLossRatio,
				RawJdbcUtil::setDecimal, RawJdbcUtil::getDecimal);
		COLUMN_LIST = list;
	}

	public ItemConstructionMasterDaoRaw(CostBenchDbManagerJdbc dbManager) {
		super(dbManager, TABLE_NAME, COLUMN_LIST);
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
	public List<ItemConstructionMaster> selectAll(LocalDate date) {
		String sql = "select * from " + TABLE_NAME + " where " + PS_COND_DATE + " order by ic_parent_i_id, ic_i_id";
		return executeQueryList(sql, ps -> {
			int i = 1;
			setDate(ps, i++, date);
		}, this::newEntity);
	}

	@Override
	public List<ItemConstructionMaster> selectAll() {
		String sql = "select * from " + TABLE_NAME;
		return executeQueryList(sql, null, this::newEntity);
	}

	@Override
	public List<ItemConstructionMasterIds> selectIds(LocalDate date) {
		throw new InternalError("yet implmented");
	}

	@Override
	public List<ItemConstructionMaster> selectByParentId(int parentId, LocalDate date) {
		String sql = "select * from " + TABLE_NAME + " where ic_parent_i_id = ? and " + PS_COND_DATE
				+ " order by ic_i_id";
		return executeQueryList(sql, ps -> {
			int i = 1;
			setInt(ps, i++, parentId);
			setDate(ps, i++, date);
		}, this::newEntity);
	}

	@Override
	public ItemConstructionMaster selectById(int parentId, int itemId, LocalDate date) {
		String sql = "select * from " + TABLE_NAME + " where ic_parent_i_id = ? and ic_i_id = ? and " + PS_COND_DATE;
		return executeQuery1(sql, ps -> {
			int i = 1;
			setInt(ps, i++, parentId);
			setInt(ps, i++, itemId);
			setDate(ps, i++, date);
		}, this::newEntity);
	}

	@Override
	public Stream<ItemConstructionMaster> selectRecursiveByParentId(int parentId, LocalDate date) {
		throw new InternalError("yet implmented");
	}

	@Override
	public List<ItemConstructionMasterKey> selectByItemType(LocalDate date, List<ItemType> typeList) {
		String s = Stream.generate(() -> "?").limit(typeList.size()).collect(Collectors.joining(","));
		String sql = "select ic_parent_i_id, ic_i_id, ic_effective_date" //
				+ " from item_construction_master ic" //
				+ " left join item_master i on i_id=ic_i_id and ? between i_effective_date and i_expired_date"
				+ " where ? between ic_effective_date and ic_expired_date" //
				+ " and i_type in (" + s + ")";
		return executeQueryList(sql, ps -> {
			int i = 1;
			setDate(ps, i++, date);
			setDate(ps, i++, date);
			for (ItemType type : typeList) {
				setItemType(ps, i++, type);
			}
		}, rs -> {
			ItemConstructionMasterKey key = new ItemConstructionMasterKey();
			key.setIcParentIId(getInt(rs, "ic_parent_i_id"));
			key.setIcIId(getInt(rs, "ic_i_id"));
			key.setIcEffectiveDate(getDate(rs, "ic_effective_date"));
			return key;
		});
	}

	@Override
	public ItemConstructionMaster lock(ItemConstructionMaster in) {
		String sql = "select * from " + TABLE_NAME //
				+ " where ic_i_id=? and ic_parent_i_id=? and ic_effective_date=?" //
				+ " for update";
		return executeQuery1(sql, ps -> {
			int i = 1;
			setInt(ps, i++, in.getIcIId());
			setInt(ps, i++, in.getIcParentIId());
			setDate(ps, i++, in.getIcEffectiveDate());
		}, this::newEntity);
	}

	@Override
	public int delete(ItemConstructionMasterKey key) {
		String sql = "delete from " + TABLE_NAME
				+ " where ic_i_id = ? and ic_parent_i_id = ? and ic_effective_date = ?";
		return executeUpdate(sql, ps -> {
			int i = 1;
			setInt(ps, i++, key.getIcIId());
			setInt(ps, i++, key.getIcParentIId());
			setDate(ps, i++, key.getIcEffectiveDate());
		});
	}

	private ItemConstructionMaster newEntity(ResultSet rs) throws SQLException {
		ItemConstructionMaster entity = new ItemConstructionMaster();
		fillEntity(entity, rs);
		return entity;
	}
}
