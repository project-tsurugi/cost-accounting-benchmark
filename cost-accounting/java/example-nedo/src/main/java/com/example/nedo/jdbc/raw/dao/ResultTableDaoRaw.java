package com.example.nedo.jdbc.raw.dao;

import static com.example.nedo.jdbc.raw.dao.RawJdbcUtil.*;
import static com.example.nedo.jdbc.raw.dao.RawJdbcUtil.setInt;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.example.nedo.jdbc.doma2.dao.ResultTableDao;
import com.example.nedo.jdbc.doma2.entity.ResultTable;
import com.example.nedo.jdbc.raw.CostBenchDbManagerJdbc;

public class ResultTableDaoRaw extends RawJdbcDao<ResultTable> implements ResultTableDao {

	private static final List<RawJdbcColumn<ResultTable, ?>> COLUMN_LIST;
	static {
		List<RawJdbcColumn<ResultTable, ?>> list = new ArrayList<>();
		add(list, "r_f_id", ResultTable::setRFId, ResultTable::getRFId, RawJdbcUtil::setInt, RawJdbcUtil::getInt);
		add(list, "r_manufacturing_date", ResultTable::setRManufacturingDate, ResultTable::getRManufacturingDate,
				RawJdbcUtil::setDate, RawJdbcUtil::getDate);
		add(list, "r_product_i_id", ResultTable::setRProductIId, ResultTable::getRProductIId, RawJdbcUtil::setInt,
				RawJdbcUtil::getInt);
		add(list, "r_parent_i_id", ResultTable::setRParentIId, ResultTable::getRParentIId, RawJdbcUtil::setInt,
				RawJdbcUtil::getInt);
		add(list, "r_i_id", ResultTable::setRIId, ResultTable::getRIId, RawJdbcUtil::setInt, RawJdbcUtil::getInt);
		add(list, "r_manufacturing_quantity", ResultTable::setRManufacturingQuantity,
				ResultTable::getRManufacturingQuantity, RawJdbcUtil::setBigInt, RawJdbcUtil::getBigInt);
		add(list, "r_weight_unit", ResultTable::setRWeightUnit, ResultTable::getRWeightUnit, RawJdbcUtil::setString,
				RawJdbcUtil::getString);
		add(list, "r_weight", ResultTable::setRWeight, ResultTable::getRWeight, RawJdbcUtil::setDecimal,
				RawJdbcUtil::getDecimal);
		add(list, "r_weight_total_unit", ResultTable::setRWeightTotalUnit, ResultTable::getRWeightTotalUnit,
				RawJdbcUtil::setString, RawJdbcUtil::getString);
		add(list, "r_weight_total", ResultTable::setRWeightTotal, ResultTable::getRWeightTotal, RawJdbcUtil::setDecimal,
				RawJdbcUtil::getDecimal);
		add(list, "r_weight_ratio", ResultTable::setRWeightRatio, ResultTable::getRWeightRatio, RawJdbcUtil::setDecimal,
				RawJdbcUtil::getDecimal);
		add(list, "r_standard_quantity_unit", ResultTable::setRStandardQuantityUnit,
				ResultTable::getRStandardQuantityUnit, RawJdbcUtil::setString, RawJdbcUtil::getString);
		add(list, "r_standard_quantity", ResultTable::setRStandardQuantity, ResultTable::getRStandardQuantity,
				RawJdbcUtil::setDecimal, RawJdbcUtil::getDecimal);
		add(list, "r_required_quantity_unit", ResultTable::setRRequiredQuantityUnit,
				ResultTable::getRRequiredQuantityUnit, RawJdbcUtil::setString, RawJdbcUtil::getString);
		add(list, "r_required_quantity", ResultTable::setRRequiredQuantity, ResultTable::getRRequiredQuantity,
				RawJdbcUtil::setDecimal, RawJdbcUtil::getDecimal);
		add(list, "r_unit_cost", ResultTable::setRUnitCost, ResultTable::getRUnitCost, RawJdbcUtil::setDecimal,
				RawJdbcUtil::getDecimal);
		add(list, "r_total_unit_cost", ResultTable::setRTotalUnitCost, ResultTable::getRTotalUnitCost,
				RawJdbcUtil::setDecimal, RawJdbcUtil::getDecimal);
		add(list, "r_manufacturing_cost", ResultTable::setRManufacturingCost, ResultTable::getRManufacturingCost,
				RawJdbcUtil::setDecimal, RawJdbcUtil::getDecimal);
		add(list, "r_total_manufacturing_cost", ResultTable::setRTotalManufacturingCost,
				ResultTable::getRTotalManufacturingCost, RawJdbcUtil::setDecimal, RawJdbcUtil::getDecimal);
		COLUMN_LIST = list;
	}

	public ResultTableDaoRaw(CostBenchDbManagerJdbc dbManager) {
		super(dbManager, TABLE_NAME, COLUMN_LIST);
	}

	@Override
	public int deleteByFactory(int factoryId, LocalDate date) {
		String sql = "delete from " + TABLE_NAME + " where r_f_id = ? and " + PS_COND_DATE;
		return executeUpdate(sql, ps -> {
			int i = 1;
			setInt(ps, i++, factoryId);
			setDate(ps, i++, date);
		});
	}

	@Override
	public int deleteByFactories(List<Integer> factoryIdList, LocalDate date) {
		String s = Stream.generate(() -> "?").limit(factoryIdList.size()).collect(Collectors.joining(","));
		String sql = "delete from " + TABLE_NAME + " where r_f_id in (" + s + ") and " + PS_COND_DATE;
		return executeUpdate(sql, ps -> {
			int i = 1;
			for (Integer id : factoryIdList) {
				setInt(ps, i++, id);
			}
			setDate(ps, i++, date);
		});
	}

	@Override
	public int deleteByProductId(int factoryId, LocalDate date, int productId) {
		String sql = "delete from " + TABLE_NAME + " where r_f_id = ? and " + PS_COND_DATE + " and r_product_i_id = ?";
		return executeUpdate(sql, ps -> {
			int i = 1;
			setInt(ps, i++, factoryId);
			setDate(ps, i++, date);
			setInt(ps, i++, productId);
		});
	}

	@Override
	public int insert(ResultTable entity) {
		return doInsert(entity);
	}

	@Override
	public int[] insertBatch(Collection<ResultTable> entityList) {
		return doInsert(entityList);
	}

	@Override
	public List<ResultTable> selectByProductId(int factoryId, LocalDate date, int productId) {
		String sql = "select * from " + TABLE_NAME + " where r_f_id = ? and " + PS_COND_DATE //
				+ " and r_product_i_id = ? " //
				+ " order by r_parent_i_id, r_i_id";
		return executeQueryList(sql, ps -> {
			int i = 1;
			setInt(ps, i++, factoryId);
			setDate(ps, i++, date);
			setInt(ps, i++, productId);
		}, this::newEntity);
	}

	@Override
	public Stream<ResultTable> selectRequiredQuantity(int factoryId, LocalDate date) {
		String sql = "select" //
				+ "  r_f_id," //
				+ "  r_manufacturing_date," //
				+ "  r_i_id," //
				+ "  sum(r_required_quantity) r_required_quantity," //
				+ "  max(r_required_quantity_unit) r_required_quantity_unit" //
				+ " from result_table r" //
				+ " left join item_master m on m.i_id=r.r_i_id" //
				+ " where r_f_id=? and r_manufacturing_date=? and m.i_type='raw_material' "
				+ " group by r_f_id, r_manufacturing_date, r_i_id" //
				+ " order by r_i_id";
		return executeQueryStream(sql, ps -> {
			int i = 1;
			setInt(ps, i++, factoryId);
			setDate(ps, i++, date);
		}, rs -> {
			ResultTable entity = new ResultTable();
			entity.setRFId(getInt(rs, "r_f_id"));
			entity.setRManufacturingDate(getDate(rs, "r_manufacturing_date"));
			entity.setRIId(getInt(rs, "r_i_id"));
			entity.setRRequiredQuantity(getDecimal(rs, "r_required_quantity"));
			entity.setRRequiredQuantityUnit(getString(rs, "r_required_quantity_unit"));
			return entity;
		});
	}

	@Override
	public Stream<ResultTable> selectCost(int factoryId, LocalDate date) {
		String sql = "select * from " + TABLE_NAME + " where r_f_id = ? and " + PS_COND_DATE
				+ " and r_i_id = r_product_i_id" //
				+ " order by r_product_i_id";
		return executeQueryStream(sql, ps -> {
			int i = 1;
			setInt(ps, i++, factoryId);
			setDate(ps, i++, date);
		}, this::newEntity);
	}

	private ResultTable newEntity(ResultSet rs) throws SQLException {
		ResultTable entity = new ResultTable();
		fillEntity(entity, rs);
		return entity;
	}
}
