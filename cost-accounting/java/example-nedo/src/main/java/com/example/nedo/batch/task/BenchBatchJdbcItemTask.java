package com.example.nedo.batch.task;

import static com.example.nedo.batch.task.JdbcTaskUtil.getDate;
import static com.example.nedo.batch.task.JdbcTaskUtil.getDecimal;
import static com.example.nedo.batch.task.JdbcTaskUtil.getInt;
import static com.example.nedo.batch.task.JdbcTaskUtil.getItemType;
import static com.example.nedo.batch.task.JdbcTaskUtil.getString;
import static com.example.nedo.batch.task.JdbcTaskUtil.setBigInt;
import static com.example.nedo.batch.task.JdbcTaskUtil.setDate;
import static com.example.nedo.batch.task.JdbcTaskUtil.setDecimal;
import static com.example.nedo.batch.task.JdbcTaskUtil.setInt;
import static com.example.nedo.batch.task.JdbcTaskUtil.setString;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.example.nedo.jdbc.doma2.dao.CostMasterDao;
import com.example.nedo.jdbc.doma2.dao.ItemConstructionMasterDao;
import com.example.nedo.jdbc.doma2.dao.ItemMasterDao;
import com.example.nedo.jdbc.doma2.dao.ResultTableDao;
import com.example.nedo.jdbc.doma2.entity.CostMaster;
import com.example.nedo.jdbc.doma2.entity.ItemConstructionMaster;
import com.example.nedo.jdbc.doma2.entity.ItemMaster;
import com.example.nedo.jdbc.doma2.entity.ResultTable;

public class BenchBatchJdbcItemTask extends BenchBatchItemTask {

	private Connection connection;

	public BenchBatchJdbcItemTask(LocalDate batchDate) {
		super(batchDate);
	}

	public void setConnection(Connection connection) {
		this.connection = connection;
	}

	@Override
	protected List<ItemConstructionMaster> selectItemConstructionMaster(int parentItemId, LocalDate batchDate) {
		// return itemConstructionMasterDao.selectByParentId(parentItemId, batchDate);
		String sql = "select * from " + ItemConstructionMasterDao.TABLE_NAME + " where ic_parent_i_id = ? and "
				+ ItemConstructionMasterDao.PS_COND_DATE + " order by ic_i_id";
		try (PreparedStatement ps = connection.prepareStatement(sql)) {
			int i = 1;
			setInt(ps, i++, parentItemId);
			setDate(ps, i++, batchDate);

			try (ResultSet rs = ps.executeQuery()) {
				List<ItemConstructionMaster> list = new ArrayList<>();
				while (rs.next()) {
					ItemConstructionMaster entity = newItemConstructionMaster(rs);
					list.add(entity);
				}
				return list;
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private ItemConstructionMaster newItemConstructionMaster(ResultSet rs) throws SQLException {
		ItemConstructionMaster entity = new ItemConstructionMaster();

		entity.setIcParentIId(getInt(rs, "ic_parent_i_id"));
		entity.setIcIId(getInt(rs, "ic_i_id"));
		entity.setIcEffectiveDate(getDate(rs, "ic_effective_date"));
		entity.setIcExpiredDate(getDate(rs, "ic_expired_date"));
		entity.setIcMaterialUnit(getString(rs, "ic_material_unit"));
		entity.setIcMaterialQuantity(getDecimal(rs, "ic_material_quantity"));
		entity.setIcLossRatio(getDecimal(rs, "ic_loss_ratio"));

		return entity;
	}

	@Override
	protected Stream<ItemConstructionMaster> selectItemConstructionMasterRecursive(int parentItemId,
			LocalDate batchDate) {
		// return itemConstructionMasterDao.selectRecursiveByParentId(parentItemId,
		// batchDate);
		throw new InternalError("yet not implemented");
	}

	@Override
	protected ItemMaster selectItemMaster(int itemId) {
		// return itemMasterDao.selectById(itemId, batchDate);
		String sql = "select * from " + ItemMasterDao.TABLE_NAME + " where i_id = ? and " + ItemMasterDao.PS_COND_DATE;
		try (PreparedStatement ps = connection.prepareStatement(sql)) {
			int i = 1;
			setInt(ps, i++, itemId);
			setDate(ps, i++, batchDate);

			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					ItemMaster entity = new ItemMaster();
					entity.setIId(getInt(rs, "i_id"));
					entity.setIEffectiveDate(getDate(rs, "i_effective_date"));
					entity.setIExpiredDate(getDate(rs, "i_expired_date"));
					entity.setIName(getString(rs, "i_name"));
					entity.setIType(getItemType(rs, "i_type"));
					entity.setIUnit(getString(rs, "i_unit"));
					entity.setIWeightRatio(getDecimal(rs, "i_weight_ratio"));
					entity.setIWeightUnit(getString(rs, "i_weight_unit"));
					entity.setIPrice(getDecimal(rs, "i_price"));
					entity.setIPriceUnit(getString(rs, "i_price_unit"));
					return entity;
				}
				return null;
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected CostMaster selectCostMaster(int factoryId, int itemId) {
		// return costMasterDao.selectById(factoryId, itemId);
		String sql = "select * from " + CostMasterDao.TABLE_NAME + " where c_f_id = ? and c_i_id=?";
		try (PreparedStatement ps = connection.prepareStatement(sql)) {
			int i = 1;
			ps.setInt(i++, factoryId);
			ps.setInt(i++, itemId);

			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					CostMaster entity = new CostMaster();
					entity.setCFId(getInt(rs, "c_f_id"));
					entity.setCIId(getInt(rs, "c_i_id"));
					entity.setCStockUnit(getString(rs, "c_stock_unit"));
					entity.setCStockQuantity(getDecimal(rs, "c_stock_quantity"));
					entity.setCStockAmount(getDecimal(rs, "c_stock_amount"));
					return entity;
				}
				return null;
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private static final String SQL_ResultTable_insert;
	static {
		List<String> list = Arrays.asList("r_f_id", "r_manufacturing_date", "r_product_i_id", "r_parent_i_id", "r_i_id",
				"r_manufacturing_quantity", //
				"r_weight_unit", "r_weight", "r_weight_total_unit", "r_weight_total", "r_weight_ratio", //
				"r_standard_quantity_unit", "r_standard_quantity", "r_required_quantity_unit", "r_required_quantity", //
				"r_unit_cost", "r_total_unit_cost", "r_manufacturing_cost", "r_total_manufacturing_cost");
		String sql = "insert into " + ResultTableDao.TABLE_NAME + "(" + String.join(",", list) + ") values ("
				+ Stream.generate(() -> "?").limit(list.size()).collect(Collectors.joining(",")) + ")";
		SQL_ResultTable_insert = sql;
	}

	@Override
	protected void insertResultTable(ResultTable entity) {
		// resultTableDao.insert(entity);
		try (PreparedStatement ps = connection.prepareStatement(SQL_ResultTable_insert)) {
			setPsResultTable(ps, entity);
			ps.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private void setPsResultTable(PreparedStatement ps, ResultTable entity) throws SQLException {
		int i = 1;
		setInt(ps, i++, entity.getRFId());
		setDate(ps, i++, entity.getRManufacturingDate());
		setInt(ps, i++, entity.getRProductIId());
		setInt(ps, i++, entity.getRParentIId());
		setInt(ps, i++, entity.getRIId());
		setBigInt(ps, i++, entity.getRManufacturingQuantity());
		setString(ps, i++, entity.getRWeightUnit());
		setDecimal(ps, i++, entity.getRWeight());
		setString(ps, i++, entity.getRWeightTotalUnit());
		setDecimal(ps, i++, entity.getRWeightTotal());
		setDecimal(ps, i++, entity.getRWeightRatio());
		setString(ps, i++, entity.getRStandardQuantityUnit());
		setDecimal(ps, i++, entity.getRStandardQuantity());
		setString(ps, i++, entity.getRRequiredQuantityUnit());
		setDecimal(ps, i++, entity.getRRequiredQuantity());
		setDecimal(ps, i++, entity.getRUnitCost());
		setDecimal(ps, i++, entity.getRTotalUnitCost());
		setDecimal(ps, i++, entity.getRManufacturingCost());
		setDecimal(ps, i++, entity.getRTotalManufacturingCost());
	}

	@Override
	protected void insertResultTable(Collection<ResultTable> list) {
		// resultTableDao.insertBatch(list);
		try (PreparedStatement ps = connection.prepareStatement(SQL_ResultTable_insert)) {
			for (ResultTable entity : list) {
				setPsResultTable(ps, entity);
				ps.addBatch();
			}
			ps.executeBatch();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}
