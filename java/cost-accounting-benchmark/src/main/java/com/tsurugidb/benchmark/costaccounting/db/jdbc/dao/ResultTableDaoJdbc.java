package com.tsurugidb.benchmark.costaccounting.db.jdbc.dao;

import static com.tsurugidb.benchmark.costaccounting.db.jdbc.dao.JdbcUtil.getDate;
import static com.tsurugidb.benchmark.costaccounting.db.jdbc.dao.JdbcUtil.getDecimal;
import static com.tsurugidb.benchmark.costaccounting.db.jdbc.dao.JdbcUtil.getInt;
import static com.tsurugidb.benchmark.costaccounting.db.jdbc.dao.JdbcUtil.getString;
import static com.tsurugidb.benchmark.costaccounting.db.jdbc.dao.JdbcUtil.setDate;
import static com.tsurugidb.benchmark.costaccounting.db.jdbc.dao.JdbcUtil.setInt;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.tsurugidb.benchmark.costaccounting.db.dao.ResultTableDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.ResultTable;
import com.tsurugidb.benchmark.costaccounting.db.jdbc.CostBenchDbManagerJdbc;

public class ResultTableDaoJdbc extends JdbcDao<ResultTable> implements ResultTableDao {

    private static final List<JdbcColumn<ResultTable, ?>> COLUMN_LIST;
    static {
        List<JdbcColumn<ResultTable, ?>> list = new ArrayList<>();
        add(list, "r_f_id", ResultTable::setRFId, ResultTable::getRFId, JdbcUtil::setInt, JdbcUtil::getInt, true);
        add(list, "r_manufacturing_date", ResultTable::setRManufacturingDate, ResultTable::getRManufacturingDate, JdbcUtil::setDate, JdbcUtil::getDate, true);
        add(list, "r_product_i_id", ResultTable::setRProductIId, ResultTable::getRProductIId, JdbcUtil::setInt, JdbcUtil::getInt, true);
        add(list, "r_parent_i_id", ResultTable::setRParentIId, ResultTable::getRParentIId, JdbcUtil::setInt, JdbcUtil::getInt, true);
        add(list, "r_i_id", ResultTable::setRIId, ResultTable::getRIId, JdbcUtil::setInt, JdbcUtil::getInt, true);
        add(list, "r_manufacturing_quantity", ResultTable::setRManufacturingQuantity, ResultTable::getRManufacturingQuantity, JdbcUtil::setBigInt, JdbcUtil::getBigInt);
        add(list, "r_weight_unit", ResultTable::setRWeightUnit, ResultTable::getRWeightUnit, JdbcUtil::setString, JdbcUtil::getString);
        add(list, "r_weight", ResultTable::setRWeight, ResultTable::getRWeight, JdbcUtil::setDecimal, JdbcUtil::getDecimal);
        add(list, "r_weight_total_unit", ResultTable::setRWeightTotalUnit, ResultTable::getRWeightTotalUnit, JdbcUtil::setString, JdbcUtil::getString);
        add(list, "r_weight_total", ResultTable::setRWeightTotal, ResultTable::getRWeightTotal, JdbcUtil::setDecimal, JdbcUtil::getDecimal);
        add(list, "r_weight_ratio", ResultTable::setRWeightRatio, ResultTable::getRWeightRatio, JdbcUtil::setDecimal, JdbcUtil::getDecimal);
        add(list, "r_standard_quantity_unit", ResultTable::setRStandardQuantityUnit, ResultTable::getRStandardQuantityUnit, JdbcUtil::setString, JdbcUtil::getString);
        add(list, "r_standard_quantity", ResultTable::setRStandardQuantity, ResultTable::getRStandardQuantity, JdbcUtil::setDecimal, JdbcUtil::getDecimal);
        add(list, "r_required_quantity_unit", ResultTable::setRRequiredQuantityUnit, ResultTable::getRRequiredQuantityUnit, JdbcUtil::setString, JdbcUtil::getString);
        add(list, "r_required_quantity", ResultTable::setRRequiredQuantity, ResultTable::getRRequiredQuantity, JdbcUtil::setDecimal, JdbcUtil::getDecimal);
        add(list, "r_unit_cost", ResultTable::setRUnitCost, ResultTable::getRUnitCost, JdbcUtil::setDecimal, JdbcUtil::getDecimal);
        add(list, "r_total_unit_cost", ResultTable::setRTotalUnitCost, ResultTable::getRTotalUnitCost, JdbcUtil::setDecimal, JdbcUtil::getDecimal);
        add(list, "r_manufacturing_cost", ResultTable::setRManufacturingCost, ResultTable::getRManufacturingCost, JdbcUtil::setDecimal, JdbcUtil::getDecimal);
        add(list, "r_total_manufacturing_cost", ResultTable::setRTotalManufacturingCost, ResultTable::getRTotalManufacturingCost, JdbcUtil::setDecimal, JdbcUtil::getDecimal);
        COLUMN_LIST = list;
    }

    public ResultTableDaoJdbc(CostBenchDbManagerJdbc dbManager) {
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
                + " left join item_master m on m.i_id=r.r_i_id and r.r_manufacturing_date between m.i_effective_date and m.i_expired_date" //
                + " where r_f_id=? and r_manufacturing_date=? and m.i_type='raw_material' " + " group by r_f_id, r_manufacturing_date, r_i_id" //
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
        String sql = "select * from " + TABLE_NAME + " where r_f_id = ? and " + PS_COND_DATE + " and r_i_id = r_product_i_id" //
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

    @Override
    public void forEach(Consumer<ResultTable> entityConsumer) {
        doForEach(ResultTable::new, entityConsumer);
    }
}
