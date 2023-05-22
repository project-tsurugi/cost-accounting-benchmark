package com.tsurugidb.benchmark.costaccounting.db.jdbc.dao;

import static com.tsurugidb.benchmark.costaccounting.db.jdbc.dao.JdbcUtil.getInt;
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

import com.tsurugidb.benchmark.costaccounting.db.dao.ItemManufacturingMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemManufacturingMaster;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemManufacturingMasterIds;
import com.tsurugidb.benchmark.costaccounting.db.jdbc.CostBenchDbManagerJdbc;

public class ItemManufacturingMasterDaoJdbc extends JdbcDao<ItemManufacturingMaster> implements ItemManufacturingMasterDao {

    private static final List<JdbcColumn<ItemManufacturingMaster, ?>> COLUMN_LIST;
    static {
        List<JdbcColumn<ItemManufacturingMaster, ?>> list = new ArrayList<>();
        add(list, "im_f_id", ItemManufacturingMaster::setImFId, ItemManufacturingMaster::getImFId, JdbcUtil::setInt, JdbcUtil::getInt, true);
        add(list, "im_i_id", ItemManufacturingMaster::setImIId, ItemManufacturingMaster::getImIId, JdbcUtil::setInt, JdbcUtil::getInt, true);
        add(list, "im_effective_date", ItemManufacturingMaster::setImEffectiveDate, ItemManufacturingMaster::getImEffectiveDate, JdbcUtil::setDate, JdbcUtil::getDate, true);
        add(list, "im_expired_date", ItemManufacturingMaster::setImExpiredDate, ItemManufacturingMaster::getImExpiredDate, JdbcUtil::setDate, JdbcUtil::getDate);
        add(list, "im_manufacturing_quantity", ItemManufacturingMaster::setImManufacturingQuantity, ItemManufacturingMaster::getImManufacturingQuantity, JdbcUtil::setBigInt, JdbcUtil::getBigInt);
        COLUMN_LIST = list;
    }

    public ItemManufacturingMasterDaoJdbc(CostBenchDbManagerJdbc dbManager) {
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
    public int insert(ItemManufacturingMaster entity) {
        return doInsert(entity);
    }

    @Override
    public int[] insertBatch(Collection<ItemManufacturingMaster> entityList) {
        return doInsert(entityList);
    }

    @Override
    public List<ItemManufacturingMaster> selectAll(LocalDate date) {
        throw new InternalError("yet implmented");
    }

    @Override
    public List<ItemManufacturingMaster> selectAll() {
        String sql = "select * from " + TABLE_NAME;
        return executeQueryList(sql, null, this::newEntity);
    }

    @Override
    public List<ItemManufacturingMasterIds> selectIds(LocalDate date) {
        throw new InternalError("yet implmented");
    }

    @Override
    public Stream<ItemManufacturingMaster> selectByFactory(int factoryId, LocalDate date) {
        String sql = "select * from " + TABLE_NAME + " where im_f_id = ? and " + PS_COND_DATE;
        return executeQueryStream(sql, ps -> {
            int i = 1;
            setInt(ps, i++, factoryId);
            setDate(ps, i++, date);
        }, this::newEntity);
    }

    @Override
    public Stream<ItemManufacturingMaster> selectByFactories(List<Integer> factoryIdList, LocalDate date) {
        String s = Stream.generate(() -> "?").limit(factoryIdList.size()).collect(Collectors.joining(","));
        String sql = "select * from " + TABLE_NAME + " where im_f_id in (" + s + ") and " + PS_COND_DATE;
        return executeQueryStream(sql, ps -> {
            int i = 1;
            for (Integer id : factoryIdList) {
                setInt(ps, i++, id);
            }
            setDate(ps, i++, date);
        }, this::newEntity);
    }

    @Override
    public ItemManufacturingMaster selectById(int factoryId, int itemId, LocalDate date) {
        throw new InternalError("yet implmented");
    }

    @Override
    public ItemManufacturingMaster selectByIdForUpdate(int factoryId, int itemId, LocalDate date) {
        String sql = "select * from " + TABLE_NAME + " where im_f_id = ? and im_i_id = ? and " + PS_COND_DATE //
                + " for update";
        return executeQuery1(sql, ps -> {
            int i = 1;
            setInt(ps, i++, factoryId);
            setInt(ps, i++, itemId);
            setDate(ps, i++, date);
        }, this::newEntity);
    }

    @Override
    public List<ItemManufacturingMaster> selectByIdFuture(int factoryId, int itemId, LocalDate date) {
        String sql = "select * from " + TABLE_NAME + " where im_f_id = ? and im_i_id = ? and " + "? < im_effective_date" //
                + " order by im_effective_date";
        return executeQueryList(sql, ps -> {
            int i = 1;
            setInt(ps, i++, factoryId);
            setInt(ps, i++, itemId);
            setDate(ps, i++, date);
        }, this::newEntity);
    }

    @Override
    public int update(ItemManufacturingMaster entity) {
        return doUpdate(entity);
    }

    @Override
    public List<Integer> selectIdByFactory(int factoryId, LocalDate date) {
        String sql = "select im_i_id from " + TABLE_NAME + " where im_f_id = ? and " + PS_COND_DATE;
        return executeQueryList(sql, ps -> {
            int i = 1;
            setInt(ps, i++, factoryId);
            setDate(ps, i++, date);
        }, rs -> getInt(rs, "im_i_id"));
    }

    private ItemManufacturingMaster newEntity(ResultSet rs) throws SQLException {
        ItemManufacturingMaster entity = new ItemManufacturingMaster();
        fillEntity(entity, rs);
        return entity;
    }

    @Override
    public void forEach(Consumer<ItemManufacturingMaster> entityConsumer) {
        doForEach(ItemManufacturingMaster::new, entityConsumer);
    }
}
