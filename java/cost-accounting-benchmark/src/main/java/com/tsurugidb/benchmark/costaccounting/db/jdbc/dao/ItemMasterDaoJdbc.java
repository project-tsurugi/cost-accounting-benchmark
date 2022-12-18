package com.tsurugidb.benchmark.costaccounting.db.jdbc.dao;

import static com.tsurugidb.benchmark.costaccounting.db.jdbc.dao.JdbcUtil.getInt;
import static com.tsurugidb.benchmark.costaccounting.db.jdbc.dao.JdbcUtil.setDate;
import static com.tsurugidb.benchmark.costaccounting.db.jdbc.dao.JdbcUtil.setInt;
import static com.tsurugidb.benchmark.costaccounting.db.jdbc.dao.JdbcUtil.setItemType;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import com.tsurugidb.benchmark.costaccounting.db.dao.ItemMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.domain.ItemType;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemMaster;
import com.tsurugidb.benchmark.costaccounting.db.jdbc.CostBenchDbManagerJdbc;

public class ItemMasterDaoJdbc extends JdbcDao<ItemMaster> implements ItemMasterDao {

    private static final List<JdbcColumn<ItemMaster, ?>> COLUMN_LIST;
    static {
        List<JdbcColumn<ItemMaster, ?>> list = new ArrayList<>();
        add(list, "i_id", ItemMaster::setIId, ItemMaster::getIId, JdbcUtil::setInt, JdbcUtil::getInt, true);
        add(list, "i_effective_date", ItemMaster::setIEffectiveDate, ItemMaster::getIEffectiveDate, JdbcUtil::setDate, JdbcUtil::getDate, true);
        add(list, "i_expired_date", ItemMaster::setIExpiredDate, ItemMaster::getIExpiredDate, JdbcUtil::setDate, JdbcUtil::getDate);
        add(list, "i_name", ItemMaster::setIName, ItemMaster::getIName, JdbcUtil::setString, JdbcUtil::getString);
        add(list, "i_type", ItemMaster::setIType, ItemMaster::getIType, JdbcUtil::setItemType, JdbcUtil::getItemType);
        add(list, "i_unit", ItemMaster::setIUnit, ItemMaster::getIUnit, JdbcUtil::setString, JdbcUtil::getString);
        add(list, "i_weight_ratio", ItemMaster::setIWeightRatio, ItemMaster::getIWeightRatio, JdbcUtil::setDecimal, JdbcUtil::getDecimal);
        add(list, "i_weight_unit", ItemMaster::setIWeightUnit, ItemMaster::getIWeightUnit, JdbcUtil::setString, JdbcUtil::getString);
        add(list, "i_price", ItemMaster::setIPrice, ItemMaster::getIPrice, JdbcUtil::setDecimal, JdbcUtil::getDecimal);
        add(list, "i_price_unit", ItemMaster::setIPriceUnit, ItemMaster::getIPriceUnit, JdbcUtil::setString, JdbcUtil::getString);
        COLUMN_LIST = list;
    }

    public ItemMasterDaoJdbc(CostBenchDbManagerJdbc dbManager) {
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
    public ItemMaster selectByKey(int id, LocalDate date) {
        String sql = "select * from " + TABLE_NAME + " where i_id = ? and i_effective_date = ?";
        return executeQuery1(sql, ps -> {
            int i = 1;
            setInt(ps, i++, id);
            setDate(ps, i++, date);
        }, this::newEntity);
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

    @Override
    public void forEach(Consumer<ItemMaster> entityConsumer) {
        doForEach(ItemMaster::new, entityConsumer);
    }
}
