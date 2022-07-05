package com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.ItemMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.doma2.domain.ItemType;
import com.tsurugidb.benchmark.costaccounting.db.doma2.entity.ItemMaster;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.CostBenchDbManagerIxeaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain.BenchVariable;
import com.tsurugidb.iceaxe.result.TgResultMapping;
import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariable;
import com.tsurugidb.iceaxe.statement.TgVariable.TgVariableInteger;
import com.tsurugidb.iceaxe.statement.TgVariableList;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementQuery0;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementQuery1;

public class ItemMasterDaoIceaxe extends IceaxeDao<ItemMaster> implements ItemMasterDao {

    private static final TgVariableInteger I_ID = BenchVariable.ofInt("i_id");
    private static final TgVariable<ItemType> I_TYPE = BenchVariable.ofItemType("i_type");
    private static final List<IceaxeColumn<ItemMaster, ?>> COLUMN_LIST;
    static {
        List<IceaxeColumn<ItemMaster, ?>> list = new ArrayList<>();
        add(list, I_ID, ItemMaster::setIId, ItemMaster::getIId, IceaxeRecordUtil::getInt, true);
        add(list, BenchVariable.ofDate("i_effective_date"), ItemMaster::setIEffectiveDate, ItemMaster::getIEffectiveDate, IceaxeRecordUtil::getDate, true);
        add(list, BenchVariable.ofDate("i_expired_date"), ItemMaster::setIExpiredDate, ItemMaster::getIExpiredDate, IceaxeRecordUtil::getDate);
        add(list, BenchVariable.ofString("i_name"), ItemMaster::setIName, ItemMaster::getIName, IceaxeRecordUtil::getString);
        add(list, I_TYPE, ItemMaster::setIType, ItemMaster::getIType, IceaxeRecordUtil::getItemType);
        add(list, BenchVariable.ofString("i_unit"), ItemMaster::setIUnit, ItemMaster::getIUnit, IceaxeRecordUtil::getString);
        add(list, BenchVariable.ofDecimal("i_weight_ratio"), ItemMaster::setIWeightRatio, ItemMaster::getIWeightRatio, IceaxeRecordUtil::getDecimal);
        add(list, BenchVariable.ofString("i_weight_unit"), ItemMaster::setIWeightUnit, ItemMaster::getIWeightUnit, IceaxeRecordUtil::getString);
        add(list, BenchVariable.ofDecimal("i_price"), ItemMaster::setIPrice, ItemMaster::getIPrice, IceaxeRecordUtil::getDecimal);
        add(list, BenchVariable.ofString("i_price_unit"), ItemMaster::setIPriceUnit, ItemMaster::getIPriceUnit, IceaxeRecordUtil::getString);
        COLUMN_LIST = list;
    }

    public ItemMasterDaoIceaxe(CostBenchDbManagerIxeaxe dbManager) {
        super(dbManager, TABLE_NAME, COLUMN_LIST, ItemMaster::new);
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
        var variableList = TgVariableList.of();
        var param = TgParameterList.of();
        int i = 0;
        for (int id : ids) {
            var variable = I_ID.copy(Integer.toString(i++));
            variableList.add(variable);
            if (sb.length() != 0) {
                sb.append(',');
            }
            sb.append(variable.sqlName());
            param.add(variable.bind(id));
        }
        variableList.add(vDate);
        param.add(vDate.bind(date));

        var sql = getSelectEntitySql() + " where i_id in (" + sb + ") and " + TG_COND_DATE;
        var parameterMapping = TgParameterMapping.of(variableList);
        var resultMapping = getEntityResultMapping();
        try (var ps = createPreparedQuery(sql, parameterMapping, resultMapping)) {
            return executeAndGetList(ps, param);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static final TgVariable<ItemType> vType = I_TYPE.copy("type");

    @Override
    public List<Integer> selectIdByType(LocalDate date, ItemType type) {
        var ps = getSelectIdByTypePs();
        var param = TgParameterList.of(vDate.bind(date), vType.bind(type));
        return executeAndGetList(ps, param);
    }

    private TsurugiPreparedStatementQuery1<TgParameterList, Integer> selectIdByTypePs;

    private synchronized TsurugiPreparedStatementQuery1<TgParameterList, Integer> getSelectIdByTypePs() {
        if (this.selectIdByTypePs == null) {
            var sql = "select i_id from " + TABLE_NAME + " where " + TG_COND_DATE + " and i_type = " + vType;
            var parameterMapping = TgParameterMapping.of(vDate, vType);
            var resultMapping = TgResultMapping.of(record -> record.nextInt4OrNull());
            this.selectIdByTypePs = createPreparedQuery(sql, parameterMapping, resultMapping);
        }
        return this.selectIdByTypePs;
    }

    @Override
    public List<ItemMaster> selectAll() {
        return doSelectAll();
    }

    @Override
    public Integer selectMaxId() {
        var ps = getSelectMaxIdPs();
        return executeAndGetRecord(ps);
    }

    private TsurugiPreparedStatementQuery0<Integer> selectMaxId;

    private synchronized TsurugiPreparedStatementQuery0<Integer> getSelectMaxIdPs() {
        if (this.selectMaxId == null) {
            var sql = "select max(i_id) + 1 from " + TABLE_NAME;
            var resultMapping = TgResultMapping.of(record -> record.nextInt4OrNull());
            this.selectMaxId = createPreparedQuery(sql, resultMapping);
        }
        return this.selectMaxId;
    }

    private static final TgVariableInteger vId = I_ID.copy("id");

    @Override
    public ItemMaster selectById(int id, LocalDate date) {
        var ps = getSelectByIdPs();
        var param = TgParameterList.of(vId.bind(id), vDate.bind(date));
        return executeAndGetRecord(ps, param);
    }

    private TsurugiPreparedStatementQuery1<TgParameterList, ItemMaster> selectById;

    private synchronized TsurugiPreparedStatementQuery1<TgParameterList, ItemMaster> getSelectByIdPs() {
        if (this.selectById == null) {
            var sql = getSelectEntitySql() + " where i_id = :id and " + TG_COND_DATE;
            var parameterMapping = TgParameterMapping.of(vId, vDate);
            var resultMapping = getEntityResultMapping();
            this.selectById = createPreparedQuery(sql, parameterMapping, resultMapping);
        }
        return this.selectById;
    }
}
