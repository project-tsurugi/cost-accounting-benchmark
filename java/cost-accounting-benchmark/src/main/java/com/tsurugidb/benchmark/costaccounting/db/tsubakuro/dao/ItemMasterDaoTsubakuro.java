package com.tsurugidb.benchmark.costaccounting.db.tsubakuro.dao;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import com.tsurugidb.benchmark.costaccounting.db.dao.ItemMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.domain.ItemType;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemMaster;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao.SqlIn;
import com.tsurugidb.benchmark.costaccounting.db.tsubakuro.CostBenchDbManagerTsubakuro;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;
import com.tsurugidb.sql.proto.SqlRequest.Parameter;
import com.tsurugidb.sql.proto.SqlRequest.Placeholder;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.Parameters;
import com.tsurugidb.tsubakuro.sql.Placeholders;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;

public class ItemMasterDaoTsubakuro extends TsubakuroDao<ItemMaster> implements ItemMasterDao {

    private static final List<TsubakuroColumn<ItemMaster, ?>> COLUMN_LIST;
    static {
        List<TsubakuroColumn<ItemMaster, ?>> list = new ArrayList<>();
        add(list, "i_id", AtomType.INT4, ItemMaster::setIId, ItemMaster::getIId, TsubakuroUtil::getParameter, TsubakuroUtil::getInt, true);
        add(list, "i_effective_date", AtomType.DATE, ItemMaster::setIEffectiveDate, ItemMaster::getIEffectiveDate, TsubakuroUtil::getParameter, TsubakuroUtil::getDate, true);
        add(list, "i_expired_date", AtomType.DATE, ItemMaster::setIExpiredDate, ItemMaster::getIExpiredDate, TsubakuroUtil::getParameter, TsubakuroUtil::getDate);
        add(list, "i_name", AtomType.CHARACTER, ItemMaster::setIName, ItemMaster::getIName, TsubakuroUtil::getParameter, TsubakuroUtil::getString);
        add(list, "i_type", AtomType.CHARACTER, ItemMaster::setIType, ItemMaster::getIType, TsubakuroUtil::getParameter, TsubakuroUtil::getItemType);
        add(list, "i_unit", AtomType.CHARACTER, ItemMaster::setIUnit, ItemMaster::getIUnit, TsubakuroUtil::getParameter, TsubakuroUtil::getString);
        add(list, "i_weight_ratio", AtomType.DECIMAL, ItemMaster::setIWeightRatio, ItemMaster::getIWeightRatio,
                (name, value) -> TsubakuroUtil.getParameter(name, value, ItemMaster.I_WEIGHT_RATIO_SCALE), TsubakuroUtil::getDecimal);
        add(list, "i_weight_unit", AtomType.CHARACTER, ItemMaster::setIWeightUnit, ItemMaster::getIWeightUnit, TsubakuroUtil::getParameter, TsubakuroUtil::getString);
        add(list, "i_price", AtomType.DECIMAL, ItemMaster::setIPrice, ItemMaster::getIPrice, (name, value) -> TsubakuroUtil.getParameter(name, value, ItemMaster.I_PRICE_SCALE),
                TsubakuroUtil::getDecimal);
        add(list, "i_price_unit", AtomType.CHARACTER, ItemMaster::setIPriceUnit, ItemMaster::getIPriceUnit, TsubakuroUtil::getParameter, TsubakuroUtil::getString);
        COLUMN_LIST = list;
    }

    public ItemMasterDaoTsubakuro(CostBenchDbManagerTsubakuro dbManager) {
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
        var inSql = new SqlIn("i_id");
        var placeholders = new ArrayList<Placeholder>();
        var parameters = new ArrayList<Parameter>();
        int i = 0;
        for (int id : ids) {
            var variable = "id" + (i++);
            placeholders.add(Placeholders.of(variable, AtomType.INT4));
            inSql.add(variable);
            parameters.add(Parameters.of(variable, id));
        }
        placeholders.add(Placeholders.of(vDate.name(), AtomType.DATE));
        parameters.add(Parameters.of(vDate.name(), date));

        var sql = getSelectEntitySql() + " where " + inSql + " and " + TG_COND_DATE;
        try (var ps = createPreparedStatement(sql, placeholders)) {
            var converter = getSelectEntityConverter();
            return executeAndGetList(ps, parameters, converter);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (ServerException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Integer> selectIdByType(LocalDate date, ItemType type) {
        var ps = getSelectIdByTypePs();
        var parameters = List.of( //
                Parameters.of(vDate.name(), date), //
                Parameters.of("type", type.getValue()));
        return executeAndGetList(ps, parameters, rs -> {
            if (rs.nextColumn()) {
                return rs.fetchInt4Value();
            }
            throw new AssertionError();
        });
    }

    private PreparedStatement selectIdByTypePs;

    private synchronized PreparedStatement getSelectIdByTypePs() {
        if (this.selectIdByTypePs == null) {
            var sql = "select i_id from " + TABLE_NAME + " where " + TG_COND_DATE + " and i_type = :type";
            var placeholders = List.of( //
                    Placeholders.of(vDate.name(), AtomType.DATE), //
                    Placeholders.of("type", AtomType.CHARACTER));
            this.selectIdByTypePs = createPreparedStatement(sql, placeholders);
        }
        return this.selectIdByTypePs;
    }

    @Override
    public List<ItemMaster> selectAll() {
        return doSelectAll();
    }

    @Override
    public Integer selectMaxId() {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public ItemMaster selectById(int id, LocalDate date) {
        var ps = getSelectByIdPs();
        var parameters = List.of( //
                Parameters.of("id", id), //
                Parameters.of(vDate.name(), date));
        var converter = getSelectEntityConverter();
        return executeAndGetRecord(ps, parameters, converter);
    }

    private PreparedStatement selectById;

    private synchronized PreparedStatement getSelectByIdPs() {
        if (this.selectById == null) {
            var sql = getSelectEntitySql() + " where i_id = :id and " + TG_COND_DATE;
            var placeholders = List.of( //
                    Placeholders.of("id", AtomType.INT4), //
                    Placeholders.of(vDate.name(), AtomType.DATE));
            this.selectById = createPreparedStatement(sql, placeholders);
        }
        return this.selectById;
    }

    @Override
    public void forEach(Consumer<ItemMaster> entityConsumer) {
        throw new UnsupportedOperationException("not yet impl");
    }
}
