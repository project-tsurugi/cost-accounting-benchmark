package com.tsurugidb.benchmark.costaccounting.db.tsubakuro.dao;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.tsurugidb.benchmark.costaccounting.db.dao.ItemConstructionMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.domain.ItemType;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemConstructionMaster;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemConstructionMasterIds;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemConstructionMasterKey;
import com.tsurugidb.benchmark.costaccounting.db.tsubakuro.CostBenchDbManagerTsubakuro;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;

public class ItemConstructionMasterDaoTsubakuro extends TsubakuroDao<ItemConstructionMaster> implements ItemConstructionMasterDao {

    private static final List<TsubakuroColumn<ItemConstructionMaster, ?>> COLUMN_LIST;
    static {
        List<TsubakuroColumn<ItemConstructionMaster, ?>> list = new ArrayList<>();
        add(list, "ic_parent_i_id", AtomType.INT4, ItemConstructionMaster::setIcParentIId, ItemConstructionMaster::getIcParentIId, TsubakuroUtil::getParameter, TsubakuroUtil::getInt, true);
        add(list, "ic_i_id", AtomType.INT4, ItemConstructionMaster::setIcIId, ItemConstructionMaster::getIcIId, TsubakuroUtil::getParameter, TsubakuroUtil::getInt, true);
        add(list, "ic_effective_date", AtomType.DATE, ItemConstructionMaster::setIcEffectiveDate, ItemConstructionMaster::getIcEffectiveDate, TsubakuroUtil::getParameter, TsubakuroUtil::getDate,
                true);
        add(list, "ic_expired_date", AtomType.DATE, ItemConstructionMaster::setIcExpiredDate, ItemConstructionMaster::getIcExpiredDate, TsubakuroUtil::getParameter, TsubakuroUtil::getDate);
        add(list, "ic_material_unit", AtomType.CHARACTER, ItemConstructionMaster::setIcMaterialUnit, ItemConstructionMaster::getIcMaterialUnit, TsubakuroUtil::getParameter, TsubakuroUtil::getString);
        add(list, "ic_material_quantity", AtomType.DECIMAL, ItemConstructionMaster::setIcMaterialQuantity, ItemConstructionMaster::getIcMaterialQuantity,
                (name, value) -> TsubakuroUtil.getParameter(name, value, ItemConstructionMaster.IC_MATERIAL_QUANTITY_SCALE), TsubakuroUtil::getDecimal);
        add(list, "ic_loss_ratio", AtomType.DECIMAL, ItemConstructionMaster::setIcLossRatio, ItemConstructionMaster::getIcLossRatio,
                (name, value) -> TsubakuroUtil.getParameter(name, value, ItemConstructionMaster.IC_LOSS_RATIO_SCALE), TsubakuroUtil::getDecimal);
        COLUMN_LIST = list;
    }

    public ItemConstructionMasterDaoTsubakuro(CostBenchDbManagerTsubakuro dbManager) {
        super(dbManager, TABLE_NAME, COLUMN_LIST, ItemConstructionMaster::new);
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
    public int insert(ItemConstructionMaster entity) {
        return doInsert(entity, false);
    }

    @Override
    public int[] insertBatch(Collection<ItemConstructionMaster> entityList, boolean insertOnly) {
        return doInsert(entityList, insertOnly);
    }

    @Override
    public List<ItemConstructionMaster> selectAll() {
        return doSelectAll();
    }

    @Override
    public List<ItemConstructionMaster> selectAll(LocalDate date) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public List<ItemConstructionMasterIds> selectIds(LocalDate date) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public List<ItemConstructionMaster> selectByParentId(int parentId, LocalDate date) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public ItemConstructionMaster selectById(int parentId, int itemId, LocalDate date) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public Stream<ItemConstructionMaster> selectRecursiveByParentId(int parentId, LocalDate date) {
        throw new UnsupportedOperationException("impossible with-recursive");
    }

    @Override
    public List<ItemConstructionMasterKey> selectByItemType(LocalDate date, List<ItemType> typeList) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public ItemConstructionMaster lock(ItemConstructionMaster in) {
        // Tsurugiにselect for updateは無い
        return in;
    }

    @Override
    public int delete(ItemConstructionMasterKey key) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public void forEach(Consumer<ItemConstructionMaster> entityConsumer) {
        throw new UnsupportedOperationException("not yet impl");
    }
}
