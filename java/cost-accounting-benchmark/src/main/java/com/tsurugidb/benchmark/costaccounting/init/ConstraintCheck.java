package com.tsurugidb.benchmark.costaccounting.init;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.ItemConstructionMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.ItemManufacturingMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.ItemMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.doma2.entity.HasDateRange;
import com.tsurugidb.benchmark.costaccounting.db.doma2.entity.ItemConstructionMaster;
import com.tsurugidb.benchmark.costaccounting.db.doma2.entity.ItemManufacturingMaster;
import com.tsurugidb.benchmark.costaccounting.db.doma2.entity.ItemMaster;
import com.tsurugidb.iceaxe.transaction.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.TgTxOption;

public class ConstraintCheck {

    private static final TgTmSetting TX_MAIN = TgTmSetting.of( //
            TgTxOption.ofOCC(), //
            TgTxOption.ofRTX());

    public static void main(String[] args) {
        new ConstraintCheck().main();
    }

    private CostBenchDbManager dbManager;

    public void main() {
        try (CostBenchDbManager manager = InitialData.createDbManager()) {
            this.dbManager = manager;

            manager.execute(TX_MAIN, () -> {
                checkDateRange(ItemMasterDao.TABLE_NAME, manager.getItemMasterDao().selectAll(), this::getKey);
                checkDateRange(ItemConstructionMasterDao.TABLE_NAME, manager.getItemConstructionMasterDao().selectAll(), this::getKey);
                checkDateRange(ItemManufacturingMasterDao.TABLE_NAME, manager.getItemManufacturingMasterDao().selectAll(), this::getKey);
                checkBomChild(InitialData.DEFAULT_BATCH_DATE);
            });
        }
    }

    protected List<Object> getKey(ItemMaster entity) {
        return Arrays.asList(entity.getIId());
    }

    protected List<Object> getKey(ItemConstructionMaster entity) {
        return Arrays.asList(entity.getIcIId(), entity.getIcParentIId());
    }

    protected List<Object> getKey(ItemManufacturingMaster entity) {
        return Arrays.asList(entity.getImFId(), entity.getImIId());
    }

    protected <T extends HasDateRange> void checkDateRange(String tableName, List<T> allList, Function<T, List<Object>> keyGetter) {
        Map<List<Object>, List<T>> map = toMap(allList, keyGetter);

        map.forEach((key, list) -> {
            for (T target : list) {
                for (T entity : list) {
                    if (entity == target) {
                        continue;
                    }
                    if (between(target.getEffectiveDate(), entity.getEffectiveDate(), entity.getExpiredDate())
                            || between(target.getExpiredDate(), entity.getEffectiveDate(), entity.getExpiredDate())) {
                        throw new ConstraintCheckException(tableName, key);
                    }
                }
            }
        });
    }

    private <T> Map<List<Object>, List<T>> toMap(List<T> allList, Function<T, List<Object>> keyGetter) {
        Map<List<Object>, List<T>> map = new HashMap<>(allList.size());
        for (T entity : allList) {
            List<Object> key = keyGetter.apply(entity);
            List<T> list = map.computeIfAbsent(key, k -> new ArrayList<>());
            list.add(entity);
        }
        return map;
    }

    private static boolean between(LocalDate date, LocalDate startDate, LocalDate endDate) {
        return startDate.compareTo(date) <= 0 && date.compareTo(endDate) <= 0;
    }

    @SuppressWarnings("serial")
    public static class ConstraintCheckException extends RuntimeException {
        public ConstraintCheckException(String tableName, List<Object> key) {
            super("constraint check error " + tableName + key);
        }
    }

    protected void checkBomChild(LocalDate date) {
        ItemConstructionMasterDao dao = dbManager.getItemConstructionMasterDao();
        List<ItemConstructionMaster> allList = dao.selectAll(date);
        Map<List<Object>, List<ItemConstructionMaster>> map = toMap(allList, e -> Arrays.asList(e.getIcParentIId()));
        map.forEach((key, list) -> {
            Set<Integer> set = new HashSet<>();
            for (ItemConstructionMaster entity : list) {
                Integer id = entity.getIcIId();
                if (set.contains(id)) {
                    throw new ConstraintCheckException("ItemConstructionMaster", Arrays.asList(entity.getIcParentIId(), id));
                }
                set.add(id);
            }
        });
    }
}
