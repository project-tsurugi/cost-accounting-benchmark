package com.tsurugidb.benchmark.costaccounting.online.task;

import java.time.LocalDate;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.UniqueConstraintException;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemConstructionMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemManufacturingMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.domain.ItemType;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemManufacturingMaster;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemMaster;
import com.tsurugidb.benchmark.costaccounting.init.InitialData;
import com.tsurugidb.benchmark.costaccounting.init.InitialData03ItemMaster;
import com.tsurugidb.benchmark.costaccounting.init.InitialData04ItemManufacturingMaster;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;

/**
 * 新規開発商品の追加
 */
public class BenchOnlineNewItemTask extends BenchOnlineTask {
    private static final Logger LOG = LoggerFactory.getLogger(BenchOnlineNewItemTask.class);

    private final TgTmSetting settingPre;
    private final TgTmSetting settingMain;

    public BenchOnlineNewItemTask() {
        super("new-item");
        this.settingPre = getSetting(() -> TgTxOption.ofLTX(ItemMasterDao.TABLE_NAME, ItemConstructionMasterDao.TABLE_NAME));
        this.settingMain = getSetting(() -> TgTxOption.ofLTX(ItemManufacturingMasterDao.TABLE_NAME));
    }

    @Override
    protected boolean execute1() {
        ItemMaster item = getNewItemMaster();
        dbManager.execute(settingMain, () -> {
            checkStop();
            executeMain(item);
        });
        return true;
    }

    protected ItemMaster getNewItemMaster() {
        for (;;) {
            try {
                ItemMaster i = dbManager.execute(settingPre, () -> {
                    checkStop();
                    return getNewItemMasterMain();
                });
                if (i != null) {
                    return i;
                }
            } catch (UniqueConstraintException e) {
                LOG.debug("duplicate item_master (transaction)", e);
                continue;
            }
        }
    }

    protected ItemMaster getNewItemMasterMain() {
        InitialData03ItemMaster initialData = new InitialData03ItemMaster(date);

        ItemMaster item;
        for (;;) {
            item = insertItemMaster(initialData);
            switch (1) {
            case 1:
                if (item == null) {
                    // 別コネクションでリトライ
                    return null;
                }
                break;
            default:
                if (item == null) {
                    // 同一コネクション内でリトライ（PostgreSQLだと例外発生時に同一コネクションでSQLを発行するとエラーになる）
                    continue;
                }
                break;
            }
            break;
        }

        logTarget("product=%s", item.getIName());

        ItemMasterDao itemMasterDao = dbManager.getItemMasterDao();
        List<Integer> workList = itemMasterDao.selectIdByType(date, ItemType.WORK_IN_PROCESS);
        int s = random.random(1, InitialData03ItemMaster.PRODUCT_TREE_SIZE);
        Set<Integer> workSet = new HashSet<>(s);
        for (int i = 0; i < s; i++) {
            Integer id = initialData.getRandomAndRemove(i, workList);
            workSet.add(id);
        }

        initialData.insertItemConstructionMasterProduct(item.getIId(), workSet, dbManager.getItemConstructionMasterDao());

        return item;
    }

    private ItemMaster insertItemMaster(InitialData03ItemMaster initialData) {
        ItemMasterDao itemMasterDao = dbManager.getItemMasterDao();
        int newId = itemMasterDao.selectMaxId();

        ItemMaster entity = initialData.newItemMasterProduct(newId);
        entity.setEffectiveDate(date);
        LocalDate endDate = initialData.getRandomExpiredDate(newId, date);
        entity.setExpiredDate(endDate);

        try {
            itemMasterDao.insert(entity);
        } catch (UniqueConstraintException e) {
            LOG.debug("duplicate item_master (insert)", e);
            if (dbManager.isTsurugi()) {
                throw e;
            }
            return null;
        } catch (Exception e) {
            if (!dbManager.isRetriable(e)) {
                LOG.warn("insertItemMaster error", e);
            }
            throw e;
        }

        return entity;
    }

    protected void executeMain(ItemMaster item) {
        InitialData04ItemManufacturingMaster initialData = new InitialData04ItemManufacturingMaster(1, date);

        ItemManufacturingMaster entity = initialData.newItemManufacturingMaster(factoryId, item.getIId());
        ItemManufacturingMasterDao itemManufacturingMasterDao = dbManager.getItemManufacturingMasterDao();
        itemManufacturingMasterDao.insert(entity);
    }

    // for test
    public static void main(String[] args) {
        BenchOnlineNewItemTask task = new BenchOnlineNewItemTask();

        try (CostBenchDbManager manager = createCostBenchDbManagerForTest()) {
            task.setDao(manager);

            task.initialize(1, InitialData.DEFAULT_BATCH_DATE);

            task.execute();
        }
    }
}
