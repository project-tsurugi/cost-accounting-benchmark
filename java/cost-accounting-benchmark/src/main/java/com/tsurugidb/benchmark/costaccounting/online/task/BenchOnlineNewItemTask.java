package com.tsurugidb.benchmark.costaccounting.online.task;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

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
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * 新規開発商品の追加
 */
public class BenchOnlineNewItemTask extends BenchOnlineTask {
    public static final String TASK_NAME = "new-item";

    private static List<Integer> itemMasterWorkKeylist;

    public static void clearPrepareData() {
        itemMasterWorkKeylist = null;
    }

    private TgTmSetting settingPre;
    private TgTmSetting settingMain;

    public BenchOnlineNewItemTask(int taskId) {
        super(TASK_NAME, taskId);
    }

    @Override
    public void initializeSetting() {
        this.settingPre = config.getSetting(LOG, this, () -> {
            if (BenchConst.useReadArea()) {
                return TgTxOption.ofLTX(BenchConst.DEFAULT_TX_OPTION).addWritePreserve(ItemMasterDao.TABLE_NAME, ItemConstructionMasterDao.TABLE_NAME) //
                        .addInclusiveReadArea(ItemMasterDao.TABLE_NAME);
            } else {
                return TgTxOption.ofLTX(ItemMasterDao.TABLE_NAME, ItemConstructionMasterDao.TABLE_NAME);
            }
        });
        this.settingMain = config.getSetting(LOG, this, () -> {
            if (BenchConst.useReadArea()) {
                return TgTxOption.ofLTX(BenchConst.DEFAULT_TX_OPTION).addWritePreserve(ItemManufacturingMasterDao.TABLE_NAME) //
                        .addInclusiveReadArea(ItemManufacturingMasterDao.TABLE_NAME);
            } else {
                return TgTxOption.ofLTX(ItemManufacturingMasterDao.TABLE_NAME);
            }
        });
        setTxOptionDescription(settingMain);
    }

    @Override
    public void executePrepare() {
        var setting = TgTmSetting.of(TgTxOption.ofRTX().label(TASK_NAME + ".prepare"));
        var date = config.getBatchDate();

        cacheItemMasterWorkKeyList(setting, date);
    }

    @Override
    protected boolean execute1() {
        ItemMaster item = getNewItemMaster();
        dbManager.execute(settingMain, () -> {
            executeMain(item);
        });
        return true;
    }

    protected ItemMaster getNewItemMaster() {
        for (;;) {
            try {
                ItemMaster i = dbManager.execute(settingPre, () -> {
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

        List<Integer> list;
        if (itemMasterWorkKeylist != null) {
            list = itemMasterWorkKeylist;
        } else {
            list = selectItemMasterWorkKeyList(date);
        }
        List<Integer> workList = new ArrayList<>(list);
        int treeSize = random.random(1, InitialData03ItemMaster.PRODUCT_TREE_SIZE);
        initialData.randomShuffle(workList, random.random(1, Integer.MAX_VALUE / 2), treeSize);
        int workIndex = 0;

        Set<Integer> workSet = new HashSet<>(treeSize);
        for (int i = 0; i < treeSize; i++) {
            Integer id = workList.get(workIndex++);
            workSet.add(id);
        }

        initialData.insertItemConstructionMasterProduct(item.getIId(), workSet, dbManager.getItemConstructionMasterDao(), new AtomicInteger());

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
            if (!dbManager.isRetryable(e)) {
                LOG.warn("insertItemMaster error", e);
            }
            throw e;
        }

        return entity;
    }

    private void cacheItemMasterWorkKeyList(TgTmSetting setting, LocalDate date) {
        synchronized (BenchOnlineNewItemTask.class) {
            if (itemMasterWorkKeylist == null) {
                var log = LoggerFactory.getLogger(BenchOnlineNewItemTask.class);
                dbManager.execute(setting, () -> {
                    log.info("itemMasterDao.selectIdByType(WORK_IN_PROCESS) start");
                    itemMasterWorkKeylist = selectItemMasterWorkKeyList(date);
                    log.info("itemMasterDao.selectIdByType(WORK_IN_PROCESS) end. size={}", itemMasterWorkKeylist.size());
                });
            }
        }
    }

    private List<Integer> selectItemMasterWorkKeyList(LocalDate date) {
        var dao = dbManager.getItemMasterDao();
        return dao.selectIdByType(date, ItemType.WORK_IN_PROCESS);
    }

    protected void executeMain(ItemMaster item) {
        InitialData04ItemManufacturingMaster initialData = new InitialData04ItemManufacturingMaster(1, date);

        ItemManufacturingMaster entity = initialData.newItemManufacturingMaster(factoryId, item.getIId());
        ItemManufacturingMasterDao itemManufacturingMasterDao = dbManager.getItemManufacturingMasterDao();
        itemManufacturingMasterDao.insert(entity);
    }

    // for test
    public static void main(String[] args) {
        try (BenchOnlineNewItemTask task = new BenchOnlineNewItemTask(0)) {
            try (CostBenchDbManager manager = createCostBenchDbManagerForTest()) {
                task.setDao(null, manager);

                task.initialize(1, InitialData.DEFAULT_BATCH_DATE);

                task.execute();
            }
        }
    }
}
