package com.tsurugidb.benchmark.costaccounting.online.task;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemConstructionMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.domain.ItemType;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemConstructionMaster;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemConstructionMasterKey;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemMaster;
import com.tsurugidb.benchmark.costaccounting.init.InitialData;
import com.tsurugidb.benchmark.costaccounting.init.InitialData03ItemMaster;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * 原材料の変更
 */
public class BenchOnlineUpdateMaterialTask extends BenchOnlineTask {
    public static final String TASK_NAME = "update-material";

    private static RandomKeySelector<ItemConstructionMasterKey> itemConstructionMasterKeySelectorForAdd;
    private static RandomKeySelector<ItemConstructionMasterKey> itemConstructionMasterKeySelectorForRemove;
    private static RandomKeySelector<Integer> itemMasterMaterialKeySelector;

    public static void clearPrepareData() {
        itemConstructionMasterKeySelectorForAdd = null;
        itemConstructionMasterKeySelectorForRemove = null;
        itemMasterMaterialKeySelector = null;
    }

    private TgTmSetting settingMain;
    private double coverRate;

    public BenchOnlineUpdateMaterialTask(int taskId) {
        super(TASK_NAME, taskId);
    }

    @Override
    public void initializeSetting() {
        this.settingMain = config.getSetting(LOG, this, () -> {
            if (BenchConst.useReadArea()) {
                return TgTxOption.ofLTX(BenchConst.DEFAULT_TX_OPTION).addWritePreserve(ItemConstructionMasterDao.TABLE_NAME) //
                        .addInclusiveReadArea(ItemConstructionMasterDao.TABLE_NAME, ItemMasterDao.TABLE_NAME);
            } else {
                return TgTxOption.ofLTX(ItemConstructionMasterDao.TABLE_NAME);
            }
        });
        setTxOptionDescription(settingMain);
        this.coverRate = config.getCoverRateForTask(title);
    }

    @Override
    public void executePrepare() {
        var setting = TgTmSetting.of(TgTxOption.ofRTX().label(TASK_NAME + ".prepare"));
        var date = config.getBatchDate();

        // 複数スレッドで実行する場合はaddとremoveのキャッシュ作成を同時に実行する
        switch (taskId % 3) {
        case 0:
        default:
            cacheItemConstructionMasterKeyListForAdd(setting, date);
            cacheItemConstructionMasterKeyListForRemove(setting, date);
            cacheItemMasterMaterialKeyList(setting, date);
            break;
        case 1:
            cacheItemConstructionMasterKeyListForRemove(setting, date);
            cacheItemMasterMaterialKeyList(setting, date);
            cacheItemConstructionMasterKeyListForAdd(setting, date);
            break;
        case 2:
            cacheItemMasterMaterialKeyList(setting, date);
            cacheItemConstructionMasterKeyListForRemove(setting, date);
            cacheItemConstructionMasterKeyListForAdd(setting, date);
            break;
        }
    }

    @Override
    protected boolean execute1() {
        return dbManager.execute(settingMain, () -> {
            int select = random.random(0, 1);
            if (select == 0) {
                return executeAdd();
            } else {
                return executeRemove();
            }
        });
    }

    protected boolean executeAdd() {
        // 変更する構成品目を決定
        ItemConstructionMaster item = selectRandomAddItem();

        // 品目構成マスターをロック
        ItemConstructionMasterDao itemCostructionMasterDao = dbManager.getItemConstructionMasterDao();
        itemCostructionMasterDao.lock(item);

        // 追加する原材料の決定
        ItemMaster material;
        {
            RandomKeySelector<Integer> selector;
            if (itemMasterMaterialKeySelector != null) {
                selector = itemMasterMaterialKeySelector;
            } else {
                var list = selectItemMasterMaterialKeyList(date);
                selector = new RandomKeySelector<>(list, random.getRawRandom(), 0, coverRate);
            }
            List<ItemConstructionMaster> childList = itemCostructionMasterDao.selectByParentId(item.getIcIId(), date);
            int materialId = -1;
            for (int j = 0; j < selector.size(); j++) {
                int mid = selector.get();
                if (childList.stream().anyMatch(child -> child.getIcIId().intValue() == mid)) {
                    continue;
                }
                materialId = mid;
                break;
            }
            if (materialId < 0) {
                return false; // retry over
            }
            ItemMasterDao itemMasterDao = dbManager.getItemMasterDao();
            material = itemMasterDao.selectById(materialId, date);
        }

        logTarget("add item=%d, parent=%d", material.getIId(), item.getIcIId());

        // 品目構成マスターへ追加
        ItemConstructionMaster entity = newItemConstructionMaster(item, material);
        itemCostructionMasterDao.insert(entity);

        return true;
    }

    private ItemConstructionMaster selectRandomAddItem() {
        ItemConstructionMasterDao itemCostructionMasterDao = dbManager.getItemConstructionMasterDao();

        RandomKeySelector<ItemConstructionMasterKey> selector;
        if (itemConstructionMasterKeySelectorForAdd != null) {
            selector = itemConstructionMasterKeySelectorForAdd;
        } else {
            var list = selectItemConstructionMasterKeyListForAdd(dbManager, date);
            selector = new RandomKeySelector<>(list, random.getRawRandom(), 0, coverRate);
        }

        ItemConstructionMaster entity;
        for (;;) {
            ItemConstructionMasterKey key = selector.get();
            entity = itemCostructionMasterDao.selectById(key.getIcParentIId(), key.getIcIId(), key.getIcEffectiveDate());
            if (entity != null) {
                break;
            }
        }
        return entity;
    }

    private static final boolean itemConstructionMasterKeyListDebug = false;
    private static final Object lockItemConstructionMasterKeyListForAdd = new Object();

    private void cacheItemConstructionMasterKeyListForAdd(TgTmSetting setting, LocalDate date) {
        synchronized (lockItemConstructionMasterKeyListForAdd) {
            if (itemConstructionMasterKeySelectorForAdd == null) {
                var log = LoggerFactory.getLogger(BenchOnlineUpdateMaterialTask.class);
                dbManager.execute(setting, () -> {
                    if (itemConstructionMasterKeyListDebug) {
                        {
                            log.info("(add)ItemConstructionMasterDao.selectAll() start");
                            var list = dbManager.getItemConstructionMasterDao().selectAll();
                            log.info("(add)ItemConstructionMasterDao.selectAll() end. size={}", list.size());
                        }
                        {
                            log.info("(add)ItemMasterDao.selectAll() start");
                            var list = dbManager.getItemMasterDao().selectAll();
                            log.info("(add)ItemMasterDao.selectAll() end. size={}", list.size());
                        }
                    }
                    log.info("(add)itemCostructionMasterDao.selectByItemType() start");
                    var list = selectItemConstructionMasterKeyListForAdd(dbManager, date);
                    log.info("(add)itemCostructionMasterDao.selectByItemType() end. size={}", list.size());
                    itemConstructionMasterKeySelectorForAdd = new RandomKeySelector<>(list, random.getRawRandom(), 0, coverRate);
                });
            }
        }
    }

    private static List<ItemConstructionMasterKey> selectItemConstructionMasterKeyListForAdd(CostBenchDbManager dbManager, LocalDate date) {
        List<ItemType> typeList = Arrays.stream(ItemType.values()).filter(t -> t != ItemType.RAW_MATERIAL).collect(Collectors.toList());
        var dao = dbManager.getItemConstructionMasterDao();
        return dao.selectByItemType(date, typeList);
    }

    private static final Object lockItemMasterMaterialKeyList = new Object();

    private void cacheItemMasterMaterialKeyList(TgTmSetting setting, LocalDate date) {
        synchronized (lockItemMasterMaterialKeyList) {
            if (itemMasterMaterialKeySelector == null) {
                var log = LoggerFactory.getLogger(BenchOnlineUpdateMaterialTask.class);
                dbManager.execute(setting, () -> {
                    log.info("itemMasterDao.selectIdByType(RAW_MATERIAL) start");
                    var list = selectItemMasterMaterialKeyList(date);
                    log.info("itemMasterDao.selectIdByType(RAW_MATERIAL) end. size={}", list.size());
                    itemMasterMaterialKeySelector = new RandomKeySelector<>(list, random.getRawRandom(), 0, coverRate);
                });
            }
        }
    }

    private List<Integer> selectItemMasterMaterialKeyList(LocalDate date) {
        ItemMasterDao dao = dbManager.getItemMasterDao();
        return dao.selectIdByType(date, ItemType.RAW_MATERIAL);
    }

    private static final BigDecimal MQ_START = new BigDecimal("0.1");
    private static final BigDecimal MQ_END = new BigDecimal("10.0");

    private ItemConstructionMaster newItemConstructionMaster(ItemConstructionMaster item, ItemMaster material) {
        ItemConstructionMaster entity = new ItemConstructionMaster();

        entity.setIcIId(material.getIId());
        entity.setIcParentIId(item.getIcIId());
        entity.setIcEffectiveDate(item.getIcEffectiveDate());
        entity.setIcExpiredDate(item.getIcExpiredDate());
        entity.setIcMaterialUnit(material.getIUnit());
        entity.setIcMaterialQuantity(random.random(MQ_START, MQ_END));
        new InitialData03ItemMaster(date).initializeLossRatio(entity.getIcIId() + entity.getIcParentIId(), entity);

        return entity;
    }

    protected boolean executeRemove() {
        // 変更する構成品目を決定
        ItemConstructionMasterKey item = selectRandomRemoveItem();
        if (item == null) {
            return false;
        }

        logTarget("delete item=%d, parent=%d", item.getIcIId(), item.getIcParentIId());

        // 品目構成マスターから削除
        ItemConstructionMasterDao itemCostructionMasterDao = dbManager.getItemConstructionMasterDao();
        itemCostructionMasterDao.delete(item);

        return true;
    }

    private ItemConstructionMasterKey selectRandomRemoveItem() {
        RandomKeySelector<ItemConstructionMasterKey> selector;
        if (itemConstructionMasterKeySelectorForRemove != null) {
            selector = itemConstructionMasterKeySelectorForRemove;
        } else {
            var list = selectItemConstructionMasterKeyListForRemove(date);
            selector = new RandomKeySelector<>(list, random.getRawRandom(), 0, coverRate);
        }

        ItemConstructionMasterKey key = selector.get();
        return key;
    }

    private static final Object lockItemConstructionMasterKeyListForRemove = new Object();

    private void cacheItemConstructionMasterKeyListForRemove(TgTmSetting setting, LocalDate date) {
        synchronized (lockItemConstructionMasterKeyListForRemove) {
            if (itemConstructionMasterKeySelectorForRemove == null) {
                var log = LoggerFactory.getLogger(BenchOnlineUpdateMaterialTask.class);
                dbManager.execute(setting, () -> {
                    if (itemConstructionMasterKeyListDebug) {
                        {
                            log.info("(remove)ItemConstructionMasterDao.selectAll() start");
                            var list = dbManager.getItemConstructionMasterDao().selectAll();
                            log.info("(remove)ItemConstructionMasterDao.selectAll() end. size={}", list.size());
                        }
                        {
                            log.info("(remove)ItemMasterDao.selectAll() start");
                            var list = dbManager.getItemMasterDao().selectAll();
                            log.info("(remove)ItemMasterDao.selectAll() end. size={}", list.size());
                        }
                    }
                    log.info("(remove)itemCostructionMasterDao.selectByItemType() start");
                    var list = selectItemConstructionMasterKeyListForRemove(date);
                    log.info("(remove)itemCostructionMasterDao.selectByItemType() end. size={}", list.size());
                    itemConstructionMasterKeySelectorForRemove = new RandomKeySelector<>(list, random.getRawRandom(), 0, coverRate);
                });
            }
        }
    }

    private List<ItemConstructionMasterKey> selectItemConstructionMasterKeyListForRemove(LocalDate date) {
        List<ItemType> typeList = Arrays.asList(ItemType.RAW_MATERIAL);
        var dao = dbManager.getItemConstructionMasterDao();
        return dao.selectByItemType(date, typeList);
    }

    // for test
    public static void main(String[] args) {
        try (BenchOnlineUpdateMaterialTask task = new BenchOnlineUpdateMaterialTask(0)) {
            try (CostBenchDbManager manager = createCostBenchDbManagerForTest()) {
                task.setDao(null, manager);

                task.initialize(1, InitialData.DEFAULT_BATCH_DATE);

                task.execute();
            }
        }
    }
}
