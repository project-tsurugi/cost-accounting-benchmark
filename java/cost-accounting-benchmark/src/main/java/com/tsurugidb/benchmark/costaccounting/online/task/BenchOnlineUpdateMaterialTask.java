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
import com.tsurugidb.benchmark.costaccounting.online.OnlineConfig;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * 原材料の変更
 */
public class BenchOnlineUpdateMaterialTask extends BenchOnlineTask {
    public static final String TASK_NAME = "update-material";

    private static List<ItemConstructionMasterKey> itemConstructionMasterKeyListForAdd;
    private static List<ItemConstructionMasterKey> itemConstructionMasterKeyListForRemove;

    public static void clearPrepareData() {
        itemConstructionMasterKeyListForAdd = null;
        itemConstructionMasterKeyListForRemove = null;
    }

    private TgTmSetting settingMain;

    public BenchOnlineUpdateMaterialTask(int taskId) {
        super(TASK_NAME, taskId);
    }

    @Override
    public void initializeSetting(OnlineConfig config) {
        this.settingMain = config.getSetting(LOG, this, () -> TgTxOption.ofLTX(ItemConstructionMasterDao.TABLE_NAME));
        setTxOptionDescription(settingMain);
    }

    @Override
    public void executePrepare(OnlineConfig config) {
        var setting = TgTmSetting.of(TgTxOption.ofRTX().label(TASK_NAME + ".prepare"));
        var date = config.getBatchDate();

        // 複数スレッドで実行する場合はaddとremoveのキャッシュ作成を同時に実行する
        if (taskId % 2 == 0) {
            cacheItemConstructionMasterKeyListForAdd(dbManager, setting, date);
            cachetItemConstructionMasterKeyListForRemove(dbManager, setting, date);
        } else {
            cachetItemConstructionMasterKeyListForRemove(dbManager, setting, date);
            cacheItemConstructionMasterKeyListForAdd(dbManager, setting, date);
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
            List<Integer> materialList = selectMaterial();
            List<ItemConstructionMaster> childList = itemCostructionMasterDao.selectByParentId(item.getIcIId(), date);
            int materialId = -1;
            for (int j = 0; j < materialList.size(); j++) {
                int mid;
                {
                    int i = random.nextInt(materialList.size());
                    mid = materialList.get(i);
                }
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

        ItemConstructionMaster entity;
        for (;;) {
            ItemConstructionMasterKey key;
            {
                List<ItemConstructionMasterKey> list = itemConstructionMasterKeyListForAdd;
                int i = random.nextInt(list.size());
                key = list.get(i);
            }
            entity = itemCostructionMasterDao.selectById(key.getIcParentIId(), key.getIcIId(), key.getIcEffectiveDate());
            if (entity != null) {
                break;
            }
        }
        return entity;
    }

    private static final boolean itemConstructionMasterKeyListDebug = false;
    private static final Object lockItemConstructionMasterKeyListForAdd = new Object();

    private static void cacheItemConstructionMasterKeyListForAdd(CostBenchDbManager dbManager, TgTmSetting setting, LocalDate date) {
        synchronized (lockItemConstructionMasterKeyListForAdd) {
            if (itemConstructionMasterKeyListForAdd == null) {
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
                    List<ItemType> typeList = Arrays.stream(ItemType.values()).filter(t -> t != ItemType.RAW_MATERIAL).collect(Collectors.toList());
                    log.info("(add)itemCostructionMasterDao.selectByItemType() start {}", typeList);
                    var dao = dbManager.getItemConstructionMasterDao();
                    itemConstructionMasterKeyListForAdd = dao.selectByItemType(date, typeList);
                    log.info("(add)itemCostructionMasterDao.selectByItemType() end. size={}", itemConstructionMasterKeyListForAdd.size());
                });
            }
        }
    }

    private static List<Integer> itemMasterMaterialKeyList;

    private List<Integer> selectMaterial() {
        synchronized (BenchOnlineUpdateMaterialTask.class) {
            if (itemMasterMaterialKeyList == null) {
                ItemMasterDao itemMasterDao = dbManager.getItemMasterDao();
                itemMasterMaterialKeyList = itemMasterDao.selectIdByType(date, ItemType.RAW_MATERIAL);
            }
        }
        return itemMasterMaterialKeyList;
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
        ItemConstructionMasterKey key;
        {
            List<ItemConstructionMasterKey> list = itemConstructionMasterKeyListForRemove;
            if (list.isEmpty()) {
                return null;
            }
            int i = random.nextInt(list.size());
            key = list.remove(i);
        }
        return key;
    }

    private static final Object lockItemConstructionMasterKeyListForRemove = new Object();

    private static void cachetItemConstructionMasterKeyListForRemove(CostBenchDbManager dbManager, TgTmSetting setting, LocalDate date) {
        synchronized (lockItemConstructionMasterKeyListForRemove) {
            if (itemConstructionMasterKeyListForRemove == null) {
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
                    List<ItemType> typeList = Arrays.asList(ItemType.RAW_MATERIAL);
                    log.info("(remove)itemCostructionMasterDao.selectByItemType() start {}", typeList);
                    var dao = dbManager.getItemConstructionMasterDao();
                    itemConstructionMasterKeyListForRemove = dao.selectByItemType(date, typeList);
                    log.info("(remove)itemCostructionMasterDao.selectByItemType() end. size={}", itemConstructionMasterKeyListForRemove.size());
                });
            }
        }
    }

    // for test
    public static void main(String[] args) {
        BenchOnlineUpdateMaterialTask task = new BenchOnlineUpdateMaterialTask(0);

        try (CostBenchDbManager manager = createCostBenchDbManagerForTest()) {
            task.setDao(manager);

            task.initialize(1, InitialData.DEFAULT_BATCH_DATE);

            task.execute();
        }
    }
}
