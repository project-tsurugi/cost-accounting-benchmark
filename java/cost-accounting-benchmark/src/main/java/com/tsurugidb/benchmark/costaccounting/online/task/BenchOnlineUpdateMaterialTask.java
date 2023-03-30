package com.tsurugidb.benchmark.costaccounting.online.task;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
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
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * 原材料の変更
 */
public class BenchOnlineUpdateMaterialTask extends BenchOnlineTask {
    private static final Logger LOG = LoggerFactory.getLogger(BenchOnlineUpdateMaterialTask.class);

    public static final String TASK_NAME = "update-material";

    private final TgTmSetting settingMain;

    public BenchOnlineUpdateMaterialTask() {
        super(TASK_NAME);
        this.settingMain = getSetting(() -> TgTxOption.ofLTX(ItemConstructionMasterDao.TABLE_NAME));
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

    private static final boolean itemConstructionMasterKeyListDebug = false;
    private static List<ItemConstructionMasterKey> itemConstructionMasterKeyListForAdd;

    private ItemConstructionMaster selectRandomAddItem() {
        ItemConstructionMasterDao itemCostructionMasterDao = dbManager.getItemConstructionMasterDao();

        ItemConstructionMaster entity;
        for (;;) {
            ItemConstructionMasterKey key;
            synchronized (BenchOnlineUpdateMaterialTask.class) {
                if (itemConstructionMasterKeyListForAdd == null) {
                    if (itemConstructionMasterKeyListDebug) {
                        {
                            LOG.info("(add)ItemConstructionMasterDao.selectAll() start");
                            var list = dbManager.getItemConstructionMasterDao().selectAll();
                            LOG.info("(add)ItemConstructionMasterDao.selectAll() end. size={}", list.size());
                        }
                        {
                            LOG.info("(add)ItemMasterDao.selectAll() start");
                            var list = dbManager.getItemMasterDao().selectAll();
                            LOG.info("(add)ItemMasterDao.selectAll() end. size={}", list.size());
                        }
                    }
                    List<ItemType> typeList = Arrays.stream(ItemType.values()).filter(t -> t != ItemType.RAW_MATERIAL).collect(Collectors.toList());
                    LOG.info("(add)itemCostructionMasterDao.selectByItemType() start {}", typeList);
                    itemConstructionMasterKeyListForAdd = itemCostructionMasterDao.selectByItemType(date, typeList);
                    LOG.info("(add)itemCostructionMasterDao.selectByItemType() end. size={}", itemConstructionMasterKeyListForAdd.size());
                }
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

    private static List<ItemConstructionMasterKey> itemConstructionMasterKeyListForRemove = null;

    private ItemConstructionMasterKey selectRandomRemoveItem() {
        ItemConstructionMasterKey key;
        synchronized (BenchOnlineUpdateMaterialTask.class) {
            if (itemConstructionMasterKeyListForRemove == null) {
                if (itemConstructionMasterKeyListDebug) {
                    {
                        LOG.info("(remove)ItemConstructionMasterDao.selectAll() start");
                        var list = dbManager.getItemConstructionMasterDao().selectAll();
                        LOG.info("(remove)ItemConstructionMasterDao.selectAll() end. size={}", list.size());
                    }
                    {
                        LOG.info("(remove)ItemMasterDao.selectAll() start");
                        var list = dbManager.getItemMasterDao().selectAll();
                        LOG.info("(remove)ItemMasterDao.selectAll() end. size={}", list.size());
                    }
                }
                List<ItemType> typeList = Arrays.asList(ItemType.RAW_MATERIAL);
                LOG.info("(remove)itemCostructionMasterDao.selectByItemType() start {}", typeList);
                ItemConstructionMasterDao itemCostructionMasterDao = dbManager.getItemConstructionMasterDao();
                itemConstructionMasterKeyListForRemove = itemCostructionMasterDao.selectByItemType(date, typeList);
                LOG.info("(remove)itemCostructionMasterDao.selectByItemType() end. size={}", itemConstructionMasterKeyListForRemove.size());
            }
            List<ItemConstructionMasterKey> list = itemConstructionMasterKeyListForRemove;
            if (list.isEmpty()) {
                return null;
            }
            int i = random.nextInt(list.size());
            key = list.remove(i);
        }
        return key;
    }

    // for test
    public static void main(String[] args) {
        BenchOnlineUpdateMaterialTask task = new BenchOnlineUpdateMaterialTask();

        try (CostBenchDbManager manager = createCostBenchDbManagerForTest()) {
            task.setDao(manager);

            task.initialize(1, InitialData.DEFAULT_BATCH_DATE);

            task.execute();
        }
    }
}
