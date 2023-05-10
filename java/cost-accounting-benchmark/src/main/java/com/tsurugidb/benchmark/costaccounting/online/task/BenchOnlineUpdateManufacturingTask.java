package com.tsurugidb.benchmark.costaccounting.online.task;

import java.math.BigInteger;
import java.time.LocalDate;
import java.util.List;

import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.UniqueConstraintException;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemManufacturingMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.domain.ItemType;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemManufacturingMaster;
import com.tsurugidb.benchmark.costaccounting.init.InitialData;
import com.tsurugidb.benchmark.costaccounting.init.InitialData04ItemManufacturingMaster;
import com.tsurugidb.benchmark.costaccounting.online.OnlineConfig;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * 生産数の変更
 */
public class BenchOnlineUpdateManufacturingTask extends BenchOnlineTask {
    public static final String TASK_NAME = "update-manufacturing";

    private static List<Integer> itemMasterProductKeylist;

    public static void clearPrepareData() {
        itemMasterProductKeylist = null;
    }

    private TgTmSetting settingMain;

    public BenchOnlineUpdateManufacturingTask(int taskId) {
        super(TASK_NAME, taskId);
    }

    @Override
    public void initializeSetting(OnlineConfig config) {
        this.settingMain = config.getSetting(LOG, this, () -> TgTxOption.ofLTX(ItemManufacturingMasterDao.TABLE_NAME));
        setTxOptionDescription(settingMain);
    }

    @Override
    public void executePrepare(OnlineConfig config) {
        var setting = TgTmSetting.of(TgTxOption.ofRTX().label(TASK_NAME + ".prepare"));
        var date = config.getBatchDate();

        cacheItemMasterProductKeyList(dbManager, setting, date);
    }

    @Override
    protected boolean execute1() {
        int productId = selectRandomItemId();
        if (productId < 0) {
            return false;
        }
        logTarget("factory=%d, date=%s, product=%d", factoryId, date, productId);

        int newQuantity = random.random(0, 500) * 100;

        for (;;) {
            try {
                boolean ok = dbManager.execute(settingMain, () -> {
                    return executeMain(productId, newQuantity);
                });
                if (ok) {
                    return true;
                }
            } catch (UniqueConstraintException e) {
                LOG.debug("duplicate item_manufacturing_master (transaction)", e);
                continue;
            }
        }
    }

    protected boolean executeMain(int productId, int newQuantity) {
        for (;;) {
            boolean ok = executeMain1(productId, newQuantity);
            if (ok) {
                return true;
            }

            switch (1) {
            case 1:
                // 別コネクションでリトライ
                return false;
            default:
                // 同一コネクション内でリトライ（PostgreSQLだと例外発生時に同一コネクションでSQLを発行するとエラーになる）
                continue;
            }
        }
    }

    protected boolean executeMain1(int productId, int newQuantity) {
        ItemManufacturingMasterDao itemManufacturingMasterDao = dbManager.getItemManufacturingMasterDao();
        ItemManufacturingMaster entity = itemManufacturingMasterDao.selectByIdForUpdate(factoryId, productId, date);
        if (entity == null) {
            InitialData04ItemManufacturingMaster initialData = new InitialData04ItemManufacturingMaster(1, date);
            entity = initialData.newItemManufacturingMaster(factoryId, productId);
            entity.setEffectiveDate(date);
            {
                List<ItemManufacturingMaster> list = itemManufacturingMasterDao.selectByIdFuture(productId, productId, date);
                if (!list.isEmpty()) {
                    ItemManufacturingMaster min = list.get(0);
                    entity.setExpiredDate(min.getEffectiveDate().minusDays(1));
                } else {
                    entity.setExpiredDate(LocalDate.of(9999, 12, 31));
                }
            }
            entity.setImManufacturingQuantity(BigInteger.valueOf(newQuantity));
            try {
                itemManufacturingMasterDao.insert(entity);
            } catch (UniqueConstraintException e) {
                LOG.debug("duplicate item_manufacturing_master (insert)", e);
                return false;
            }
        } else {
            entity.setImManufacturingQuantity(BigInteger.valueOf(newQuantity));
            itemManufacturingMasterDao.update(entity);
        }

        return true;
    }

    protected int selectRandomItemId() {
        List<Integer> list = itemMasterProductKeylist;
        if (list.isEmpty()) {
            return -1;
        }
        int i = random.nextInt(list.size());
        return list.get(i);
    }

    private static synchronized void cacheItemMasterProductKeyList(CostBenchDbManager dbManager, TgTmSetting setting, LocalDate date) {
        if (itemMasterProductKeylist == null) {
            var log = LoggerFactory.getLogger(BenchOnlineUpdateManufacturingTask.class);
            dbManager.execute(setting, () -> {
                log.info("ItemMasterDao.selectIdByType(PRODUCT) start");
                var dao = dbManager.getItemMasterDao();
                itemMasterProductKeylist = dao.selectIdByType(date, ItemType.PRODUCT);
                log.info("ItemMasterDao.selectIdByType(PRODUCT) end. size={}", itemMasterProductKeylist.size());
            });
        }
    }

    // for test
    public static void main(String[] args) {
        BenchOnlineUpdateManufacturingTask task = new BenchOnlineUpdateManufacturingTask(0);

        try (CostBenchDbManager manager = createCostBenchDbManagerForTest()) {
            task.setDao(manager);

            task.initialize(1, InitialData.DEFAULT_BATCH_DATE);

            task.execute();
//			manager.execute(() -> {
//				task.executeMain(49666);
//			});
        }
    }
}
