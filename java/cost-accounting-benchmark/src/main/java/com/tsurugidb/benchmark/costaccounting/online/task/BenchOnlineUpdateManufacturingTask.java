package com.tsurugidb.benchmark.costaccounting.online.task;

import java.math.BigInteger;
import java.time.LocalDate;
import java.util.List;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.UniqueConstraintException;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemManufacturingMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.domain.ItemType;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemManufacturingMaster;
import com.tsurugidb.benchmark.costaccounting.init.InitialData;
import com.tsurugidb.benchmark.costaccounting.init.InitialData04ItemManufacturingMaster;
import com.tsurugidb.iceaxe.transaction.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.TgTxOption;

/**
 * 生産数の変更
 */
public class BenchOnlineUpdateManufacturingTask extends BenchOnlineTask {

    private static final TgTmSetting TX_PRE = TgTmSetting.of( //
            TgTxOption.ofOCC(), //
            TgTxOption.ofRTX());
    private static final TgTmSetting TX_MAIN = TgTmSetting.of( //
            TgTxOption.ofOCC(), //
            TgTxOption.ofLTX(ItemManufacturingMasterDao.TABLE_NAME));

    public BenchOnlineUpdateManufacturingTask() {
        super("update-manufacturing");
    }

    @Override
    protected boolean execute1() {
        int productId = dbManager.execute(TX_PRE, this::selectRandomItemId);
        if (productId < 0) {
            return false;
        }
        logTarget("factory=%d, date=%s, product=%d", factoryId, date, productId);

        int newQuantity = random.random(0, 500) * 100;

        for (;;) {
            boolean ok = dbManager.execute(TX_MAIN, () -> {
                return executeMain(productId, newQuantity);
            });
            if (ok) {
                return true;
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
                return false;
            }
        } else {
            entity.setImManufacturingQuantity(BigInteger.valueOf(newQuantity));
            itemManufacturingMasterDao.update(entity);
        }

        return true;
    }

    protected int selectRandomItemId() {
        ItemMasterDao itemMasterDao = dbManager.getItemMasterDao();
        List<Integer> list = itemMasterDao.selectIdByType(date, ItemType.PRODUCT);
        if (list.isEmpty()) {
            return -1;
        }
        int i = random.nextInt(list.size());
        return list.get(i);
    }

    // for test
    public static void main(String[] args) {
        BenchOnlineUpdateManufacturingTask task = new BenchOnlineUpdateManufacturingTask();

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
