package com.tsurugidb.benchmark.costaccounting.time.table;

import java.math.BigDecimal;
import java.text.MessageFormat;
import java.time.LocalDate;

import com.tsurugidb.benchmark.costaccounting.db.dao.ItemMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.domain.ItemType;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemMaster;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;

public class ItemMasterTime extends TableTime {

    private ItemMasterDao dao;

    public ItemMasterTime() {
        super(ItemMasterDao.TABLE_NAME);
    }

    @Override
    public void execute() {
        this.dao = dbManager.getItemMasterDao();
        clearItemMaster();
        insert();
        selectAll();
        selectPoint();
        selectRangeScan();
    }

    private void clearItemMaster() {
        var setting = TgTmSetting.of(TgTxOption.ofLTX(tableName));
        dbManager.execute(setting, () -> {
            dao.deleteAll();
        });
    }

    private void insert() {
        execute("insert", () -> {
            for (int i = 0; i < size; i++) {
                var entity = createItemMaster(i);
                dao.insert(entity);
            }
        });
    }

    private static final LocalDate START_DATE = LocalDate.of(2022, 12, 1);

    private ItemMaster createItemMaster(int i) {
        var entity = new ItemMaster();
        entity.setIId(i);
        entity.setIEffectiveDate(START_DATE);
        entity.setIExpiredDate(LocalDate.of(2022, 12, 31));
        entity.setIName("name" + i);
        entity.setIType(ItemType.PRODUCT);
        entity.setIUnit("g");
        entity.setIWeightRatio(BigDecimal.valueOf(100));
        entity.setIWeightUnit("g");
        entity.setIPrice(BigDecimal.valueOf(300));
        entity.setIPriceUnit("g");
        return entity;
    }

    private void selectAll() {
        execute("selectAll", 3, () -> {
            var list = dao.selectAll();
            if (list.size() != size) {
                throw new AssertionError(MessageFormat.format("size unmatch. list={0}, size={1}", list.size(), size));
            }
        });
    }

    private void selectPoint() {
        execute("select(point)", 3, () -> {
            for (int i = 0; i < size; i++) {
                var entity = dao.selectByKey(i, START_DATE);
                if (entity == null) {
                    throw new AssertionError("id=" + i);
                }
            }
        });
    }

    private void selectRangeScan() {
        execute("select(range-scan)", 3, () -> {
            var date = LocalDate.of(2022, 12, 13);
            for (int i = 0; i < size; i++) {
                var entity = dao.selectById(i, date);
                if (entity == null) {
                    throw new AssertionError("id=" + i);
                }
            }
        });
    }
}
