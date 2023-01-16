package com.tsurugidb.benchmark.costaccounting.time.table;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;

import com.tsurugidb.benchmark.costaccounting.db.dao.ResultTableDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.ResultTable;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

public class ResultTableTime extends TableTime {

    private ResultTableDao dao;
    private int factoryId = 2;

    public ResultTableTime() {
        super(ResultTableDao.TABLE_NAME);
    }

    @Override
    public void execute() {
        this.dao = dbManager.getResultTableDao();
        clear();
        insert();
        if (BenchConst.timeCommandExecute(tableName, "delete")) {
            delete();
        } else {
            deleteInsert();
        }
    }

    private void clear() {
        if (!BenchConst.timeCommandExecute(tableName, "clear")) {
            return;
        }

        var setting = TgTmSetting.of(TgTxOption.ofLTX(tableName));
        dbManager.execute(setting, () -> {
            dao.truncate();
        });
    }

    private void insert() {
        execute("insert", () -> {
            for (int f = sizeAdjustStart; f <= sizeAdjustEnd; f++) {
                for (int i = 0; i < size; i++) {
                    var entity = createResultTable(f, i);
                    dao.insert(entity);
                }
            }
        });
    }

    private static final LocalDate BASE_DATE = LocalDate.of(2022, 12, 13);

    private ResultTable createResultTable(int factoryId, int i) {
        var entity = new ResultTable();
        entity.setRFId(factoryId);
        entity.setRManufacturingDate(BASE_DATE);
        entity.setRProductIId(i);
        entity.setRParentIId(i);
        entity.setRIId(1);
        entity.setRManufacturingQuantity(BigInteger.valueOf(101));
        entity.setRWeightUnit("g");
        entity.setRWeight(BigDecimal.valueOf(102));
        entity.setRWeightTotalUnit("g");
        entity.setRWeightTotal(BigDecimal.valueOf(103));
        entity.setRWeightRatio(BigDecimal.valueOf(104));
        entity.setRStandardQuantityUnit("g");
        entity.setRStandardQuantity(BigDecimal.valueOf(105));
        entity.setRRequiredQuantityUnit("g");
        entity.setRRequiredQuantity(BigDecimal.valueOf(106));
        entity.setRUnitCost(BigDecimal.valueOf(107));
        entity.setRTotalUnitCost(BigDecimal.valueOf(108));
        entity.setRManufacturingCost(BigDecimal.valueOf(109));
        entity.setRTotalManufacturingCost(BigDecimal.valueOf(110));
        return entity;
    }

    private void delete() {
        execute("delete", () -> {
            dao.deleteByFactory(factoryId, BASE_DATE);
        });
    }

    private void deleteInsert() {
        execute("deleteInsert", () -> {
            dao.deleteByFactory(factoryId, BASE_DATE);
            for (int i = 0; i < size; i++) {
                var entity = createResultTable(factoryId, i);
                dao.insert(entity);
            }
        });
    }
}
