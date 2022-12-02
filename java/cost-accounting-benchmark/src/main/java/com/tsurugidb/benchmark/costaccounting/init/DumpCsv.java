package com.tsurugidb.benchmark.costaccounting.init;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.dao.CostMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.FactoryMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemConstructionMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemManufacturingMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.MeasurementMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ResultTableDao;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;

public class DumpCsv {
    private static final Logger LOG = LoggerFactory.getLogger(DumpCsv.class);

    public static void main(String... args) throws IOException {
        var outputDir = Path.of(args[0]);
        var list = List.of(Arrays.copyOfRange(args, 1, args.length));
        new DumpCsv().main(outputDir, list);
    }

    private CostBenchDbManager dbManager;

    public void main(Path outputDir, List<String> tableList) throws IOException {
        if (tableList.isEmpty()) {
            tableList = List.of( //
                    MeasurementMasterDao.TABLE_NAME, //
                    FactoryMasterDao.TABLE_NAME, //
                    ItemMasterDao.TABLE_NAME, //
                    ItemConstructionMasterDao.TABLE_NAME, //
                    ItemManufacturingMasterDao.TABLE_NAME, //
                    CostMasterDao.TABLE_NAME, //
                    ResultTableDao.TABLE_NAME);
        }

        try (CostBenchDbManager manager = InitialData.createDbManager()) {
            this.dbManager = manager;

            for (var tableName : tableList) {
                dump(outputDir, tableName);
            }
        }
    }

    private void dump(Path outputDir, String tableName) throws IOException {
        var csvFile = outputDir.resolve(tableName + ".csv");
        LOG.info("write {}", csvFile);

        try (var writer = Files.newBufferedWriter(csvFile, StandardCharsets.UTF_8)) {
            switch (tableName) {
            case MeasurementMasterDao.TABLE_NAME:
                dumpMeasurementMaster(writer);
                break;
            case FactoryMasterDao.TABLE_NAME:
                dumpFactoryMaster(writer);
                break;
            case ItemMasterDao.TABLE_NAME:
                dumpItemMaster(writer);
                break;
            case ItemConstructionMasterDao.TABLE_NAME:
                dumpItemConstructionMaster(writer);
                break;
            case ItemManufacturingMasterDao.TABLE_NAME:
                dumpItemManufacturingMaster(writer);
                break;
            case CostMasterDao.TABLE_NAME:
                dumpCostMaster(writer);
                break;
            case ResultTableDao.TABLE_NAME:
                dumpResultTable(writer);
                break;
            default:
                throw new UnsupportedOperationException(tableName);
            }
        }
    }

    private void dumpMeasurementMaster(BufferedWriter writer) {
        var dao = dbManager.getMeasurementMasterDao();
        dump(writer, dao::forEach, entity -> entity.toCsv("\n"));
    }

    private void dumpFactoryMaster(BufferedWriter writer) {
        var dao = dbManager.getFactoryMasterDao();
        dump(writer, dao::forEach, entity -> entity.toCsv("\n"));
    }

    private void dumpItemMaster(BufferedWriter writer) {
        var dao = dbManager.getItemMasterDao();
        dump(writer, dao::forEach, entity -> entity.toCsv("\n"));
    }

    private void dumpItemConstructionMaster(BufferedWriter writer) {
        var dao = dbManager.getItemConstructionMasterDao();
        dump(writer, dao::forEach, entity -> entity.toCsv("\n"));
    }

    private void dumpItemManufacturingMaster(BufferedWriter writer) {
        var dao = dbManager.getItemManufacturingMasterDao();
        dump(writer, dao::forEach, entity -> entity.toCsv("\n"));
    }

    private void dumpCostMaster(BufferedWriter writer) {
        var dao = dbManager.getCostMasterDao();
        dump(writer, dao::forEach, entity -> entity.toCsv("\n"));
    }

    private void dumpResultTable(BufferedWriter writer) {
        var dao = dbManager.getResultTableDao();
        dump(writer, dao::forEach, entity -> entity.toCsv("\n"));
    }

    private <E> void dump(BufferedWriter writer, Consumer<Consumer<E>> forEach, Function<E, String> converter) {
        var setting = TgTmSetting.of(TgTxOption.ofRTX());
        dbManager.execute(setting, () -> {
            forEach.accept(entity -> {
                String line = converter.apply(entity);
                try {
                    writer.write(line);
                } catch (IOException e) {
                    throw new UncheckedIOException(e.getMessage(), e);
                }
            });
        });
    }
}