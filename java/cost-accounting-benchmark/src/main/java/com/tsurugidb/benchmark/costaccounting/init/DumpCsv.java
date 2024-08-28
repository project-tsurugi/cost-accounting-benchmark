/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import com.tsurugidb.benchmark.costaccounting.db.dao.StockHistoryDao;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

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
                    StockHistoryDao.TABLE_NAME, //
                    ResultTableDao.TABLE_NAME);
        }

        if (!Files.exists(outputDir)) {
            Files.createDirectories(outputDir);
        }
        try (CostBenchDbManager manager = InitialData.createDbManager()) {
            this.dbManager = manager;

            for (var tableName : tableList) {
                var outputFile = outputDir.resolve(tableName + ".csv");
                LOG.info("write {}", outputFile);
                dumpTable(outputFile, tableName);
            }
        }
    }

    public void dump(Path outputFile, String tableName) throws IOException {
        try (CostBenchDbManager manager = InitialData.createDbManager()) {
            this.dbManager = manager;

            dumpTable(outputFile, tableName);
        }
    }

    private void dumpTable(Path outputFile, String tableName) throws IOException {
        try (var writer = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8)) {
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
            case StockHistoryDao.TABLE_NAME:
                dumpStockHistory(writer);
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

    private void dumpStockHistory(BufferedWriter writer) {
        var dao = dbManager.getStockHistoryDao();
        dump(writer, dao::forEach, entity -> entity.toCsv("\n"));
    }

    private void dumpResultTable(BufferedWriter writer) {
        var dao = dbManager.getResultTableDao();
        dump(writer, dao::forEach, entity -> entity.toCsv("\n"));
    }

    private <E> void dump(BufferedWriter writer, Consumer<Consumer<E>> forEach, Function<E, String> converter) {
        var setting = TgTmSetting.of(TgTxOption.ofRTX().label("table dump"));
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
