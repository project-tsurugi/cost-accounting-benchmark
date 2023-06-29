package com.tsurugidb.benchmark.costaccounting.init;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.dao.MeasurementMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.domain.MeasurementType;
import com.tsurugidb.benchmark.costaccounting.db.entity.MeasurementMaster;
import com.tsurugidb.benchmark.costaccounting.generate.util.SheetWrapper;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;

public class InitialData01MeasurementMaster extends InitialData {
    private static final Logger LOG = LoggerFactory.getLogger(InitialData01MeasurementMaster.class);

    public static void main(String... args) throws Exception {
//		new InitialDataMeasureMaster().main(args[0]);
        new InitialData01MeasurementMaster().main(BenchConst.measurementXlsxStream(LOG));
    }

    public InitialData01MeasurementMaster() {
        super(null);
    }

    private void main(InputStream src) throws IOException {
        logStart();

        try (CostBenchDbManager manager = initializeDbManager()) {
            try (Workbook workbook = WorkbookFactory.create(src)) {
                Sheet sheet = workbook.getSheet("measurement");
                TableSheet table = new TableSheet(sheet);

                insertMeasurementMaster(table);
            }
        } finally {
            shutdown();
        }

        logEnd();
    }

    private static class TableSheet extends SheetWrapper {

        public TableSheet(Sheet sheet) {
            super(sheet);
        }

        @Override
        protected int getStartRowIndex() {
            return 2;
        }

        @Override
        public boolean hasData(Row row) {
            if (row == null) {
                return false;
            }
            Cell cell = row.getCell(0);
            if (cell == null) {
                return false;
            }
            return true;
        }
    }

    private void insertMeasurementMaster(TableSheet table) {
        MeasurementMasterDao dao = dbManager.getMeasurementMasterDao();

        var setting = getSetting(MeasurementMasterDao.TABLE_NAME);
        var insertCount = new AtomicInteger();
        dbManager.execute(setting, () -> {
            dao.truncate();
            insertCount.set(0);
            insertMeasureMaster(table, dao, insertCount);
        });
        LOG.info("insert {}={}", MeasurementMasterDao.TABLE_NAME, insertCount.get());
    }

    private void insertMeasureMaster(TableSheet table, MeasurementMasterDao dao, AtomicInteger insertCount) {
        table.getRows().forEachOrdered(row -> {
            MeasurementMaster entity = new MeasurementMaster();
            int c = 0;
            entity.setMUnit(table.getCellAsString(row, c++));
            entity.setMName(table.getCellAsString(row, c++));
            entity.setMType(MeasurementType.of(table.getCellAsString(row, c++)));
            entity.setMDefaultUnit(table.getCellAsString(row, c++));
            entity.setMScale(table.getCellAsDecimal(row, c++));

            dao.insert(entity);
            insertCount.incrementAndGet();
        });
    }
}
