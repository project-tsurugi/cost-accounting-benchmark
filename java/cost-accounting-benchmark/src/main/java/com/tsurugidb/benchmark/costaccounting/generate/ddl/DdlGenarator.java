package com.tsurugidb.benchmark.costaccounting.generate.ddl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.DbmsType;
import com.tsurugidb.benchmark.costaccounting.generate.OracleDdlGenarator;
import com.tsurugidb.benchmark.costaccounting.generate.PostgresqlDdlGenarator;
import com.tsurugidb.benchmark.costaccounting.generate.TsurugiDdlGenarator;
import com.tsurugidb.benchmark.costaccounting.generate.util.TableSheet;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;

public abstract class DdlGenarator {
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    public static DdlGenarator createDdlGenerator(DbmsType dbmsType) {
        switch (dbmsType) {
        case ORACLE:
            return new OracleDdlGenarator();
        case POSTGRESQL:
            return new PostgresqlDdlGenarator();
        case TSURUGI:
            return new TsurugiDdlGenarator();
        default:
            throw new AssertionError(dbmsType);
        }
    }

    public void writeDdlFile(String ddlFileName) throws Exception {
        File srcFile = new File(BenchConst.tableXlsxPath());
        LOG.info("src={}", srcFile);

        File dstFile = new File(srcFile.getParent(), ddlFileName);
        LOG.info("dst={}", dstFile);

        try (Workbook workbook = WorkbookFactory.create(srcFile); BufferedWriter writer = Files.newBufferedWriter(dstFile.toPath(), StandardCharsets.UTF_8)) {
            for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
                Sheet sheet = workbook.getSheetAt(i);
                TableSheet table = new TableSheet(sheet);

                TableDdlWriter ddlWriter = createTableDdlWriter(table, writer);
                ddlWriter.write();
            }
        }
    }

    public void executeDdl(CostBenchDbManager dbManager) throws IOException {
        File srcFile = new File(BenchConst.tableXlsxPath());
        LOG.info("src={}", srcFile);

        try (Workbook workbook = WorkbookFactory.create(srcFile)) {
            for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
                Sheet sheet = workbook.getSheetAt(i);
                TableSheet table = new TableSheet(sheet);

                TableDdlWriter ddlWriter = createTableDdlWriter(table, null);
                String dropSql = ddlWriter.getDropDdl();
                String createSql = ddlWriter.getCreateDdl();

                dbManager.executeDdl(dropSql, createSql);
            }
        }
    }

    protected abstract TableDdlWriter createTableDdlWriter(TableSheet table, BufferedWriter writer);
}