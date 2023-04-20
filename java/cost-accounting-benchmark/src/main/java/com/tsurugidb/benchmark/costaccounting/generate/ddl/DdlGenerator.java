package com.tsurugidb.benchmark.costaccounting.generate.ddl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.DbmsType;
import com.tsurugidb.benchmark.costaccounting.generate.OracleDdlGenerator;
import com.tsurugidb.benchmark.costaccounting.generate.PostgresqlDdlGenerator;
import com.tsurugidb.benchmark.costaccounting.generate.TsurugiDdlGenerator;
import com.tsurugidb.benchmark.costaccounting.generate.util.TableSheet;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;

public abstract class DdlGenerator {
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    public static DdlGenerator createDdlGenerator(DbmsType dbmsType) {
        switch (dbmsType) {
        case ORACLE:
            return new OracleDdlGenerator();
        case POSTGRESQL:
            return new PostgresqlDdlGenerator();
        case TSURUGI:
            return new TsurugiDdlGenerator();
        default:
            throw new AssertionError(dbmsType);
        }
    }

    public void writeDdlFile(String ddlFileName) throws Exception {
        Path dstFile = Path.of(BenchConst.docDir(), ddlFileName);
        writeDdlFile(dstFile);
    }

    public void writeDdlFile(Path ddlFilePath) throws Exception {
        InputStream srcFile = BenchConst.tableXlsxStream(LOG);

        File dstFile = ddlFilePath.toFile();
        LOG.info("dst={}", dstFile);

        try (Workbook workbook = WorkbookFactory.create(srcFile); //
                BufferedWriter writer = Files.newBufferedWriter(dstFile.toPath(), StandardCharsets.UTF_8)) {
            for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
                Sheet sheet = workbook.getSheetAt(i);
                TableSheet table = new TableSheet(sheet);

                TableDdlWriter ddlWriter = createTableDdlWriter(table, writer);
                ddlWriter.write();
            }
        }
    }

    public void executeDdl(CostBenchDbManager dbManager) throws IOException, InterruptedException {
        InputStream srcFile = BenchConst.tableXlsxStream(LOG);

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
