package com.tsurugidb.benchmark.costaccounting.ddl.ddl;

import java.io.BufferedWriter;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import com.tsurugidb.benchmark.costaccounting.BenchConst;
import com.tsurugidb.benchmark.costaccounting.ddl.common.TableSheet;

public abstract class DdlGenarator {

    public void execute() throws Exception {
        File srcFile = new File(BenchConst.tableXlsxPath());
        System.out.println("src = " + srcFile);

        File dstFile = new File(srcFile.getParent(), getDdlFileName());
        System.out.println("dst = " + dstFile);

        try (Workbook workbook = WorkbookFactory.create(srcFile); BufferedWriter writer = Files.newBufferedWriter(dstFile.toPath(), StandardCharsets.UTF_8)) {
            for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
                Sheet sheet = workbook.getSheetAt(i);
                TableSheet table = new TableSheet(sheet);

                TableDdlWriter c = createTableDdlWriter(table, writer);
                c.convert();
            }
        }
    }

    protected abstract String getDdlFileName();

    protected abstract TableDdlWriter createTableDdlWriter(TableSheet table, BufferedWriter writer);
}
