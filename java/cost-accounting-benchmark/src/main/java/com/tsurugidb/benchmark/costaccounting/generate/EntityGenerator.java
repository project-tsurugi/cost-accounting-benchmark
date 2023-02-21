package com.tsurugidb.benchmark.costaccounting.generate;

import java.io.BufferedWriter;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.generate.entity.TableEntityWriter;
import com.tsurugidb.benchmark.costaccounting.generate.util.TableSheet;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;

public class EntityGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(EntityGenerator.class);

    public static void main(String[] args) throws Exception {
        InputStream src = BenchConst.tableXlsxStream(LOG);
        Path dst = Path.of(/* src/main/java/ full path */args[0], BenchConst.PACKAGE_ENTITY.replace('.', '/'));
        new EntityGenerator().main(src, dst);
    }

    private void main(InputStream src, Path dstDir) throws Exception {
        try (Workbook workbook = WorkbookFactory.create(src)) {
            for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
                Sheet sheet = workbook.getSheetAt(i);
                TableSheet table = new TableSheet(sheet);
                String className = table.getClassName();

                Path dstFile = dstDir.resolve(className + ".java");
                LOG.info("dst={}", dstFile);

                try (BufferedWriter writer = Files.newBufferedWriter(dstFile, StandardCharsets.UTF_8)) {
                    TableEntityWriter c = new TableEntityWriter(table, writer);
                    c.convert();
                }
            }
        }
    }
}
