package com.tsurugidb.benchmark.costaccounting.ddl;

import java.io.BufferedWriter;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.BenchConst;
import com.tsurugidb.benchmark.costaccounting.ddl.common.TableSheet;
import com.tsurugidb.benchmark.costaccounting.ddl.entity.TableEntityWriter;

public class EntityGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(EntityGenerator.class);

    public static void main(String[] args) throws Exception {
        // new EntityExample().main(args[0], args[1]);
        new EntityGenerator().main(BenchConst.tableXlsxPath(), BenchConst.srcDir() + "/" + BenchConst.PACKAGE_ENTITY.replace('.', '/'));
    }

    private void main(String src, String dst) throws Exception {
        File srcFile = new File(src);
        LOG.info("src={}", srcFile);

        Path dstDir = Paths.get(dst);

        try (Workbook workbook = WorkbookFactory.create(srcFile)) {
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
