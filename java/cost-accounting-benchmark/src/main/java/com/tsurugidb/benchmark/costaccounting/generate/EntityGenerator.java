/*
 * Copyright 2023-2025 Project Tsurugi.
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
