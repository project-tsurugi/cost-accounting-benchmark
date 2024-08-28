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

import java.io.IOException;

import com.tsurugidb.benchmark.costaccounting.db.DbmsType;
import com.tsurugidb.benchmark.costaccounting.generate.ddl.DdlGenerator;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;

public class InitialData00CreateTable extends InitialData {

    public static void main(String... args) throws Exception {
        var dbmsType = BenchConst.dbmsType();
        new InitialData00CreateTable().main(dbmsType);
    }

    public InitialData00CreateTable() {
        super(null);
    }

    private void main(DbmsType dbmsType) throws IOException, InterruptedException {
        logStart();

        var generator = DdlGenerator.createDdlGenerator(dbmsType);
        try (var manager = initializeDbManager()) {
            generator.executeDdl(manager);
        } finally {
            shutdown();
        }

        logEnd();
    }
}
