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
package com.tsurugidb.benchmark.costaccounting.online.command;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.LocalDate;

import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.costaccounting.db.DbmsType;
import com.tsurugidb.benchmark.costaccounting.online.OnlineConfig;

class OnlineAppReportHeaderTest {

    @Test
    void online() {
        var header = OnlineAppReportHeader.ofOnline(DbmsType.TSURUGI, "label1", "VAR3-1", "LTX", 100);
        var line = header.toString();
        var parse = OnlineAppReportHeader.parse(line);
        assertEquals(header, parse);
    }

    @Test
    void batch() {
        var onlineConfig = new OnlineConfig(LocalDate.of(2023, 6, 6));
        onlineConfig.setTxOption("online", "VAR3-1");
        onlineConfig.setTxOption("periodic", "LTX");
        onlineConfig.setCoverRate(100);
        var header = OnlineAppReportHeader.ofBatch(DbmsType.TSURUGI, "label1", "scope", "LTX", onlineConfig);
        var line = header.toString();
        var parse = OnlineAppReportHeader.parse(line);
        assertEquals(header, parse);
    }
}
