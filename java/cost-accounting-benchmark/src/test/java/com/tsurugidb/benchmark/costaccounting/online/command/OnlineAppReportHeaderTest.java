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
