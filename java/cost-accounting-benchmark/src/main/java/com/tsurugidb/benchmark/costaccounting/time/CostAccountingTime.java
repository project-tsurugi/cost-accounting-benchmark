package com.tsurugidb.benchmark.costaccounting.time;

import java.io.IOException;
import java.util.List;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.time.table.ItemMasterTime;
import com.tsurugidb.benchmark.costaccounting.time.table.ResultTableTime;
import com.tsurugidb.benchmark.costaccounting.time.table.TableTime;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst.IsolationLevel;

public class CostAccountingTime {

    private final IsolationLevel isolation;
    private final String txOption;

    public CostAccountingTime(IsolationLevel isolation, String txOption) {
        this.isolation = isolation;
        this.txOption = txOption;
    }

    public int main(List<TimeRecord> records) throws IOException {
        TableTime[] list = { //
                new ItemMasterTime(), //
                new ResultTableTime(), //
        };

        int exitCode = 0;
        try (CostBenchDbManager manager = createDbManager()) {
            for (var time : list) {
                time.initialize(manager, isolation, txOption, records);
                time.execute();
            }
        }
        return exitCode;
    }

    private CostBenchDbManager createDbManager() {
        int type = BenchConst.timeCommandDbManagerType();
        return CostBenchDbManager.createInstance(type, isolation, false);
    }
}
