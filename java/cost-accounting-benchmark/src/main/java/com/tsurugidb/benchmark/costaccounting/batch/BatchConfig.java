package com.tsurugidb.benchmark.costaccounting.batch;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager.DbManagerPurpose;
import com.tsurugidb.benchmark.costaccounting.db.dao.ResultTableDao;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst.IsolationLevel;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

public class BatchConfig {

    private final DbManagerPurpose purpose;
    private final String executeType;
    private final LocalDate batchDate;
    private List<Integer> factoryList;
    private int threadSize;
    private final int commitRatio;
    private IsolationLevel isolationLevel;
    private TgTxOption defaultTxOption;
    private Map<Integer, TgTxOption> txOptionMap;

    public BatchConfig(DbManagerPurpose purpose, String executeType, LocalDate batchDate, List<Integer> factoryList, int commitRatio) {
        this.purpose = purpose;
        this.executeType = executeType;
        this.batchDate = batchDate;
        this.factoryList = factoryList;
        this.commitRatio = commitRatio;
    }

    public DbManagerPurpose getDbManagerPurpose() {
        return this.purpose;
    }

    public String getExecuteType() {
        return this.executeType;
    }

    public LocalDate getBatchDate() {
        return this.batchDate;
    }

    public void setFactoryList(List<Integer> idList) {
        this.factoryList = idList;
    }

    public List<Integer> getFactoryList() {
        return this.factoryList;
    }

    public void setThreadSize(int threadSize) {
        this.threadSize = threadSize;
    }

    public int getThreadSize() {
        if (this.threadSize <= 0) {
            return factoryList.size();
        }
        return this.threadSize;
    }

    public int getCommitRatio() {
        return this.commitRatio;
    }

    public void setDefaultTxOption(TgTxOption txOption) {
        this.defaultTxOption = txOption;
    }

    public void setTxOption(int factoryId, TgTxOption txOption) {
        if (this.txOptionMap == null) {
            this.txOptionMap = new TreeMap<>();
        }
        txOptionMap.put(factoryId, txOption);
    }

    public Map<Integer, TgTxOption> getFactoryMap() {
        return this.txOptionMap;
    }

    public void setIsolationLevel(IsolationLevel level) {
        this.isolationLevel = level;
    }

    public IsolationLevel getIsolationLevel() {
        return this.isolationLevel;
    }

    public void setTxOptions(String txOptionProperty) {
        String s = txOptionProperty.toUpperCase();

        int n = s.indexOf('[');
        if (n >= 0) {
            TgTxOption txOption;
            if (s.startsWith("LTX")) {
                txOption = createTxOption("LTX");
                setDefaultTxOption(createTxOption("OCC"));
            } else {
                txOption = createTxOption("OCC");
                setDefaultTxOption(createTxOption("LTX"));
            }

            int m = s.indexOf(']', n);
            var list = StringUtil.toIntegerList(s.substring(n + 1, m));
            for (int factoryId : list) {
                setTxOption(factoryId, txOption);
            }

            return;
        }

        setDefaultTxOption(createTxOption(s));
    }

    private static TgTxOption createTxOption(String s) {
        switch (s) {
        case "LTX":
            return TgTxOption.ofLTX(ResultTableDao.TABLE_NAME);
        case "OCC":
            return TgTxOption.ofOCC();
        default:
            throw new UnsupportedOperationException(s);
        }
    }

    public TgTxOption getDefaultTxOption() {
        return this.defaultTxOption;
    }

    public TgTxOption getTxOption(int factoryId) {
        if (this.txOptionMap == null) {
            return this.defaultTxOption;
        }

        var txOption = txOptionMap.get(factoryId);
        return (txOption != null) ? txOption : this.defaultTxOption;
    }
}
