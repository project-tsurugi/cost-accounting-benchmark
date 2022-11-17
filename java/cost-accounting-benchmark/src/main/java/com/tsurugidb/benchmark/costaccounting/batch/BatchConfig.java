package com.tsurugidb.benchmark.costaccounting.batch;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.tsurugidb.benchmark.costaccounting.db.dao.ResultTableDao;
import com.tsurugidb.iceaxe.transaction.TgTxOption;

public class BatchConfig {

    private final String executeType;
    private final LocalDate batchDate;
    private List<Integer> factoryList;
    private final int commitRatio;
    private TgTxOption defaultTxOption;
    private Map<Integer, TgTxOption> txOptionMap;

    public BatchConfig(String executeType, LocalDate batchDate, List<Integer> factoryList, int commitRatio) {
        this.executeType = executeType;
        this.batchDate = batchDate;
        this.factoryList = factoryList;
        this.commitRatio = commitRatio;
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

    public TgTxOption getTxOption(int factoryId) {
        if (this.txOptionMap == null) {
            return this.defaultTxOption;
        }

        var txOption = txOptionMap.get(factoryId);
        return (txOption != null) ? txOption : this.defaultTxOption;
    }
}
