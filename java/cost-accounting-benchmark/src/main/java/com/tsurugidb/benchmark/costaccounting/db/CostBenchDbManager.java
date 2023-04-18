package com.tsurugidb.benchmark.costaccounting.db;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.db.BenchDbCounter.CounterName;
import com.tsurugidb.benchmark.costaccounting.db.dao.CostMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.FactoryMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemConstructionMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemManufacturingMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.MeasurementMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ResultTableDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.StockHistoryDao;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.CostBenchDbManagerIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.jdbc.CostBenchDbManagerJdbc;
import com.tsurugidb.benchmark.costaccounting.db.tsubakuro.CostBenchDbManagerTsubakuro;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst.DbManagerType;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst.IsolationLevel;
import com.tsurugidb.benchmark.costaccounting.util.MeasurementUtil;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;

public abstract class CostBenchDbManager implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(CostBenchDbManager.class);

    protected final static BenchDbCounter counter = new BenchDbCounter();

    private final String name;
    private final boolean isTsurugi;
    private MeasurementMasterDao measurementMasterDao;
    private FactoryMasterDao factoryMasterDao;
    private ItemMasterDao itemMasterDao;
    private ItemConstructionMasterDao itemConstructionMasterDao;
    private ItemManufacturingMasterDao itemManufacturingMasterDao;
    private CostMasterDao costMasterDao;
    private StockHistoryDao stockHistoryDao;
    private ResultTableDao resultTableDao;

    private boolean isSingleTransaction = false;

    public static CostBenchDbManager createInstance(DbManagerType type, IsolationLevel isolationLevel, boolean isMultiSession) {
        CostBenchDbManager manager;
        {
            switch (type) {
            case JDBC:
                manager = new CostBenchDbManagerJdbc(isolationLevel);
                break;
            case ICEAXE:
                manager = new CostBenchDbManagerIceaxe(isMultiSession);
                break;
            case TSUBAKURO:
                manager = new CostBenchDbManagerTsubakuro();
                break;
            default:
                throw new AssertionError(type);
            }
        }
        LOG.info("using {}", manager.getClass().getSimpleName());

        MeasurementUtil.initialize(manager.getMeasurementMasterDao());

        return manager;
    }

    public CostBenchDbManager(String name, boolean isTsurugi) {
        this.name = name;
        this.isTsurugi = isTsurugi;
    }

    public String getName() {
        return this.name;
    }

    public boolean isTsurugi() {
        return this.isTsurugi;
    }

    // DAO

    public synchronized MeasurementMasterDao getMeasurementMasterDao() {
        if (measurementMasterDao == null) {
            this.measurementMasterDao = newMeasurementMasterDao();
        }
        return measurementMasterDao;
    }

    protected abstract MeasurementMasterDao newMeasurementMasterDao();

    public synchronized FactoryMasterDao getFactoryMasterDao() {
        if (factoryMasterDao == null) {
            this.factoryMasterDao = newFactoryMasterDao();
        }
        return factoryMasterDao;
    }

    protected abstract FactoryMasterDao newFactoryMasterDao();

    public synchronized ItemMasterDao getItemMasterDao() {
        if (itemMasterDao == null) {
            this.itemMasterDao = newItemMasterDao();
        }
        return itemMasterDao;
    }

    protected abstract ItemMasterDao newItemMasterDao();

    public synchronized ItemConstructionMasterDao getItemConstructionMasterDao() {
        if (itemConstructionMasterDao == null) {
            this.itemConstructionMasterDao = newItemConstructionMasterDao();
        }
        return itemConstructionMasterDao;
    }

    protected abstract ItemConstructionMasterDao newItemConstructionMasterDao();

    public synchronized ItemManufacturingMasterDao getItemManufacturingMasterDao() {
        if (itemManufacturingMasterDao == null) {
            this.itemManufacturingMasterDao = newItemManufacturingMasterDao();
        }
        return itemManufacturingMasterDao;
    }

    protected abstract ItemManufacturingMasterDao newItemManufacturingMasterDao();

    public synchronized CostMasterDao getCostMasterDao() {
        if (costMasterDao == null) {
            this.costMasterDao = newCostMasterDao();
        }
        return costMasterDao;
    }

    protected abstract CostMasterDao newCostMasterDao();

    public synchronized StockHistoryDao getStockHistoryDao() {
        if (stockHistoryDao == null) {
            this.stockHistoryDao = newStockHistoryDao();
        }
        return stockHistoryDao;
    }

    protected abstract StockHistoryDao newStockHistoryDao();

    public synchronized ResultTableDao getResultTableDao() {
        if (resultTableDao == null) {
            this.resultTableDao = newResultTableDao();
        }
        return resultTableDao;
    }

    protected abstract ResultTableDao newResultTableDao();

    // execute

    public void setSingleTransaction(boolean isSingleTransaction) {
        this.isSingleTransaction = isSingleTransaction;
    }

    public boolean isSingleTransaction() {
        return this.isSingleTransaction;
    }

    public abstract void executeDdl(String... sqls);

    public abstract void execute(TgTmSetting setting, Runnable runnable);

    public abstract <T> T execute(TgTmSetting setting, Supplier<T> supplier);

    public abstract boolean isRetriable(Throwable t);

    public final void commit() {
        commit(null);
    }

    public abstract void commit(Runnable listener);

    public final void rollback() {
        rollback(null);
    }

    public abstract void rollback(Runnable listener);

    @Override
    public abstract void close();

    public static void initCounter() {
        counter.reset();
    }

    public void incrementTaskCounter(String label, CounterName counterName) {
        counter.increment(label, counterName);
    }

    /**
     * カウンターのレポートを作成する
     *
     * @return レポートの文字列
     */
    public static String createCounterReport() {
        var counterNames = List.of(CounterName.BEGIN_TX, CounterName.TRY_COMMIT, CounterName.ABORTED, CounterName.SUCCESS);
        List<String> labels = counter.getCountMap().keySet().stream().sorted().collect(Collectors.toList());

        // レポートの作成
        try (var sw = new StringWriter(); var pw = new PrintWriter(sw)) {
            // ヘッダーを生成
            pw.print("TX_LABELS");
            for (var name : counterNames) {
                pw.print(',');
                pw.print(name);
            }
            pw.println();

            // 本体
            for (String label : labels) {
                pw.print(label);
                for (var name : counterNames) {
                    pw.print(',');
                    pw.print(counter.getCount(label, name));
                }
                pw.println();
            }

            return sw.toString();
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }
    }

    public static BenchDbCounter getCounter() {
        return counter;
    }
}
