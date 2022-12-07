package com.tsurugidb.benchmark.costaccounting.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import com.tsurugidb.benchmark.costaccounting.db.DbmsType;
import com.tsurugidb.benchmark.costaccounting.db.dao.ResultTableDao;
import com.tsurugidb.iceaxe.transaction.TgTxOption;

public class BenchConst {

    public static DbmsType dbmsType() {
        String type = getProperty("dbms.type");
        return DbmsType.of(type);
    }

    public static String jdbcUrl() {
        return getProperty("jdbc.url");
    }

    public static String jdbcUser() {
        return getProperty("jdbc.user");
    }

    public static String jdbcPassword() {
        return getProperty("jdbc.password");
    }

    public static String tsurugiEndpoint() {
        return getProperty("tsurugi.endpoint");
    }

    public static String tsurugiUser() {
        return getProperty("tsurugi.user");
    }

    public static String tsurugiPassword() {
        return getProperty("tsurugi.password");
    }

    public static String docDir() {
        return getProperty("doc.dir");
    }

    public static String tableXlsxPath() {
        return docDir() + "/table.xlsx";
    }

    public static String measurementXlsxPath() {
        return docDir() + "/measurement.xlsx";
    }

    public static final String PACKAGE_DOMAIN = "com.tsurugidb.benchmark.costaccounting.db.domain";
    public static final String PACKAGE_ENTITY = "com.tsurugidb.benchmark.costaccounting.db.entity";

    // batch
    /** sequential / single-tx */
    public static final String SEQUENTIAL_SINGLE_TX = "sequential-single-tx";
    /** sequential / tx per factory */
    public static final String SEQUENTIAL_FACTORY_TX = "sequential-factory-tx";
    /** parallel / single-tx */
    public static final String PARALLEL_SINGLE_TX = "parallel-single-tx";
    /** parallel / tx per factory */
    public static final String PARALLEL_FACTORY_TX = "parallel-factory-tx";

    public static String batchExecuteType() {
        return getProperty("batch.execute.type");
    }

    public static int batchDbManagerType() {
        return getPropertyInt("batch.dbmanager.type", 1);
    }

    public static int batchParallelism() {
        return getPropertyInt("batch.parallelism", 0);
    }

    /**
     * トランザクション分離レベル
     */
    public enum IsolationLevel {
        SERIALIZABLE, READ_COMMITTED;
    }

    public static IsolationLevel batchJdbcIsolationLevel() {
        return getIsolationLevel("batch.jdbc.isolation.level");
    }

    private static IsolationLevel getIsolationLevel(String key) {
        String s = getProperty(key, false);
        if (s == null) {
            return IsolationLevel.READ_COMMITTED;
        }
        try {
            return IsolationLevel.valueOf(s.toUpperCase());
        } catch (Exception e) {
            throw new RuntimeException("invalid IsolationLevel. " + key + "=" + s, e);
        }
    }

    public static String batchTsurugiTxOption() {
        return getProperty("batch.tsurugi.tx.option");
    }

    public static String batchCommandExecuteType() {
        String s = getProperty("batch-command.execute.type", false);
        if (s == null) {
            return String.join(",", SEQUENTIAL_SINGLE_TX, SEQUENTIAL_FACTORY_TX, PARALLEL_SINGLE_TX, PARALLEL_FACTORY_TX);
        }
        return s;
    }

    public static String batchCommandFactoryList() {
        return getProperty("batch-command.factory.list");
    }

    public static List<IsolationLevel> batchCommandIsolationLevel() {
        String s = getProperty("batch-command.isolation.level", false);
        if (s != null) {
            return Arrays.stream(s.split(",")).map(String::trim).map(String::toUpperCase).map(IsolationLevel::valueOf).collect(Collectors.toList());
        }

        if (dbmsType() == DbmsType.TSURUGI) {
            return List.of(IsolationLevel.SERIALIZABLE);
        } else {
            return List.of(IsolationLevel.READ_COMMITTED, IsolationLevel.SERIALIZABLE);
        }
    }

    public static List<TgTxOption> batchCommandTxOption() {
        String s = getProperty("batch-command.tx.option", false);
        if (s != null) {
            return Arrays.stream(s.split(",")).map(String::trim).map(String::toUpperCase).map(option -> {
                switch (option) {
                case "OCC":
                    return TgTxOption.ofOCC();
                default:
                    return TgTxOption.ofLTX(ResultTableDao.TABLE_NAME);
                }
            }).collect(Collectors.toList());
        }

        if (dbmsType() == DbmsType.TSURUGI) {
            return List.of(TgTxOption.ofOCC(), TgTxOption.ofLTX(ResultTableDao.TABLE_NAME));
        } else {
            return List.of(TgTxOption.ofOCC());
        }
    }

    public static final int DECIMAL_SCALE = getPropertyInt("decimal.scale", 20);

    // online
    public static Path onlineLogFilePath(int threadId) {
        String f = getProperty("online.log.file");
        String s = String.format(f, threadId);
        return Paths.get(s);
    }

    public static int onlineDbManagerType() {
        return getPropertyInt("online.dbmanager.type", 1);
    }

    public static IsolationLevel onlineJdbcIsolationLevel() {
        return getIsolationLevel("online.jdbc.isolation.level");
    }

    public static int onlineTaskRatio(String taskName) {
        return getPropertyInt("online.task.ratio." + taskName);
    }

    public static int onlineTaskSleepTime(String taskName) {
        return getPropertyInt("online.task.sleep." + taskName, 0);
    }

    // initial data
    public static int initDbManagerType() {
        return getPropertyInt("init.dbmanager.type", 1);
    }

    public static String initTsurugiTxOption() {
        String s = getProperty("init.tsurugi.tx.option", false);
        return (s != null) ? s : "LTX";
    }

    public static LocalDate initBatchDate() {
        return getPropertyDate("init.batch.date");
    }

    public static int initFactorySize() {
        return getPropertyInt("init.factory.size");
    }

    public static int initItemProductSize() {
        return getPropertyInt("init.item.product.size");
    }

    public static int initItemWorkSize() {
        return getPropertyInt("init.item.work.size");
    }

    public static int initItemMaterialSize() {
        return getPropertyInt("init.item.material.size");
    }

    public static int initItemManufacturingSize() {
        return getPropertyInt("init.item.manufacturing.size");
    }

    public static int initParallelism() {
        int defaultValue = Runtime.getRuntime().availableProcessors();
        return getPropertyInt("init.parallelism", defaultValue);
    }

    // properties

    private static Properties properties;

    private static Properties getProperties(boolean requiredFile) {
        if (properties == null) {
            String s = System.getProperty("property");
            if (s == null) {
                if (requiredFile) {
                    throw new RuntimeException("not found -Dproperty=property-file-path");
                } else {
                    return new Properties();
                }
            }
            Path path = Paths.get(s);
            try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
                Properties p = new Properties();
                p.load(reader);
                properties = p;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return properties;
    }

    private static String getProperty(String key, boolean requiredFile) {
        String spv = System.getProperty(key);
        if (spv == null) {
            Properties p = getProperties(requiredFile);
            return p.getProperty(key);
        } else {
            return spv;
        }
    }

    private static String getProperty(String key) {
        String s = getProperty(key, true);
        if (s == null) {
            throw new RuntimeException("not found key'" + key + "' in property-file");
        }
        return s;
    }

    private static int getPropertyInt(String key, int defaultValue) {
        String s = getProperty(key, false);
        if (s == null) {
            return defaultValue;
        }
        return getPropertyInt(key, s);
    }

    private static int getPropertyInt(String key) {
        String s = getProperty(key);
        return getPropertyInt(key, s);
    }

    private static int getPropertyInt(String key, String value) {
        try {
            String s = value.trim().replaceAll("_", "");
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            throw new RuntimeException("not integer key'" + key + "' in property-file", e);
        }
    }

    private static LocalDate getPropertyDate(String key) {
        try {
            String s = getProperty(key).trim();
            return LocalDate.parse(s);
        } catch (NumberFormatException e) {
            throw new RuntimeException("not date key'" + key + "' in property-file", e);
        }
    }
}
