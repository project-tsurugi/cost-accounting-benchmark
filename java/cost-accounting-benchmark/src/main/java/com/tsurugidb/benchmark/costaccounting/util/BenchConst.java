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

public class BenchConst {
    /** 暫定回避 */ // TODO 暫定回避策廃止
    public static final boolean WORKAROUND = true;

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
    /** parallel / session per factory */
    public static final String PARALLEL_FACTORY_SESSION = "parallel-factory-session";

    public static String batchExecuteType() {
        return getProperty("batch.execute.type");
    }

    public enum DbManagerType {
        JDBC, ICEAXE, TSUBAKURO
    }

    public static DbManagerType batchDbManagerType() {
        return getDbManagerType("batch.dbmanager.type");
    }

    private static DbManagerType getDbManagerType(String key) {
        String s = getProperty(key, null);
        if (s != null) {
            return DbManagerType.valueOf(s.toUpperCase());
        }
        switch (dbmsType()) {
        case TSURUGI:
            return DbManagerType.ICEAXE;
        default:
            return DbManagerType.JDBC;
        }
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
        String s = getProperty(key, null);
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
        String s = getProperty("batch-command.execute.type", null);
        if (s == null) {
            return String.join(",", SEQUENTIAL_SINGLE_TX, SEQUENTIAL_FACTORY_TX, PARALLEL_SINGLE_TX, PARALLEL_FACTORY_TX, PARALLEL_FACTORY_SESSION);
        }
        return s;
    }

    public static String batchCommandFactoryList() {
        return getProperty("batch-command.factory.list");
    }

    public static List<IsolationLevel> batchCommandIsolationLevel() {
        return getCommandIsolationLevel("batch-command.isolation.level");
    }

    private static List<IsolationLevel> getCommandIsolationLevel(String key) {
        String s = getProperty(key, null);
        if (s != null) {
            return Arrays.stream(s.split(",")).map(String::trim).map(String::toUpperCase).map(IsolationLevel::valueOf).collect(Collectors.toList());
        }

        if (dbmsType() == DbmsType.TSURUGI) {
            return List.of(IsolationLevel.SERIALIZABLE);
        } else {
            return List.of(IsolationLevel.READ_COMMITTED, IsolationLevel.SERIALIZABLE);
        }
    }

    public static List<String> batchCommandTxOption() {
        return getCommandTxOption("batch-command.tx.option");
    }

    public static List<String> getCommandTxOption(String key) {
        String s = getProperty(key, null);
        if (s != null) {
            return Arrays.stream(s.split(",")).map(String::trim).map(String::toUpperCase).collect(Collectors.toList());
        }

        if (dbmsType() == DbmsType.TSURUGI) {
            return List.of("OCC", "LTX");
        } else {
            return List.of("OCC");
        }
    }

    public static int batchCommandExecuteTimes() {
        return getPropertyInt("batch-command.execute.times", 1);
    }

    public static String batchCommandDiffDir() {
        return getProperty("batch-command.diff.dir", null);
    }

    public static String batchCommandResultFile() {
        return getProperty("batch-command.result.file");
    }

    public static boolean batchCommandInitData() {
        return getPropertyBoolean("batch-command.with.initdata", false);
    }

    public static boolean batchCommandOnline() {
        return getPropertyBoolean("batch-command.with.online", false);
    }

    public static final int DECIMAL_SCALE = getPropertyInt("decimal.scale", 20);

    // online

    public static Path onlineLogFilePath(int threadId) {
        String f = getProperty("online.log.file");
        String s = String.format(f, threadId);
        return Paths.get(s);
    }

    public static DbManagerType onlineDbManagerType() {
        return getDbManagerType("online.dbmanager.type");
    }

    public static boolean onlineDbManagerMultiSession() {
        return getPropertyBoolean("online.dbmanager.multi.session", true);
    }

    public static IsolationLevel onlineJdbcIsolationLevel() {
        return getIsolationLevel("online.jdbc.isolation.level");
    }

    public static String onlineTsurugiTxOption(String taskName) {
        String s = getProperty("online.tsurugi.tx.option." + taskName, null);
        if (s == null) {
            s = getProperty("online.tsurugi.tx.option", "OCC");
        }
        return s;
    }

    /** スレッド毎にランダムにタスクを実行する */
    public static final String ONLINE_RANDOM = "random";
    /** スレッド毎にタスクを固定し、一定時間内の実行回数を指定する */
    public static final String ONLINE_SCHEDULE = "schedule";

    public static String onlineType() {
        return getProperty("online.type", ONLINE_RANDOM);
    }

    public static int onlineThreadSize() {
        return getPropertyInt("online.random.thread.size");
    }

    public static int onlineTaskRatio(String taskName) {
        return getPropertyInt("online.random.task.ratio." + taskName, 0);
    }

    public static int onlineTaskSleepTime(String taskName) {
        return getPropertyInt("online.random.task.sleep." + taskName, 0);
    }

    public static int onlineThreadSize(String taskName) {
        return getPropertyInt("online.schedule.thread.size." + taskName, 0);
    }

    public static int onlineExecutePerMinute(String taskName) {
        // 負数: waitせずに連続してタスクを実行する
        // 0: タスクを実行しない
        // 正数: 1分間に実行するタスクの個数
        return getPropertyInt("online.schedule.execute.per.minute." + taskName, -1);
    }

    // initial data

    public static DbManagerType initDbManagerType() {
        return getDbManagerType("init.dbmanager.type");
    }

    public static boolean initDbManagerMultiSession() {
        return getPropertyBoolean("init.dbmanager.multi.session", true);
    }

    public static String initTsurugiTxOption() {
        return getProperty("init.tsurugi.tx.option", "LTX");
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

    public static boolean debugExplain() {
        return getPropertyBoolean("debug.explain", false);
    }

    // time

    public static DbManagerType timeCommandDbManagerType() {
        return getDbManagerType("time-command.dbmanager.type");
    }

    public static List<IsolationLevel> timeCommandIsolationLevel() {
        return getCommandIsolationLevel("time-command.isolation.level");
    }

    public static List<String> timeCommandTxOption() {
        return getCommandTxOption("time-command.tx.option");
    }

    public static int timeCommandSize(String tableName) {
        return getPropertyInt("time-command." + tableName + ".size");
    }

    public static int timeCommandSizeAdjust(String tableName, String adjustKey) {
        return getPropertyInt("time-command." + tableName + ".size.adjust." + adjustKey, 0);
    }

    public static boolean timeCommandExecute(String tableName, String sqlName) {
        String s = getProperty("time-command." + tableName + "." + sqlName, null);
        if (s == null) {
            s = getProperty("time-command." + tableName, null);
        }
        if (s == null) {
            s = getProperty("time-command", null);
        }
        return (s != null) ? Boolean.parseBoolean(s) : true;
    }

    public static String timeCommandResultFile() {
        return getProperty("time-command.result.file");
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

    private static String getProperty(String key, String defaultValue) {
        String s = getProperty(key, false);
        return (s != null) ? s : defaultValue;
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

    private static boolean getPropertyBoolean(String key, boolean defaultValue) {
        String s = getProperty(key, false);
        if (s == null) {
            return defaultValue;
        }
        return Boolean.parseBoolean(s);
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
