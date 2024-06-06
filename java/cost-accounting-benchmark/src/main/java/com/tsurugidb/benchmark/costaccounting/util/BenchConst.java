package com.tsurugidb.benchmark.costaccounting.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager.DbManagerPurpose;
import com.tsurugidb.benchmark.costaccounting.db.DbmsType;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

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

    public static boolean tsurugiWatcherEnable() {
        return getPropertyBoolean("tsurugi.watcher.enable", false);
    }

    public static String docDir() {
        return getProperty("doc.dir");
    }

    public static InputStream tableXlsxStream(Logger log) {
        return xlsxStream(log, "table.xlsx");
    }

    public static InputStream measurementXlsxStream(Logger log) {
        return xlsxStream(log, "measurement.xlsx");
    }

    private static InputStream xlsxStream(Logger log, String fileName) {
        String docDir = getProperty("doc.dir", null);
        if (docDir != null) {
            var path = Path.of(docDir, fileName);
            InputStream is;
            try {
                is = Files.newInputStream(path);
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
            log.info("src={}", path);
            return is;
        }

        var classLoader = Thread.currentThread().getContextClassLoader();
        var is = classLoader.getResourceAsStream(fileName);
        log.info("src=classpath:{}", fileName);
        return is;
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

    public static int batchThreadSize() {
        return getPropertyInt("batch.thread.size", -1);
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

    public enum BatchFactoryOrder {
        NONE, COUNT_ASC, COUNT_DESC
    }

    public static BatchFactoryOrder getBatchFactoryOrder() {
        return getBatchFactoryOrder("batch.factory.order");
    }

    private static BatchFactoryOrder getBatchFactoryOrder(String key) {
        String s = getProperty(key, BatchFactoryOrder.NONE.name());
        try {
            return BatchFactoryOrder.valueOf(s.replaceAll("[-.]", "_").toUpperCase());
        } catch (Exception e) {
            throw new RuntimeException("invalid BatchFactoryOrder. " + key + "=" + s, e);
        }
    }

    // batch-command

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

    public static List<Integer> batchCommandThreadSize() {
        String s = getProperty("batch-command.thread.size", "-1");
        return Arrays.stream(s.split(",")).map(String::trim).map(Integer::valueOf).collect(Collectors.toList());
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

    public static List<Integer> batchCommandOnlineCoverRate() {
        String s = getProperty("batch-command.online.cover.rate", "100");
        return Arrays.stream(s.split(",")).map(String::trim).map(Integer::valueOf).collect(Collectors.toList());
    }

    public static int batchCommandExecuteTimes() {
        return getPropertyInt("batch-command.execute.times", 1);
    }

    public static String batchCommandDiffDir() {
        return getProperty("batch-command.diff.dir", null);
    }

    public static String batchCommandLabel() {
        return getProperty("batch-command.label", null);
    }

    public static String batchCommandResultFile() {
        return getProperty("batch-command.result.file");
    }

    public static Path batchCommandBatchCompareBase() {
        String s = getProperty("batch-command.batch.compare.base", null);
        if (s != null) {
            return Path.of(s);
        }
        return null;
    }

    public static String batchCommandOnlineReport() {
        return getProperty("batch-command.online.report");
    }

    public static boolean batchCommandInitData() {
        return getPropertyBoolean("batch-command.with.initdata", false);
    }

    public static boolean batchCommandPreBatch() {
        return getPropertyBoolean("batch-command.with.prebatch", false);
    }

    public static boolean batchCommandOnline() {
        return getPropertyBoolean("batch-command.with.online", false);
    }

    public static Path batchCommandOnlineCompareBase() {
        String s = getProperty("batch-command.online.compare.base", null);
        if (s != null) {
            return Path.of(s);
        }
        return null;
    }

    public static BatchFactoryOrder getBatchCommandFactoryOrder() {
        return getBatchFactoryOrder("batch-command.factory.order");
    }

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

    public static int onlineCoverRate() {
        return getPropertyInt("online.cover.rate", 100);
    }

    /** スレッド毎にランダムにタスクを実行する */
    public static final String ONLINE_RANDOM = "random";
    /** スレッド毎にタスクを固定し、一定時間内の実行回数を指定する */
    public static final String ONLINE_SCHEDULE = "schedule";

    public static String onlineType() {
        return getProperty("online.type", ONLINE_RANDOM);
    }

    public static int onlineRandomThreadSize() {
        return getPropertyInt("online.random.thread.size");
    }

    public static int onlineRandomTaskRatio(String taskName) {
        return getPropertyInt("online.random.task.ratio." + taskName, 0);
    }

    public static int onlineRandomTaskSleepTime(String taskName) {
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

    public enum ConsoleType {
        NULL, STDOUT
    }

    public static ConsoleType onlineConsoleType() {
        String key = "online.console.type";
        String s = getProperty(key, ConsoleType.STDOUT.name());
        try {
            return ConsoleType.valueOf(s.toUpperCase());
        } catch (Exception e) {
            throw new RuntimeException("invalid ConsoleType. " + key + "=" + s, e);
        }
    }

    public static String onlineDebugDir(String taskName) {
        return getProperty("online.debug.dir." + taskName, null);
    }

    public static String periodicTsurugiTxOption(String taskName) {
        String s = getProperty("periodic.tsurugi.tx.option." + taskName, null);
        if (s == null) {
            s = getProperty("periodic.tsurugi.tx.option", null);
        }
        if (s == null) {
            return onlineTsurugiTxOption(taskName);
        }
        return s;
    }

    public static int periodicThreadSize(String taskName) {
        return getPropertyInt("periodic.schedule.thread.size." + taskName, 0);
    }

    public static long periodicInterval(String taskName) {
        int seconds = getPropertyInt("periodic.schedule.interval." + taskName, -1);
        return TimeUnit.SECONDS.toMillis(seconds);
    }

    public static int periodicSplitSize(String taskName) {
        return getPropertyInt("periodic.schedule." + taskName + ".split.size", 1);
    }

    public static int periodicKeepSize(String taskName) {
        return getPropertyInt("periodic.schedule." + taskName + ".keep.size", -1);
    }

    // online-command

    public static String onlineCommandLabel() {
        return getProperty("online-command.label", null);
    }

    public static List<IsolationLevel> onlineCommandIsolationLevel() {
        return getCommandIsolationLevel("online-command.isolation.level");
    }

    public static List<String> onlineCommandTxOption() {
        return getCommandTxOption("online-command.tx.option");
    }

    public static String onlineCommandFactoryList() {
        return getProperty("online-command.factory.list", "all");
    }

    public static List<Integer> onlineCommandCoverRate() {
        String s = getProperty("online-command.cover.rate", "100");
        return Arrays.stream(s.split(",")).map(String::trim).map(Integer::valueOf).collect(Collectors.toList());
    }

    public static int onlineCommandExecuteTimes() {
        return getPropertyInt("online-command.execute.times", 1);
    }

    public static boolean onlineCommandInitData() {
        return getPropertyBoolean("online-command.with.initdata", true);
    }

    public static boolean onlineCommandPreBatch() {
        return getPropertyBoolean("online-command.with.prebatch", false);
    }

    public static int onlineCommandExecuteTime() {
        return getPropertyInt("online-command.execute.time");
    }

    public static String onlineCommandResultFile() {
        return getProperty("online-command.result.file");
    }

    public static String onlineCommandOnlineReport() {
        return getProperty("online-command.online.report");
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

    public static int initCostFactoryPerMeterial() {
        return getPropertyInt("init.cost.factory.per.material", -1);
    }

    public static int initParallelism() {
        int defaultValue = Runtime.getRuntime().availableProcessors();
        return getPropertyInt("init.parallelism", defaultValue);
    }

    public static int initTaskThreshold() {
        return getPropertyInt("init.task.threshold", 1000);
    }

    public static boolean init03MaterialCache() {
        return getPropertyBoolean("init.03.material.cache", true);
    }

    public static boolean init05MaterialCache() {
        return getPropertyBoolean("init.05.material.cache", false);
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

    // share

    public static String sqlInsert(DbManagerPurpose purpose) {
        String s = getProperty("sql.insert." + purpose.name().toLowerCase(), null);
        if (s != null) {
            return s;
        }
        return getProperty("sql.insert", "insert or replace");
    }

    public static boolean useReadArea() {
        return getPropertyBoolean("use.read-area", true);
    }

    public enum SqlDistinct {
        /** group by */
        GROUP,
        /** distinct */
        DISTINCT
    }

    public static SqlDistinct sqlDistinct() {
        String s = getProperty("sql.distinct", SqlDistinct.GROUP.name());
        return SqlDistinct.valueOf(s.toUpperCase());
    }

    public static final int DECIMAL_SCALE = getPropertyInt("decimal.scale", 20);

    // Iceaxeのデフォルトトランザクションオプションの使用例
    // 例えば原価計算ベンチマークと料金計算ベンチマークのテーブルが同じDBにあるとき、原価計算ベンチマークからは料金計算ベンチマークのテーブルを絶対参照しない。
    // こうした絶対参照しないと分かっているテーブルをデフォルトトランザクションオプション（exclusive read area）として定義しておく。
    /** Iceaxe default transaction option */
    public static final TgTxOption DEFAULT_TX_OPTION = TgTxOption.ofLTX()/* .addExclusiveReadArea("billing", "contracts", "history") */;

    // properties

    private static volatile Properties properties;

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
