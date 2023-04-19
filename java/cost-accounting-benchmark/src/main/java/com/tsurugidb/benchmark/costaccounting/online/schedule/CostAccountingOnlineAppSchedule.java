package com.tsurugidb.benchmark.costaccounting.online.schedule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.online.task.BenchOnlineTask;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;

public class CostAccountingOnlineAppSchedule implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(CostAccountingOnlineAppSchedule.class);

    /**
     * スケジュールを生成するインターバル(ミリ秒)
     */
    protected static final int CREATE_SCHEDULE_INTERVAL_MILLS = (int) TimeUnit.MINUTES.toMillis(1);

    /**
     * 終了リクエストの有無を表すフラグ
     */
    private final AtomicBoolean terminationRequested;

    /**
     * 1分間に実行する回数。負数の場合は連続で実行する
     */
    private int execPerMin;

    /**
     * スケジュール作成時に使用する乱数発生器
     */
    private final Random random = new Random();

    /**
     * スレッドの開始時刻
     */
    private long startTime;

    /**
     * 処理を実行する時刻を格納したセット
     */
    private final List<Long> scheduleList = new ArrayList<Long>();

    /**
     * タスク名(=スレッド名)
     */
    private String name;

    private final BenchOnlineTask onlineTask;
    private final List<Integer> factoryList;
    private final LocalDate date;

    public CostAccountingOnlineAppSchedule(BenchOnlineTask task, int threadId, List<Integer> factoryList, LocalDate date, AtomicBoolean terminationRequested) {
        this.onlineTask = task;
        this.factoryList = factoryList;
        this.date = date;
        this.terminationRequested = terminationRequested;

        this.name = "online." + task.getTitle() + "." + threadId;
        this.execPerMin = BenchConst.onlineExecutePerMinute(task.getTitle());
    }

    @Override
    public void run() {
        try {
            Thread.currentThread().setName(name);
            if (execPerMin == 0) {
                // txPerMinが0の場合は何もしない
                return;
            }
            LOG.info("{} started.", name);
            startTime = System.currentTimeMillis();
            scheduleList.add(startTime);
            while (!terminationRequested.get()) {
                schedule();
            }
            LOG.info("{} terminated.", name);
        } catch (IOException e) {
            LOG.error("Aborting by exception", e);
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (Throwable e) {
            LOG.error("Aborting by exception", e);
            throw e;
        }
    }

    private void schedule() throws IOException {
        long schedule = scheduleList.get(0);
        if (System.currentTimeMillis() < schedule) {
            if (execPerMin > 0) {
                // 処理の開始時刻になっていなければ、10ミリ秒スリープしてリターンする
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    // Nothing to do;
                }
                return;
            } else {
                // 連続実行が指定されているケース
                execute();
                return;
            }
        }
        // スケジュール時刻になったとき
        scheduleList.remove(0);
        if (scheduleList.isEmpty()) {
            // スケジュールリストの最後のエントリはスケジュールを作成する時刻
            createScheduleList(schedule);
        } else {
            execute();
        }
    }

    /**
     * スケジュールを作成する
     *
     * @param base スケジュール生成のスケジュール(時刻)
     * @throws IOException
     */
    private void createScheduleList(long base) throws IOException {
        long now = System.currentTimeMillis();
        if (base + CREATE_SCHEDULE_INTERVAL_MILLS < now) {
            // スケジュール生成の呼び出しが、予定よりCREATE_SCHEDULE_INTERVAL_MILLSより遅れた場合は、
            // 警告のログを出力し、スケジュールのベースとなる時刻を進める。
            LOG.warn("Detected a large delay in the schedule and reset the base time(base = {}, now = {}).", new Timestamp(base), new Timestamp(now));
            base = System.currentTimeMillis();
        }

        for (int i = 0; i < execPerMin; i++) {
            long schedule = base + random.nextInt(CREATE_SCHEDULE_INTERVAL_MILLS);
            scheduleList.add(schedule);
        }
        Collections.sort(scheduleList);

        // 次にスケジュールを作成する時刻
        scheduleList.add(base + CREATE_SCHEDULE_INTERVAL_MILLS);
    }

    private void execute() {
        int factoryId = factoryList.get(random.nextInt(factoryList.size()));
        onlineTask.initialize(factoryId, date);

        for (;;) { // 処理に成功するまで無限にリトライする
            if (terminationRequested.get()) {
                return;
            }
            try {
                onlineTask.execute();
                return;
            } catch (UncheckedIOException e) {
                throw e;
            } catch (RuntimeException e) {
                LOG.info("Caught exception, retrying... ", e);
            }
        }
    }

    /**
     * このオンラインアプリケーションをアボートする
     */
    public void terminate() {
        terminationRequested.set(true);
    }
}
