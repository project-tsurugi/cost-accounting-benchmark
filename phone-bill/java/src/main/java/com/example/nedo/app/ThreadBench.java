package com.example.nedo.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.nedo.app.billing.PhoneBill;
import com.example.nedo.testdata.CreateTestData;

/**
 * スレッド数とコネクション共有の有無で、PhoneBillコマンドの実行時間がどう変化するのかを調べる
 * ためのコマンド.
 *
 * threadCounts, sharedConnection以外の設定値はコンフィグレーションファイルで指定された
 * 値を使用する。
 *
 */
public class ThreadBench implements ExecutableCommand {
    private static final Logger LOG = LoggerFactory.getLogger(ThreadBench.class);

	public static void main(String[] args) throws Exception {
		Config config = Config.getConfig(args);
		ThreadBench threadBench = new ThreadBench();
		threadBench.execute(config);
	}


	@Override
	public void execute(Config config) throws Exception {
		new CreateTable().execute(config);
		new CreateTestData().execute(config);
		PhoneBill phoneBill = new PhoneBill();
		boolean[] sharedConnections = { false, true };
		int[] threadCounts = { 1, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32 };
		for (boolean sharedConnection : sharedConnections) {
			for (int threadCount : threadCounts) {
				config.threadCount = threadCount;
				config.sharedConnection = sharedConnection;
				LOG.info("Executing phoneBill.exec() with threadCount =" + threadCount +
						", sharedConnection = " + sharedConnection);
				phoneBill.execute(config);
			}
		}
	}
}
