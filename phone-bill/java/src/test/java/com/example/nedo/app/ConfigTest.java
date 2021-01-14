package com.example.nedo.app;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import com.example.nedo.db.DBUtils;

class ConfigTest {
	/**
	 * 設定ファイルを指定しないケースのテスト
	 * @throws IOException
	 */
	@Test
	void testDefault() throws IOException {
		Config config = Config.getConfig();
		checkDefault(config);
	}



	/**
	 * 指定のconfigにデフォルト値が設定されていることを確認する
	 *
	 * @param config
	 * @throws IOException
	 */
	private void checkDefault(Config config) throws IOException {
		// 料金計算に関するパラメータ
		assertEquals(DBUtils.toDate("2020-12-01"), config.targetMonth);

		/* 契約マスタ生成に関するパラメータ */
		assertEquals((int) 1e3, config.numberOfContractsRecords);
		assertEquals(10, config.duplicatePhoneNumberRatio);
		assertEquals(30, config.expirationDateRate);
		assertEquals(50, config.noExpirationDateRate);
		assertEquals(DBUtils.toDate("2010-11-11"), config.minDate);
		assertEquals(DBUtils.toDate("2021-03-01"), config.maxDate);

		/* 通話履歴生成に関するパラメータ */
		assertEquals((int) 1e6, config.numberOfHistoryRecords);

		/* その他のパラメータ */
		assertEquals(0, config.randomSeed);
		assertEquals(Paths.get("logs"), config.logDir);

		// toStringのチェック
		Path path = Paths.get("src/test/config/default.properties");
		String expected = new String(Files.readAllBytes(path));
		assertEquals(expected, config.toString());
	}


}
