package com.example.nedo.app;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Date;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.nedo.db.DBUtils;

public class Config {
	private Properties prop;


	/* 料金計算に関するパラメータ */

	/**
	 * 計算対象日(指定の日を含む月を計算対象とする)
	 */
	public Date targetMonth;

	/* 契約マスタ生成に関するパラメータ */

	/**
	 * 契約マスタのレコード数
	 */
	public int numberOfContractsRecords;

	/**
	 *  契約マスタの電話番号が重複する割合
	 */
	public int duplicatePhoneNumberRatio;

	/**
	 * 契約終了日がある電話番号の割合
	 */
	public int expirationDateRate;

	/**
	 * 契約終了日がない電話番号の割合
	 */
	public int noExpirationDateRate;

	/**
	 * 契約開始日の最小値
	 */
	public Date minDate;

	/**
	 * 契約終了日の最大値
	 */
	public Date maxDate;

	/* 通話履歴生成に関するパラメータ */

	/**
	 * 通話履歴のレコード数
	 */
	public int numberOfHistoryRecords;


	/* jdbcのパラメータ */

    public String url;
    public String user;
    public String password;


	/* その他のパラメータ */

	/**
	 * 乱数のシード
	 */
	public long randomSeed;

	/**
	 * ログ出力ディレクトリ
	 */
	public String logDir;



	/**
	 * コンストラクタ
	 *
	 * @param configFileName
	 * @throws IOException
	 */
	private Config(String configFileName) throws IOException {
		prop = new Properties();
		if (configFileName != null) {
			prop.load(Files.newBufferedReader(Paths.get(configFileName), StandardCharsets.UTF_8));
		}
		init();

		Files.createDirectories(Paths.get(logDir));
		System.setProperty("log.dir", logDir);
		Logger logger = LoggerFactory.getLogger(Config.class);
		logger.info("Config initialized" +
				System.lineSeparator() + "--- " + System.lineSeparator() + this.toString() + "---");
	}

	/**
	 * config値を初期化する
	 */
	private void init() {
		// 料金計算に関するパラメータ
		 targetMonth = getDate("target.month", DBUtils.toDate("2020-12-01"));

		// 契約マスタ生成に関するパラメータ
		 numberOfContractsRecords =
				 getInt("number.of.contracts.records", 1000);
		 duplicatePhoneNumberRatio = getInt("duplicate.phone.number.ratio", 10);
		 expirationDateRate = getInt("expiration.date.rate", 30);
		 noExpirationDateRate = getInt("no.expiration.date.rate", 50);
		 minDate = getDate("min.date", DBUtils.toDate("2010-11-11"));
		 maxDate = getDate("max.date", DBUtils.toDate("2021-03-01"));

		// 通話履歴生成に関するパラメータ
		 numberOfHistoryRecords = getInt("number.of.history.records", (int) 1e6);

		// JDBCに関するパラメータ
		 url = getString("url", "jdbc:postgresql://127.0.0.1/phonebill");
		 user = getString("user", "phonebill");
		 password = getString("password", "phonebill");

		// その他のパラメータ
		 randomSeed = getLong("random.seed", 0);
		 logDir = getString("log.dir", "logs");
	}




	/**
	 * int型のプロパティの値を取得する
	 *
	 * @param key プロパティ名
	 * @param defaultValue プロパティが存在しない時のデフォルト値
	 * @return
	 */
	private int getInt(String key, int defaultValue) {
		int value = defaultValue;
		if (prop.containsKey(key)) {
			String s = prop.getProperty(key);
			value = Integer.parseInt(s);
		}
		return value;
	}

	/**
	 * long型のプロパティの値を取得する
	 *
	 * @param key プロパティ名
	 * @param defaultValue プロパティが存在しない時のデフォルト値
	 * @return
	 */
	private long getLong(String key, long defaultValue) {
		long value = defaultValue;
		if (prop.containsKey(key)) {
			String s = prop.getProperty(key);
			value = Long.parseLong(s);
		}
		return value;
	}

	/**
	 * Stringのプロパティの値を取得する
	 *
	 * @param key プロパティ名
	 * @param defaultValue プロパティが存在しない時のデフォルト値
	 * @return
	 */
	private String getString(String key, String defaultValue) {
		String value = defaultValue;
		if (prop.containsKey(key)) {
			value = prop.getProperty(key);
		}
		return value;
	}

	/**
	 * Date型のプロパティの値を取得する
	 *
	 * @param key プロパティ名
	 * @param defaultValue プロパティが存在しない時のデフォルト値
	 * @return
	 */
	private Date getDate(String key, Date defaultValue) {
		Date value = defaultValue;
		if (prop.containsKey(key)) {
			String s = prop.getProperty(key);
			value = DBUtils.toDate(s);
		}
		return value;
	}


	/**
	 * configオブジェクトの生成.
	 * <br>
	 * コマンドライン引数で指定されたファイル名のファイルを設定ファイルとみなして
	 * configオブジェクトを生成する。引数が指定されていない場合は、デフォルト値で
	 * configオブジェクトを生成する。
	 *
	 * @param args
	 * @return
	 * @throws IOException
	 */
	public static Config getConfig(String[] args) throws IOException {
		if (args.length == 0) {
			return new Config(null);
		}
		return new Config(args[0]);
	}

	public static Config getConfig() throws IOException {
		return getConfig(new String[0]);
	}



	@Override
	public String toString() {
		String format = "%s=%s%n";
		String commentFormat = "# %s%n";
		StringBuilder sb = new StringBuilder();

		sb.append(String.format(commentFormat, "料金計算に関するパラメータ"));
		sb.append(String.format(format, "target.month", targetMonth));
		sb.append(System.lineSeparator());
		sb.append(String.format(commentFormat, "契約マスタ生成に関するパラメータ"));
		sb.append(String.format(format, "number.of.contracts.records", numberOfContractsRecords));
		sb.append(String.format(format, "duplicate.phone.number.ratio", duplicatePhoneNumberRatio));
		sb.append(String.format(format, "expiration.date.rate", expirationDateRate));
		sb.append(String.format(format, "no.expiration.date.rate", noExpirationDateRate));
		sb.append(String.format(format, "min.date", minDate));
		sb.append(String.format(format, "max.date", maxDate));
		sb.append(System.lineSeparator());
		sb.append(String.format(commentFormat, "通話履歴生成に関するパラメータ"));
		sb.append(String.format(format, "number.of.history.records", numberOfHistoryRecords));
		sb.append(System.lineSeparator());
		sb.append(String.format(commentFormat, "JDBCに関するパラメータ"));
		sb.append(String.format(format, "url", url));
		sb.append(String.format(format, "user", user));
		sb.append(String.format(format, "password", password));
		sb.append(System.lineSeparator());
		sb.append(String.format(commentFormat, "その他のパラメータ"));
		sb.append(String.format(format, "random.seed", randomSeed));
		sb.append(String.format(format, "log.dir", logDir));
		return sb.toString();
	}




}
