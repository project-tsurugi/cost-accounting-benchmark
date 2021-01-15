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
	public Date targetMonth = DBUtils.toDate("2020-12-01");

	/* 契約マスタ生成に関するパラメータ */

	/**
	 * 契約マスタのレコード数
	 */
	public int numberOfContractsRecords = (int)1e3;

	/**
	 *  契約マスタの電話番号が重複する割合
	 */
	public int duplicatePhoneNumberRatio = 10;

	/**
	 * 契約終了日がある電話番号の割合
	 */
	public int expirationDateRate = 30;

	/**
	 * 契約終了日がない電話番号の割合
	 */
	public int noExpirationDateRate = 50;;

	/**
	 * 契約開始日の最小値
	 */
	public Date minDate = DBUtils.toDate("2010-11-11");;

	/**
	 * 契約終了日の最大値
	 */
	public Date maxDate = DBUtils.toDate("2021-03-01");

	/* 通話履歴生成に関するパラメータ */

	/**
	 * 通話履歴のレコード数
	 */
	public int numberOfHistoryRecords = (int) 1e6;


	/* jdbcのパラメータ */

    public String url = "jdbc:postgresql://127.0.0.1/phonebill";
    public String user = "phonebill";
    public String password = "phonebill";


	/* その他のパラメータ */

	/**
	 * 乱数のシード
	 */
	public long randomSeed = 0;

	/**
	 * ログ出力ディレクトリ
	 */
	public String logDir = "logs";



	/**
	 * コンストラクタ
	 *
	 * @param configFileName
	 * @throws IOException
	 */
	private Config(String configFileName) throws IOException {
		if (configFileName != null) {
			prop = new Properties();
			prop.load(Files.newBufferedReader(Paths.get(configFileName), StandardCharsets.UTF_8));
			init();
		}
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
		 targetMonth = getDate("target.month", targetMonth);

		// 契約マスタ生成に関するパラメータ
		 numberOfContractsRecords =
				 getInt("number.of.contracts.records", numberOfContractsRecords);
		 duplicatePhoneNumberRatio = getInt("duplicate.phone.number.ratio", duplicatePhoneNumberRatio);
		 expirationDateRate = getInt("expiration.date.rate", expirationDateRate);
		 noExpirationDateRate = getInt("no.expiration.date.rate", noExpirationDateRate);
		 minDate = getDate("min.date", minDate);
		 maxDate = getDate("max.date", maxDate);

		// 通話履歴生成に関するパラメータ
		 numberOfHistoryRecords = getInt("number.of.history.records", numberOfHistoryRecords);

		// JDBCに関するパラメータ
		 url = getString("url", url);
		 user = getString("user", user);
		 password = getString("password", password);

		// その他のパラメータ
		 randomSeed = getLong("random.seed", randomSeed);
		 logDir = getString("log.dir", logDir);
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
