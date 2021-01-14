package com.example.nedo.app;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Date;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.nedo.db.DBUtils;

/**
 * @author umega
 *
 */
public class Config {
	/* 料金計算に関するパラメータ */

	/**
	 * 計算対象日(指定の日を含む月を計算対象とする)
	 */
	public Date targetMonth = DBUtils.toDate("2020-12-01");


	/**
	 * 計算対象の終了日
	 */


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
	public Path logDir = Paths.get("logs");



	/**
	 * コンストラクタ
	 *
	 * @param configFileName
	 * @throws IOException
	 */
	private Config(String configFileName) throws IOException {
		if (configFileName != null) {
			Properties prop = new Properties();
			prop.load(Files.newBufferedReader(Paths.get(configFileName), StandardCharsets.UTF_8));
		}
		Files.createDirectories(logDir);
		System.setProperty("log.dir", logDir.toString());
		Logger logger = LoggerFactory.getLogger(Config.class);
		logger.info("Config initialized" +
				System.lineSeparator() + "--- " + System.lineSeparator() + this.toString() + "---");
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
