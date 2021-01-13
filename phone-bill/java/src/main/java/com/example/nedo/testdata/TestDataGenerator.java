package com.example.nedo.testdata;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.example.nedo.app.Config;
import com.example.nedo.db.DBUtils;
import com.example.nedo.db.Duration;
import com.example.nedo.db.History;

/**
 * @author umega
 *
 */
public class TestDataGenerator {
	private Config config;

	/**
	 * 同じ発信時刻のデータを作らないための作成済みのHistoryDataの発信時刻を記録するSet
	 */
	private Set<Long> startTimeSet;

	/**
	 * 11桁の電話番号をLONG値で表したときの最大値
	 */
	private static final long MAX_PHNE_NUMBER = 99999999999L;

	/**
	 * 一度にインサートする行数
	 */
	private static final long SQL_BATCH_EXEC_SIZE = 300000;

	/**
	 * 契約期間のパターンを記録するリスト
	 */
	private List<Duration> durationList = new ArrayList<>();

	private Random random;

	/**
	 * テストデータ生成のためのパラメータを指定してContractsGeneratorのインスタンスを生成する.
	 *
	 *
	 * @param seed 乱数のシード
	 * @param numberOfContractsRecords 契約マスタのレコード数
	 * @param numberOfHistoryRecords 通話履歴のレコード数
	 * @param duplicatePhoneNumberRatio 電話番号が重複する割合
	 * @param expirationDateRate 契約終了日がある電話番号の割合
	 * @param noExpirationDateRate 契約終了日がない電話番号の割合
	 * @param minDate 契約開始日の最小値
	 * @param maxDate 契約終了日の最大値
	 */
	public TestDataGenerator(Config config) {
		this.config = config;
		if (config.minDate.getTime() >= config.maxDate.getTime()) {
			new RuntimeException("maxDate is less than or equal to minDate, minDate =" + config.minDate + ", maxDate = "
					+ config.maxDate);
		}
		this.random = new Random(config.randomSeed);
		this.startTimeSet = new HashSet<Long>(config.numberOfHistoryRecords);
		initDurationList();
	}

	/**
	 * 契約マスタのテストデータを生成する
	 *
	 * @throws SQLException
	 */
	public void generateContract() throws SQLException {
		try (Connection conn = DBUtils.getConnection(config)) {
			// TODO オプション指定により、truncateするのではなく、データが存在する場合警告して終了するようにする
			Statement stmt = conn.createStatement();
			stmt.executeUpdate("truncate table contracts");

			PreparedStatement ps = conn.prepareStatement("insert into contracts("
					+ "phone_number,"
					+ "start_date,"
					+ "end_date,"
					+ "charge_rule"
					+ ") values(?, ?, ?, ?)");
			int batchSize = 0;

			for (long n = 0; n < config.numberOfContractsRecords; n++) {
				Duration d = getDuration(n);
				String rule = "Simple";
				ps.setString(1, getPhoneNumber(n));
				ps.setDate(2, d.start);
				ps.setDate(3, d.end);
				ps.setString(4, rule);
				ps.addBatch();
				if (++batchSize == SQL_BATCH_EXEC_SIZE) {
					execBatch(ps);
					batchSize = 0;
				}
			}
			execBatch(ps);
		}
	}

	/**
	 * 二つの期間に共通の期間を返す
	 *
	 * @param d1
	 * @param d2
	 * @return 共通な期間、共通な期間がない場合nullを返す。
	 */
	public static Duration getCommonDuration(Duration d1, Duration d2) {
		// d1, d2に共通な期間がない場合
		if (d1.end.getTime() < d2.start.getTime()) {
			return null;
		}
		if (d2.end.getTime() < d1.start.getTime()) {
			return null;
		}
		if (d1.start.getTime() < d2.start.getTime()) {
			if (d1.end.getTime() < d2.end.getTime()) {
				return new Duration(d2.start, d1.end);
			} else {
				return d2;
			}
		} else {
			if (d1.end.getTime() < d2.end.getTime()) {
				return d1;
			} else {
				return new Duration(d1.start, d2.end);
			}
		}
	}

	/**
	 * 通話履歴のテストデータを作成する
	 *
	 * 生成する通話履歴の通話開始時刻は、minDate以上、maxDate未満の値にする。
	 *
	 * @throws SQLException
	 */
	public void generateHistory(Date minDate, Date maxDate) throws SQLException {
		// TODO 低確率でPK(電話番号と、通話開始時間)の重複が起きるので、重複が起きないアルゴリズムに変更する
		if (!isValidDurationList(durationList, minDate, maxDate)) {
			throw new RuntimeException("Invalid duration list.");
		}

		Duration targetDuration = new Duration(minDate, maxDate);

		try (Connection conn = DBUtils.getConnection(config)) {
			// TODO オプション指定により、truncateするのではなく、データが存在する場合警告して終了するようにする
			Statement stmt = conn.createStatement();
			stmt.executeUpdate("truncate table history");

			PreparedStatement ps = conn.prepareStatement("insert into history("
					+ "caller_phone_number,"
					+ "recipient_phone_number,"
					+ "payment_categorty,"
					+ "start_time,"
					+ "time_secs,"
					+ "charge,"
					+ "df"
					+ ") values(?, ?, ?, ?, ?, ?, ? )");
			int batchSize = 0;
			// numberOfHistoryRecords だけレコードを生成する
			for (long n = 0; n < config.numberOfHistoryRecords; n++) {
				History h = createHistoryRecord(targetDuration);
				ps.setString(1, h.caller_phone_number);
				ps.setString(2, h.recipient_phone_number);
				ps.setString(3, h.payment_categorty);
				ps.setTimestamp(4, h.start_time);
				ps.setInt(5, h.time_secs);
				if (h.charge == null) {
					ps.setNull(6, Types.INTEGER);
				} else {
					ps.setInt(6, h.charge);
				}
				ps.setBoolean(7, h.df);
				ps.addBatch();
				if (++batchSize == SQL_BATCH_EXEC_SIZE) {
					execBatch(ps);
					batchSize = 0;
				}
			}
			execBatch(ps);
		}
	}

	/**
	 * minDate～maxDateの間の全ての日付に対して、当該日付を含むdurationがlistに二つ以上あることを確認する
	 *
	 * @param list
	 * @param minDate
	 * @param maxDate
	 */
	static boolean isValidDurationList(List<Duration> list, Date minDate, Date maxDate) {
		if (minDate.getTime() > maxDate.getTime()) {
			return false;
		}
		for (Date date = minDate; date.getTime() <= maxDate.getTime(); date = DBUtils.nextDate(date)) {
			int c = 0;
			for (Duration duration : list) {
				long start = duration.start.getTime();
				long end = duration.end == null ? Long.MAX_VALUE : duration.end.getTime();
				if (start <= date.getTime() && date.getTime() <= end) {
					c++;
					if (c >= 2) {
						break;
					}
				}
			}
			if (c < 2) {
				System.err.println("Duration List not contains date: " + date);
				return false;
			}
		}
		return true;
	}

	private History createHistoryRecord(Duration targetDuration) {
		History history = new History();
		// 通話開始時刻
		long startTime;
		do {
			startTime = getRandomLong(targetDuration.start.getTime(), targetDuration.end.getTime());
		} while (startTimeSet.contains(startTime));
		startTimeSet.add(startTime);
		history.start_time = new Timestamp(startTime);

		// 電話番号の生成
		long caller = selectContract(startTime, -1, getRandomLong(0, config.numberOfContractsRecords));
		long recipient = selectContract(startTime, caller, getRandomLong(0, config.numberOfContractsRecords));
		history.caller_phone_number = getPhoneNumber(caller);
		history.recipient_phone_number = getPhoneNumber(recipient);

		// 料金区分(発信者負担、受信社負担)
		// TODO 割合を指定可能にする
		history.payment_categorty = random.nextInt(2) == 0 ? "C" : "R";

		// 通話時間
		// TODO 分布関数を指定可能にする
		history.time_secs = random.nextInt(3600) + 1;

		return history;
	}

	/**
	 * 指定の通話開始時刻が契約範囲に含まれる選択する。
	 * <br>
	 * 発信者電話番号、受信者電話番号の順にこのメソッドを使用して電話番号を選択する。
	 * 発信者電話番号の選択時には、exceptPhoneNumberに-1を指定する。受信者電話番号の
	 * 選択時には、exceptPhoneNumberに発信者電話番号を指定することにより、受信者電話番号と
	 * 発信者電話番号が等しくなるのを避ける。
	 *
	 *
	 * @param startTime 通話開始時刻
	 * @param exceptPhoneNumber 選択しない電話番号。
	 * @param startPos 0以上numberOfContractsRecords以下のランダムな値を指定する
	 * @return 選択為た電話番号
	 */
	long selectContract(long startTime, long exceptPhoneNumber, long startPos) {
		long pos = startPos;
		int c = 0;
		for (;;) {
			if (pos != exceptPhoneNumber) {
				Duration d = getDuration(pos);
				if (d.end == null) {
					if (d.start.getTime() <= startTime) {
						break;
					}
				} else {
					if (d.start.getTime() <= startTime && startTime < d.end.getTime()) {
						break;
					}
				}
			}
			pos++;
			if (pos >= config.numberOfContractsRecords) {
				pos = 0;
			}
			if (++c >= durationList.size()) {
				throw new RuntimeException("Not found! start time = " + new java.util.Date(startTime));
			}
		}
		return pos;
	}

	private void execBatch(PreparedStatement ps) throws SQLException {
		int rets[] = ps.executeBatch();
		for (int ret : rets) {
			if (ret < 0 && ret != PreparedStatement.SUCCESS_NO_INFO) {
				throw new SQLException("Fail to batch exexecute");
			}
		}
		ps.getConnection().commit();
		ps.clearBatch();
	}

	/**
	 * 契約日のパターンのリストを作成する
	 */
	private void initDurationList() {
		// TODO: もっとバリエーションが欲しい
		// 契約終了日がないduration
		for (int i = 0; i < config.noExpirationDateRate; i++) {
			Date start = getDate(config.minDate, config.maxDate);
			durationList.add(new Duration(start, null));
		}
		// 契約終了日があるduration
		for (int i = 0; i < config.expirationDateRate; i++) {
			Date start = getDate(config.minDate, config.maxDate);
			Date end = getDate(start, config.maxDate);
			durationList.add(new Duration(start, end));
		}
		// 同一電話番号の契約が複数あるパターン用のduration
		for (int i = 0; i < config.duplicatePhoneNumberRatio; i++) {
			Date end = getDate(config.minDate, config.maxDate);
			Date start = getDate(end, config.maxDate);
			;
			durationList.add(new Duration(config.minDate, end));
			durationList.add(new Duration(DBUtils.nextDate(start), null));
		}
	}

	/**
	 * durationListを取得する(UT用)
	 *
	 * @return durationList
	 */
	List<Duration> getDurationList() {
		return durationList;
	}

	/**
	 * min～maxの範囲のランダムな日付を取得する
	 *
	 * @param min
	 * @param max
	 * @return
	 */
	Date getDate(Date min, Date max) {
		int days = (int) ((max.getTime() - min.getTime()) / DBUtils.A_DAY_IN_MILLISECONDS);
		long offset = random.nextInt(days + 1) * DBUtils.A_DAY_IN_MILLISECONDS;
		return new Date(min.getTime() + offset);
	}

	/**
	 * n番目の電話番号(11桁)を返す
	 *
	 * @param n
	 * @return
	 */
	String getPhoneNumber(long n) {
		if (n < 0 || MAX_PHNE_NUMBER <= n) {
			throw new RuntimeException("Out of phone number range: " + n);
		}
		// TODO 電話番号が連番にならないようにする
		long blockSize = config.duplicatePhoneNumberRatio * 2 + config.expirationDateRate + config.noExpirationDateRate;
		long noDupSize = config.expirationDateRate + config.noExpirationDateRate;
		long posInBlock = n % blockSize;
		long phoneNumber = n;
		if (posInBlock >= noDupSize && posInBlock % 2 == 0) {
			phoneNumber = n + 1;
		}
		String format = "%011d";
		return String.format(format, phoneNumber);
	}

	/**
	 * n番目のレコードのDurationを返す
	 *
	 * @param n
	 * @return
	 */
	Duration getDuration(long n) {
		return durationList.get((int) (n % durationList.size()));
	}

	/**
	 * min以上max未満のランダムなlong値を取得する
	 *
	 * @param min
	 * @param max
	 * @return
	 */
	private long getRandomLong(long min, long max) {
		return min + (long) (random.nextDouble() * (max - min));
	}
}
