package com.example.nedo.testdata;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.example.nedo.db.DBUtils;

/**
 * @author umega
 *
 */
public class TestDataGenerator {

	/**
	 * 11桁の電話番号をLONG値で表したときの最大値
	 */
	private static final long MAX_PHNE_NUMBER = 99999999999L;

	/**
	 * 一度にインサートする行数
	 */
	private static final long SQL_BATCH_EXEC_SIZE = 3000;

	/**
	 * ミリ秒で表した1日
	 */
	private static final long A_DAY_IN_MILLISECONDS = 24 * 3600 * 1000;

	private List<Duration> durationList = new ArrayList<>();



	private Random random;
	private long numberOfContractsRecords;
	private long numberOfHistoryRecords;
	private int duplicatePhoneNumberRatio;
	private int expirationDateRate;
	private int noExpirationDateRate;
	private Date minDate;
	private Date maxDate;


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
	public TestDataGenerator(long seed, long numberOfContractsRecords, long numberOfHistoryRecords,
			int duplicatePhoneNumberRatio,
			int expirationDateRate, int noExpirationDateRate, Date minDate, Date maxDate) {
		if (minDate.getTime() >= maxDate.getTime()) {
			new RuntimeException(
					"maxDate is less than or equal to minDate, minDate =" + minDate + ", maxDate = " + maxDate);
		}

		this.random = new Random(seed);
		this.numberOfContractsRecords = numberOfContractsRecords;
		this.numberOfHistoryRecords = numberOfHistoryRecords;
		this.duplicatePhoneNumberRatio = duplicatePhoneNumberRatio;
		this.expirationDateRate =expirationDateRate;
		this.noExpirationDateRate = noExpirationDateRate;
		this.minDate = minDate;
		this.maxDate = maxDate;
		initDurationList();
	}


	/**
	 * 契約マスタのテストデータを生成する
	 *
	 * @throws SQLException
	 */
	public void generateContract() throws SQLException {
		try (Connection conn = DBUtils.getConnection()) {
			// オプション指定により、truncateするのではなく、データが存在する場合警告して終了するようにする
			Statement stmt = conn.createStatement();
			stmt.executeUpdate("truncate table contracts");

			PreparedStatement ps = conn.prepareStatement("insert into contracts("
					+ "phone_number,"
					+ "start_date,"
					+ "end_date,"
					+ "charge_rule"
					+ ") values(?, ?, ?, ?)");
			int batchSize = 0;
			for(long n = 0; n < numberOfContractsRecords; n++) {
				Duration d = getDuration(n);
				String rule = "dummy";
				ps.setString(1, getPhoneNumber(n));
				ps.setDate(2, d.start);
				ps.setDate(3, d.end);
				ps.setString(4, rule);
				ps.addBatch();
				if (++batchSize == SQL_BATCH_EXEC_SIZE) {
					execBatch(ps);
					conn.commit();
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
		Duration targetDuration = new Duration(minDate, maxDate);

		try (Connection conn = DBUtils.getConnection()) {
			// オプション指定により、truncateするのではなく、データが存在する場合警告して終了するようにする
			Statement stmt = conn.createStatement();
			stmt.executeUpdate("truncate table history");

			PreparedStatement ps = conn.prepareStatement("insert into contracts("
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
			for(long n = 0; n < numberOfHistoryRecords; n++) {
				Duration d = getDuration(n);


				String rule = "dummy";
				ps.setString(1, getPhoneNumber(n));
				ps.setDate(2, d.start);
				ps.setDate(3, d.end);
				ps.setString(4, rule);
				ps.addBatch();
				if (++batchSize == SQL_BATCH_EXEC_SIZE) {
					execBatch(ps);
					conn.commit();
				}
			}
			execBatch(ps);
		}
	}


	private HistoryRecord createHistoryRecord(Duration targetDuration) {
		 HistoryRecord record = new HistoryRecord();
		 // 発信者電話番号の生成
		long caller = random.nextLong() % numberOfContractsRecords;

		// 受信者電話番号の生成
		long recipient = random.nextLong() % (numberOfContractsRecords);
		long loopCount = 0;
		for(;;) { 			// 通話可能な受信者が見つかるまでループ
			loopCount++;
		}

		// 通話開始時刻の生成
	}

	/**
	 * 契約の有効期間とtargetDurationに共通の期間が存在する電話番号を
	 * サーチする
	 *
	 * @param startPos サーチ開始位置
	 * @param targetDuration
	 * @return 見つかった電話番号
	 */
	private long searchPhoneNumber(long startPos, Duration targetDuration) {
		int loopCount = 0;
		for(;;) {

		}
	}


	/**
	 * 通話履歴1レコード分の情報を表すクラス
	 *
	 */
	static class HistoryRecord {
		 String caller_phone_number;
		 String recipient_phone_number;
		 char payment_categorty;
		 Timestamp start_time;
		 long time_secs;
		 int charge;
		 boolean df;

	}


	private void execBatch(PreparedStatement ps) throws SQLException {
		int rets[] = ps.executeBatch();
		for(int ret: rets) {
			if (ret < 0 && ret != PreparedStatement.SUCCESS_NO_INFO ) {
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
		for(int i = 0; i <noExpirationDateRate; i++) {
			Date start = getDate(minDate, maxDate);
			durationList.add(new Duration(start, null));
		}
		// 契約終了日があるduration
		for(int i = 0; i <expirationDateRate; i++) {
			Date start = getDate(minDate, maxDate);
			Date end = getDate(start, maxDate);
			durationList.add(new Duration(start, end));
		}
		// 同一電話番号の契約が複数あるパターン用のduration
		for(int i = 0; i <duplicatePhoneNumberRatio; i++) {
			Date end = getDate(minDate, maxDate);
			Date start = getDate(end, maxDate);;
			durationList.add(new Duration(minDate, end));
			durationList.add(new Duration(nextDate(start), null));
		}
	}


	/**
	 * 指定のdateの次の日を返す
	 *
	 * @param date
	 * @return
	 */
	Date nextDate(Date date) {
		return new Date(date.getTime() + A_DAY_IN_MILLISECONDS);
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
		int days = (int) ((max.getTime() - min.getTime()) / A_DAY_IN_MILLISECONDS);
		long offset = random.nextInt(days + 1) * A_DAY_IN_MILLISECONDS;
		return new Date(min.getTime() + offset);
	}



	/**
	 * n番目の電話番号(11桁)を返す
	 *
	 * @param n
	 * @return
	 */
	String getPhoneNumber(long  n) {
		if (n < 0 || MAX_PHNE_NUMBER <= n) {
			throw new RuntimeException("Out of phone number range: " + n);
		}
		// TODO 電話番号が連番にならないようにする
		long blockSize = duplicatePhoneNumberRatio * 2 + expirationDateRate + noExpirationDateRate;
		long noDupSize = expirationDateRate + noExpirationDateRate;
		long posInBlock = n % blockSize;
		long phoneNumber = n;
		if (posInBlock >= noDupSize && posInBlock % 2 == 0) {
			phoneNumber = n + 1;
		}
		String format = "%011d";
		return  String.format(format, phoneNumber);
	}


	/**
	 * n番目のレコードのDurationを返す
	 *
	 * @param n
	 * @return
	 */
	Duration getDuration(long  n) {
		return  durationList.get((int) (n % durationList.size()));
	}


	/**
	 * 期間を表すクラス
	 *
	 */
	public static class Duration {
		Date start;
		Date end;
		public Duration(Date start, Date end) {
			this.start = start;
			this.end = end;
		}
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((end == null) ? 0 : end.hashCode());
			result = prime * result + ((start == null) ? 0 : start.hashCode());
			return result;
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Duration other = (Duration) obj;
			if (end == null) {
				if (other.end != null)
					return false;
			} else if (!end.equals(other.end))
				return false;
			if (start == null) {
				if (other.start != null)
					return false;
			} else if (!start.equals(other.start))
				return false;
			return true;
		}
	}
}
