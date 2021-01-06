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
import com.example.nedo.db.History;

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
	 * 契約期間のパターンを記録するリスト
	 */
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

			for(long n = 0; n < numberOfContractsRecords; n++) {
				Duration d = getDuration(n);
				String rule = "Simple";
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
		isValidDurationList(durationList, minDate, maxDate);

		Duration targetDuration = new Duration(minDate, maxDate);

		try (Connection conn = DBUtils.getConnection()) {
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
			for(long n = 0; n < numberOfHistoryRecords; n++) {
				History h = createHistoryRecord(targetDuration);
				ps.setString(1, h.caller_phone_number);
				ps.setString(2, h.recipient_phone_number);
				ps.setString(3, h.payment_categorty);
				ps.setTimestamp(4, h.start_time);
				ps.setInt(5, h.time_secs);
				ps.setInt(6, h.charge);
				ps.setBoolean(7, h.df);
				ps.addBatch();
				if (++batchSize == SQL_BATCH_EXEC_SIZE) {
					execBatch(ps);
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
		for(Date date = minDate; date.getTime() <= maxDate.getTime(); date = DBUtils.nextDate(date)) {
			int c = 0;
			for (Duration duration: list) {
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
		long startTime = getRandomLong(targetDuration.start.getTime(), targetDuration.end.getTime());
		history.start_time = new Timestamp(startTime);

		 // 電話番号の生成
		long caller = selectContract(startTime, -1,getRandomLong(0, numberOfContractsRecords));
		long recipient = selectContract(startTime, caller,getRandomLong(0, numberOfContractsRecords));
		history.caller_phone_number = getPhoneNumber(caller);
		history.recipient_phone_number = getPhoneNumber(recipient);


		// 料金区分(発信者負担、受信社負担)
		// TODO 割合を指定可能にする
		history.payment_categorty = random.nextInt(2) == 0 ? "C" : "R";


		// 通話時間
		// TODO 分布関数を指定可能にする
		history.time_secs = random.nextInt(3600)+1;

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
		int c=0;
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
			if (pos >= numberOfContractsRecords) {
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

	/**
	 * min以上max未満のランダムなlong値を取得する
	 *
	 * @param min
	 * @param max
	 * @return
	 */
	private long getRandomLong(long min, long max) {
		return min + (long)(random.nextDouble() * (max - min));
	}
}
