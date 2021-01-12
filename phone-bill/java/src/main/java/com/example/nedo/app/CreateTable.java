package com.example.nedo.app;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.example.nedo.db.DBUtils;

public class CreateTable implements ExecutableCommand{
	Config config;

	public static void main(String[] args) throws SQLException, IOException {
		CreateTable createTable = new CreateTable();
		createTable.execute(args);
	}


	@Override
	public void execute(String[] args) throws SQLException, IOException {
		Config config = Config.getConfig(args);
		try (Connection conn = DBUtils.getConnection(config)) {
			conn.setAutoCommit(true);
			Statement stmt = conn.createStatement();
			dropTables(stmt);
			createHistoryTable(stmt);
			createContractsTable(stmt);
			createBillingTable(stmt);
		}
	}

	void createHistoryTable(Statement stmt) throws SQLException {
		String create_table = "create table history ("
				+ "caller_phone_number varchar(15) not null," 		// 発信者電話番号
				+ "recipient_phone_number varchar(15) not null," 	// 受信者電話番号
				+ "payment_categorty char(1) not null," 			// 料金区分
				+ "start_time timestamp not null,"			 		// 通話開始時刻
				+ "time_secs integer not null," 								// 通話時間(秒)
				+ "charge integer," 								// 料金
				+ "df boolean not null default '0'," 				// 論理削除フラグ
				+ "primary key(caller_phone_number, start_time)"
				+ ")";
		stmt.execute(create_table);

		String create_index = "CREATE INDEX IDX_DF ON HISTORY(DF)";
		stmt.execute(create_index);
	}

	void createContractsTable(Statement stmt) throws SQLException {
		String create_table = "create table contracts ("
				+ "phone_number varchar(15) not null," 		// 電話番号
				+ "start_date date not null," 				// 契約開始日
				+ "end_date date,"					// 契約終了日
				+ "charge_rule varchar(255) not null,"		// 料金計算ルール
				+ "primary key(phone_number, start_date)"
				+ ")";
		stmt.execute(create_table);
	}

	void createBillingTable(Statement stmt) throws SQLException {
		String create_table = "create table billing ("
				+ "phone_number varchar(15) not null," 		// 電話番号
				+ "target_month date not null," 			// 対象年月
				+ "basic_charge integer not null," 					// 基本料金
				+ "metered_charge integer not null,"					// 従量料金
				+ "billing_amount integer not null,"					// 請求金額
				+ "constraint  billing_pkey primary key(phone_number, target_month)"
				+ ")";
		stmt.execute(create_table);
	}


	void dropTables(Statement stmt) throws SQLException {
		// 通話履歴テーブル
		stmt.execute("drop table if exists history");

		// 契約マスタ
		stmt.execute("drop table if exists contracts");

		// 月額利用料金
		stmt.execute("drop table if exists billing");
	}
}
