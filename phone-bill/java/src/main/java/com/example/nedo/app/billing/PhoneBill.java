package com.example.nedo.app;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.example.nedo.db.Contract;
import com.example.nedo.db.DBUtils;

public class PhoneBill implements ExecutableCommand {
	public static void main(String[] args) throws SQLException {
		PhoneBill phoneBill = new PhoneBill();
		phoneBill.execute(args);
	}



	Connection conn;

	@Override
	public void execute(String[] args) throws SQLException {
		// TODO 引数パラメータの処理を追加
		Date start = DBUtils.toDate("2020-12-01");
		Date end = DBUtils.toDate("2021-01-01");

		init();
		ResultSet contractResultSet = getContractResultSet(start, end);
		while (contractResultSet.next()) {
			Contract contract = getContract(contractResultSet);
			System.out.println(contract);
		}
	}




	private Contract getContract(ResultSet rs) throws SQLException {
		Contract contract = new Contract();
		contract.phoneNumber = rs.getString(1);
		contract.startDate = rs.getDate(2);
		contract.endDate = rs.getDate(3);
		contract.rule = rs.getString(4);
		return contract;
	}




	/**
	 * 契約期間がstart～endと被るcontractテーブルのレコードのResultSetを取得する
	 *
	 * @param start
	 * @param end
	 * @return
	 * @throws SQLException
	 */
	private ResultSet getContractResultSet(Date start, Date end) throws SQLException {
		String sql = "select phone_number, start_date, end_date, charge_rule"
				+ " from contracts where start_date < ? and ( end_date is null or end_date > ?)";
		PreparedStatement ps = conn.prepareStatement(sql);
		ps.setDate(1, end);
		ps.setDate(2, start);
		ResultSet rs = ps.executeQuery();
		return rs;
	}

	private void init() {
		conn = DBUtils.getConnection();

	}

}
