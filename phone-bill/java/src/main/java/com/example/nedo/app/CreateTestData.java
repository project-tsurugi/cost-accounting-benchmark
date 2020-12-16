package com.example.nedo.app;

import java.sql.SQLException;
import java.util.Random;

public class CreateTestData implements ExecutableCommand {
	private Random random;
	

	public static void main(String[] args) throws SQLException {
		CreateTestData createTestData = new CreateTestData();
		createTestData.execute(args);
	}


	@Override
	public void execute(String[] args) throws SQLException {
		random = new Random(0); // seedを引数から設定する

	}

	
}
