package com.example.nedo.app;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.example.nedo.app.billing.PhoneBill;
import com.example.nedo.testdata.CreateTestData;

public class Main {
	private static final Map<String, ExecutableCommand> COMMAND_MAP = new HashMap<>();
	static {
		COMMAND_MAP.put("CreateTable", new CreateTable());
		COMMAND_MAP.put("CreateTestData", new CreateTestData());
		COMMAND_MAP.put("PhoneBill", new PhoneBill());
	}

	public static void main(String[] args) throws SQLException {
		if (args.length == 0) {
			System.err.println("No argument is specified. Please specify one of the following arguments.");
			usage();
			System.exit(1);
		}
		String cmd = args[0];
		ExecutableCommand executableCommand = COMMAND_MAP.get(cmd);
		if (executableCommand == null) {
			System.err.println("Command '" + cmd + "' is not supprted. Please specify one of the following commands.");
			usage();
			System.exit(1);
		}
		executableCommand.execute(args);
	}


	private static void usage() {
		for(String cmd: COMMAND_MAP.keySet()) {
			System.err.println("  " + cmd);
		}
	}
}
