package com.example.nedo.app;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Main {
	private static final Map<String, ExecutableCommand> COMMANDS_MAP = new HashMap<>();
	static {
		COMMANDS_MAP.put("CreateTable", new CreateTable());
	}

	private static final Set<String> COMMANDS = new HashSet<String>(Arrays.asList("CreateTable", "CreateTestData"));

	public static void main(String[] args) throws SQLException {
		if (args.length == 0) {
			System.err.println("No argument is specified. Please specify one of the following arguments.");
			usage();
			System.exit(1);
		}
		String cmd = args[0];
		ExecutableCommand executableCommand = COMMANDS_MAP.get(cmd);
		if (executableCommand == null) {
			System.err.println("Command '" + cmd + "' is not supprted. Please specify one of the following commands.");
			usage();
			System.exit(1);
		}
		executableCommand.execute(args);
	}


	private static void usage() {
		for(String cmd: COMMANDS) {
			System.err.println("  " + cmd);
		}
	}
}
