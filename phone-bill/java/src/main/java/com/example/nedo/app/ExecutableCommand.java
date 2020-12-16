package com.example.nedo.app;

import java.sql.SQLException;

public interface ExecutableCommand {
	void execute(String[] args) throws SQLException;
}
