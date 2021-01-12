package com.example.nedo.app;

import java.io.IOException;
import java.sql.SQLException;

public interface ExecutableCommand {
	void execute(String[] args) throws SQLException, IOException;
}
