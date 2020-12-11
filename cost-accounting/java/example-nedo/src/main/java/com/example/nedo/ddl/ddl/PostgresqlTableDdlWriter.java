package com.example.nedo.ddl.ddl;

import java.io.BufferedWriter;

import org.apache.poi.ss.usermodel.Row;

import com.example.nedo.ddl.common.TableSheet;

public class PostgresqlTableDdlWriter extends TableDdlWriter {

	public PostgresqlTableDdlWriter(TableSheet table, BufferedWriter writer) {
		super(table, writer);
	}

	@Override
	protected String getType(Row row, String typeName) {
		switch (typeName) {
		case "unique ID":
		case "unsigned numeric":
			return getTypeWithSize(row, "numeric");
		case "variable text":
			return getTypeWithSize(row, "varchar");
		case "date":
			return "date";
		default:
			throw new UnsupportedOperationException(typeName);
		}
	}
}
