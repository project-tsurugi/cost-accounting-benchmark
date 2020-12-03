package com.example.nedo.ddl.ddl;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import org.apache.poi.ss.usermodel.Row;

import com.example.nedo.ddl.common.TableSheet;
import com.example.nedo.ddl.common.WriterWrapper;

public abstract class TableDdlWriter extends WriterWrapper {

	private final TableSheet table;

	public TableDdlWriter(TableSheet table, BufferedWriter writer) {
		super(writer, "  ");
		this.table = table;
	}

	public void convert() throws IOException {
		System.out.println(table.getSheetName());

		writeComment();
		writeDrop();
		writeCreate();

		writeln("(");
		table.getRows().forEachOrdered(row -> {
			try {
				writeField(row);
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		});

		List<String> pkList = table.getPrimaryKeyList();
		if (!pkList.isEmpty()) {
			String keys = String.join(", ", pkList);
			writeln(1, ",primary key(", keys, ")");
		}

		writeln(");");
	}

	protected void writeComment() throws IOException {
		writeln();

		String tableLogicalName = table.getTableLogicalName();
		writeln("-- ", tableLogicalName);
	}

	protected void writeDrop() throws IOException {
		String tableName = table.getTableName();
		System.out.println(tableName);
		writeln("drop table ", tableName, ";");
	}

	protected void writeCreate() throws IOException {
		String tableName = table.getTableName();
		writeln("create table ", tableName);
	}

	protected void writeField(Row row) throws IOException {
		String name = table.getColumnName(row);
		if (name == null) {
			return;
		}

		String desc = table.getColumnLogicalName(row);
		if (desc == null) {
			desc = "";
		} else {
			desc = " -- " + desc;
		}

		String type = getType(row);

		String comma;
		if (table.hasNext(row)) {
			comma = ",";
		} else {
			comma = "";
		}

		writeln(1, name, " ", type, comma, desc);
	}

	protected String getType(Row row) {
		String typeName = table.getColumnType(row);
		if (typeName == null) {
			return "";
		}
		return getType(row, typeName);
	}

	protected abstract String getType(Row row, String typeName);

	protected String getTypeWithSize(Row row, String type) {
		Integer size = table.getColumnTypeSize(row);
		if (size == null) {
			return type;
		} else {
			Integer scale = table.getColumnTypeScale(row);
			if (scale == null) {
				return type + "(" + size + ")";
			} else {
				return type + "(" + size + ", " + scale + ")";
			}
		}
	}
}
