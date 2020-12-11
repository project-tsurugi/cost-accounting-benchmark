package com.example.nedo.ddl.common;

import java.io.BufferedWriter;
import java.io.IOException;

public class WriterWrapper {

	protected final BufferedWriter writer;
	private final String tabString;

	public WriterWrapper(BufferedWriter writer, String tab) {
		this.writer = writer;
		this.tabString = tab;
	}

	// Writer

	protected void writeln(int tab, String... ss) throws IOException {
		for (int i = 0; i < tab; i++) {
			writer.write(tabString);
		}
		writeln(ss);
	}

	protected void writeln(String... ss) throws IOException {
		for (String s : ss) {
			writer.write(s);
		}
		writer.write("\n");
	}

	protected void writef(String format, String... args) throws IOException {
		String s = String.format(format, (Object[]) args);
		writer.write(s);
	}
}
