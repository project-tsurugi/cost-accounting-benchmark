package com.example.nedo.ddl;

import java.io.BufferedWriter;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import com.example.nedo.BenchConst;
import com.example.nedo.ddl.common.TableSheet;
import com.example.nedo.ddl.ddl.TableDdlWriter;

public class DdlGenarator {

	public static void main(String[] args) throws Exception {
		// new DdlExample().main(args[0]);
		new DdlGenarator().main(BenchConst.tableXlsxPath());
	}

	private void main(String src) throws Exception {
		File srcFile = new File(src);
		System.out.println(srcFile);

		File dstFile = new File(srcFile.getParent(), "ddl-postgresql.txt");
		System.out.println(dstFile);

		try (Workbook workbook = WorkbookFactory.create(srcFile);
				BufferedWriter writer = Files.newBufferedWriter(dstFile.toPath(), StandardCharsets.UTF_8)) {
			for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
				Sheet sheet = workbook.getSheetAt(i);
				TableSheet table = new TableSheet(sheet);

				TableDdlWriter c = new TableDdlWriter(table, writer);
				c.convert();
			}
		}
	}
}
