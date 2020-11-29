package com.example.nedo.ddl;

import java.io.BufferedWriter;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import com.example.nedo.BenchConst;
import com.example.nedo.ddl.common.TableSheet;
import com.example.nedo.ddl.entity.TableEntityWriter;

public class EntityGenerator {

	public static void main(String[] args) throws Exception {
		// new EntityExample().main(args[0], args[1]);
		new EntityGenerator().main(BenchConst.TABLE_XLSX,
				BenchConst.SRC_DIR + "/" + BenchConst.PACKAGE_ENTITY.replace('.', '/'));
	}

	private void main(String src, String dst) throws Exception {
		File srcFile = new File(src);
		System.out.println(srcFile);

		Path dstDir = Paths.get(dst);

		try (Workbook workbook = WorkbookFactory.create(srcFile)) {
			for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
				Sheet sheet = workbook.getSheetAt(i);
				TableSheet table = new TableSheet(sheet);
				String className = table.getClassName();

				Path dstFile = dstDir.resolve(className + ".java");
				System.out.println(dstFile);

				try (BufferedWriter writer = Files.newBufferedWriter(dstFile, StandardCharsets.UTF_8)) {
					TableEntityWriter c = new TableEntityWriter(table, writer);
					c.convert();
				}
			}
		}
	}
}
