package com.example.nedo.init;

import java.io.File;
import java.io.IOException;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.seasar.doma.jdbc.tx.TransactionManager;

import com.example.nedo.BenchConst;
import com.example.nedo.ddl.common.SheetWrapper;
import com.example.nedo.jdbc.doma2.config.AppConfig;
import com.example.nedo.jdbc.doma2.dao.MeasurementMasterDao;
import com.example.nedo.jdbc.doma2.dao.MeasurementMasterDaoImpl;
import com.example.nedo.jdbc.doma2.domain.MeasurementType;
import com.example.nedo.jdbc.doma2.entity.MeasurementMaster;

public class InitialData01MeasurementMaster extends InitialData {

	public static void main(String[] args) throws Exception {
//		new InitialDataMeasureMaster().main(args[0]);
		new InitialData01MeasurementMaster().main(BenchConst.MEASUREMENT_XLSX);
	}

	public InitialData01MeasurementMaster() {
		super(null);
	}

	private void main(String src) throws IOException {
		logStart();

		File srcFile = new File(src);
		System.out.println(srcFile);

		try (Workbook workbook = WorkbookFactory.create(srcFile)) {
			Sheet sheet = workbook.getSheet("measurement");
			TableSheet table = new TableSheet(sheet);

			generateMetrologyMaster(table);
		}

		logEnd();
	}

	private static class TableSheet extends SheetWrapper {

		public TableSheet(Sheet sheet) {
			super(sheet);
		}

		@Override
		protected int getStartRowIndex() {
			return 2;
		}

		@Override
		public boolean hasData(Row row) {
			if (row == null) {
				return false;
			}
			Cell cell = row.getCell(0);
			if (cell == null) {
				return false;
			}
			return true;
		}
	}

	private void generateMetrologyMaster(TableSheet table) {
		MeasurementMasterDao dao = new MeasurementMasterDaoImpl();

		TransactionManager tm = AppConfig.singleton().getTransactionManager();

		tm.required(() -> {
			dao.deleteAll();
			insertMeasureMaster(table, dao);
		});
	}

	private void insertMeasureMaster(TableSheet table, MeasurementMasterDao dao) {
		table.getRows().forEachOrdered(row -> {
			MeasurementMaster entity = new MeasurementMaster();
			int c = 0;
			entity.setMUnit(table.getCellAsString(row, c++));
			entity.setMName(table.getCellAsString(row, c++));
			entity.setMType(MeasurementType.of(table.getCellAsString(row, c++)));
			entity.setMDefaultUnit(table.getCellAsString(row, c++));
			entity.setMScale(table.getCellAsDecimal(row, c++));

			dao.insert(entity);
		});
	}
}
