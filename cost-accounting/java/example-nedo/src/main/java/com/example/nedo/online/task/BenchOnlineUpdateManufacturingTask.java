package com.example.nedo.online.task;

import java.math.BigInteger;
import java.time.LocalDate;
import java.util.List;

import org.seasar.doma.jdbc.tx.TransactionManager;

import com.example.nedo.init.InitialData;
import com.example.nedo.init.InitialData04ItemManufacturingMaster;
import com.example.nedo.jdbc.doma2.config.AppConfig;
import com.example.nedo.jdbc.doma2.dao.FactoryMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ItemConstructionMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ItemManufacturingMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ItemMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ResultTableDaoImpl;
import com.example.nedo.jdbc.doma2.domain.ItemType;
import com.example.nedo.jdbc.doma2.entity.ItemManufacturingMaster;

/**
 * 生産数の変更
 */
public class BenchOnlineUpdateManufacturingTask extends BenchOnlineTask {

	public BenchOnlineUpdateManufacturingTask() {
		super("update-manufacturing");
	}

	@Override
	public void execute() {
		logStart("factory=%d, date=%s", factoryId, date);

		tm.required(() -> {
			int productId = selectRandomItemId();

			logTarget("factory=%d, date=%s, product=%d", factoryId, date, productId);
			executeMain(productId);
		});

		logEnd("factory=%d, date=%s", factoryId, date);
	}

	protected void executeMain(int productId) {
		int newQuantity = random.random(0, 500) * 100;

		ItemManufacturingMaster entity = itemManufacturingMasterDao.selectByIdForUpdate(factoryId, productId, date);
		if (entity == null) {
			InitialData04ItemManufacturingMaster initialData = new InitialData04ItemManufacturingMaster(1, date);
			entity = initialData.newItemManufacturingMaster(factoryId, productId);
			entity.setEffectiveDate(date);
			{
				List<ItemManufacturingMaster> list = itemManufacturingMasterDao.selectByIdFuture(productId, productId,
						date);
				if (!list.isEmpty()) {
					ItemManufacturingMaster min = list.get(0);
					entity.setExpiredDate(min.getEffectiveDate().minusDays(1));
				} else {
					entity.setExpiredDate(LocalDate.of(9999, 12, 31));
				}
			}
			entity.setImManufacturingQuantity(BigInteger.valueOf(newQuantity));
			itemManufacturingMasterDao.insert(entity);
		} else {
			entity.setImManufacturingQuantity(BigInteger.valueOf(newQuantity));
			itemManufacturingMasterDao.update(entity);
		}
	}

	protected int selectRandomItemId() {
		List<Integer> list = itemMasterDao.selectIdByType(date, ItemType.PRODUCT);
		int i = random.nextInt(list.size());
		return list.get(i);
	}

	// for test
	public static void main(String[] args) {
		BenchOnlineUpdateManufacturingTask task = new BenchOnlineUpdateManufacturingTask();

		TransactionManager tm = AppConfig.singleton().getTransactionManager();
		task.setDao(tm, new ItemManufacturingMasterDaoImpl(), new FactoryMasterDaoImpl(), new ItemMasterDaoImpl(),
				new ItemConstructionMasterDaoImpl(), null, new ResultTableDaoImpl());

		task.initialize(1, InitialData.DEFAULT_BATCH_DATE);

//		task.execute();
		tm.required(() -> {
			task.executeMain(49666);
		});
	}
}
