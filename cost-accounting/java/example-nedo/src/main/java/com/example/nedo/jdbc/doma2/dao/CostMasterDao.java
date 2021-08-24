package com.example.nedo.jdbc.doma2.dao;

import java.math.BigDecimal;
import java.util.List;

import org.seasar.doma.Dao;
import org.seasar.doma.Delete;
import org.seasar.doma.Insert;
import org.seasar.doma.Select;
import org.seasar.doma.Sql;
import org.seasar.doma.Update;

import com.example.nedo.jdbc.doma2.config.AppConfig;
import com.example.nedo.jdbc.doma2.entity.CostMaster;

@Dao(config = AppConfig.class)
public interface CostMasterDao {

	public static final String TABLE_NAME = "cost_master";

	@Delete
//	@Sql("delete from " + TABLE_NAME)
	@Sql("truncate table " + TABLE_NAME)
	int deleteAll();

	@Insert
	int insert(CostMaster entity);

	@Select
	@Sql("select * from " + TABLE_NAME + " where c_f_id = /*fId*/1")
	List<CostMaster> selectByFactory(int fId);

	@Select
	@Sql("select * from " + TABLE_NAME + " where c_f_id = /*fId*/1 and c_i_id = /*iId*/2")
	CostMaster selectById(int fId, int iId);

	@Select
	@Sql("select * from " + TABLE_NAME + " where c_f_id=/*in.cFId*/1 and c_i_id=/*in.cIId*/2 for update")
	CostMaster lock(CostMaster in);

	@Update
	@Sql("update " + TABLE_NAME + " set" //
			+ " c_stock_quantity = c_stock_quantity + /*quantity*/1" //
			+ ",c_stock_amount = c_stock_amount + /*amount*/1" //
			+ " where c_f_id=/*entity.cFId*/1 and c_i_id=/*entity.cIId*/2")
	int updateIncrease(CostMaster entity, BigDecimal quantity, BigDecimal amount);

	@Update
	@Sql("update " + TABLE_NAME + " set" //
			+ " c_stock_quantity = c_stock_quantity - /*quantity*/1" //
			+ ",c_stock_amount = c_stock_amount - c_stock_amount * /*quantity*/1 / c_stock_quantity" //
			+ " where c_f_id=/*entity.cFId*/1 and c_i_id=/*entity.cIId*/2")
	int updateDecrease(CostMaster entity, BigDecimal quantity);
}
