package com.example.nedo.jdbc.doma2.dao;

import java.time.LocalDate;
import java.util.List;

import org.seasar.doma.Dao;
import org.seasar.doma.Delete;
import org.seasar.doma.Insert;
import org.seasar.doma.Select;
import org.seasar.doma.Sql;

import com.example.nedo.jdbc.doma2.config.AppConfig;
import com.example.nedo.jdbc.doma2.domain.ItemType;
import com.example.nedo.jdbc.doma2.entity.ItemMaster;

@Dao(config = AppConfig.class)
public interface ItemMasterDao {

	public static final String TABLE_NAME = "item_master";

	static final String COND_DATE = "/* date */'2020-09-23' between i_effective_date and i_expired_date";

	@Delete
//	@Sql("delete from " + TABLE_NAME)
	@Sql("truncate table " + TABLE_NAME)
	int deleteAll();

	@Insert
	int insert(ItemMaster entity);

	@Select
	@Sql("select * from " + TABLE_NAME + " where i_id in /* ids */(1,2,3) and " + COND_DATE)
	List<ItemMaster> selectByIds(Iterable<Integer> ids, LocalDate date);

	@Select
	@Sql("select i_id from " + TABLE_NAME + " where " + COND_DATE + " and i_type = /* type */'product'")
	List<Integer> selectIdByType(LocalDate date, ItemType type);

	@Select
	@Sql("select * from " + TABLE_NAME + " where " + COND_DATE + " and i_type = /* type */'raw_material'")
	List<ItemMaster> selectByType(LocalDate date, ItemType type);

	@Select
	@Sql("select * from " + TABLE_NAME)
	List<ItemMaster> selectAll();

	@Select
	@Sql("select max(i_id) + 1 from " + TABLE_NAME)
	Integer selectMaxId();

	@Select
	@Sql("select * from " + TABLE_NAME + " where i_id = /* id */1 and " + COND_DATE)
	ItemMaster selectById(int id, LocalDate date);
}
