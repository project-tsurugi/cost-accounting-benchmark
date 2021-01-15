package com.example.nedo.jdbc.doma2.dao;

import java.time.LocalDate;
import java.util.List;
import java.util.stream.Stream;

import org.seasar.doma.Dao;
import org.seasar.doma.Delete;
import org.seasar.doma.Insert;
import org.seasar.doma.Select;
import org.seasar.doma.Sql;
import org.seasar.doma.Suppress;
import org.seasar.doma.message.Message;

import com.example.nedo.jdbc.doma2.config.AppConfig;
import com.example.nedo.jdbc.doma2.domain.ItemType;
import com.example.nedo.jdbc.doma2.entity.ItemConstructionMaster;
import com.example.nedo.jdbc.doma2.entity.ItemConstructionMasterIds;

@Dao(config = AppConfig.class)
public interface ItemConstructionMasterDao {

	public static final String TABLE_NAME = "item_construction_master";

	static final String COND_DATE = "/* date */'2020-09-23' between ic_effective_date and ic_expired_date";

	@Delete
//	@Sql("delete from " + TABLE_NAME)
	@Sql("truncate table " + TABLE_NAME)
	int deleteAll();

	@Insert
	int insert(ItemConstructionMaster entity);

	@Select
	@Sql("select * from " + TABLE_NAME + " where " + COND_DATE + " order by ic_parent_i_id, ic_i_id")
	List<ItemConstructionMaster> selectAll(LocalDate date);

	@Select
	@Sql("select * from " + TABLE_NAME)
	List<ItemConstructionMaster> selectAll();

	@Select
	@Sql("select ic_parent_i_id, ic_i_id from " + TABLE_NAME + " where " + COND_DATE
			+ " order by ic_parent_i_id, ic_i_id")
	List<ItemConstructionMasterIds> selectIds(LocalDate date);

	@Select
	@Sql("select * from " + TABLE_NAME + " where ic_parent_i_id = /*parentId*/1 and " + COND_DATE + " order by ic_i_id")
	List<ItemConstructionMaster> selectByParentId(int parentId, LocalDate date);

	@Select
	@Sql("select * from " + TABLE_NAME + " where ic_parent_i_id = /*parentId*/1 and ic_i_id = /*itemId*/2 and " + COND_DATE)
	ItemConstructionMaster selectById(int parentId, int itemId, LocalDate date);

	@Select
	@Suppress(messages = { Message.DOMA4274 })
	Stream<ItemConstructionMaster> selectRecursiveByParentId(int parentId, LocalDate date);

	@Select
	List<ItemConstructionMaster> selectByItemType(LocalDate date, List<ItemType> typeList);

	@Select
	@Sql("select * from " + TABLE_NAME
			+ " where ic_i_id=/*in.icIId*/1 and ic_parent_i_id=/*in.icParentIId*/2 and ic_effective_date=/*in.icEffectiveDate*/'2020-09-23'"
			+ " for update")
	ItemConstructionMaster lock(ItemConstructionMaster in);

	@Delete
	int delete(ItemConstructionMaster entity);
}
