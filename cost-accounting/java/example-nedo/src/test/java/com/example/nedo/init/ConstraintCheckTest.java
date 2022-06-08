package com.example.nedo.init;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.example.nedo.db.doma2.entity.ItemMaster;
import com.example.nedo.init.ConstraintCheck.ConstraintCheckException;

class ConstraintCheckTest {

    @Test
    void testCheck() {
        String tableName = "test";

        List<ItemMaster> allList = new ArrayList<>();
        allList.add(create(1, LocalDate.of(2020, 9, 1), LocalDate.of(2020, 9, 30)));
        allList.add(create(2, LocalDate.of(2020, 9, 1), LocalDate.of(2020, 9, 30)));
        allList.add(create(2, LocalDate.of(2020, 10, 1), LocalDate.of(2020, 10, 31)));
        allList.add(create(3, LocalDate.of(2020, 9, 1), LocalDate.of(2020, 9, 30)));
        allList.add(create(3, LocalDate.of(2020, 10, 1), LocalDate.of(2020, 10, 31)));
        allList.add(create(3, LocalDate.of(2020, 11, 1), LocalDate.of(2020, 11, 30)));

        ConstraintCheck target = new ConstraintCheck();
        assertDoesNotThrow(() -> target.checkDateRange(tableName, allList, target::getKey));
    }

    @Test
    void testCheck_error() {
        String tableName = "test";

        List<ItemMaster> allList = new ArrayList<>();
        allList.add(create(1, LocalDate.of(2020, 9, 1), LocalDate.of(2020, 9, 30)));
        allList.add(create(2, LocalDate.of(2020, 9, 1), LocalDate.of(2020, 9, 30)));
        allList.add(create(2, LocalDate.of(2020, 10, 1), LocalDate.of(2020, 10, 31)));
        allList.add(create(3, LocalDate.of(2020, 9, 1), LocalDate.of(2020, 9, 30)));
        allList.add(create(3, LocalDate.of(2020, 9, 30), LocalDate.of(2020, 10, 31)));
        allList.add(create(3, LocalDate.of(2020, 11, 1), LocalDate.of(2020, 11, 30)));

        ConstraintCheck target = new ConstraintCheck();
        ConstraintCheckException e = assertThrows(ConstraintCheckException.class, () -> target.checkDateRange(tableName, allList, target::getKey));
        assertEquals("constraint check error test[3]", e.getMessage());
    }

    private ItemMaster create(int id, LocalDate startDate, LocalDate endDate) {
        ItemMaster entity = new ItemMaster();
        entity.setIId(id);
        entity.setIEffectiveDate(startDate);
        entity.setIExpiredDate(endDate);
        return entity;
    }
}
