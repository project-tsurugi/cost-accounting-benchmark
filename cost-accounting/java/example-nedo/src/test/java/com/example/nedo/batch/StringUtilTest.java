package com.example.nedo.batch;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

class StringUtilTest {

    @Test
    void testToStringListOfInteger() {
        assertEquals("[]", StringUtil.toString(Arrays.asList()));
        assertEquals("[1]", StringUtil.toString(Arrays.asList(1)));
        assertEquals("[1, 2]", StringUtil.toString(Arrays.asList(1, 2)));
        assertEquals("[1, 3]", StringUtil.toString(Arrays.asList(1, 3)));
        assertEquals("[1-3]", StringUtil.toString(Arrays.asList(1, 2, 3)));
        assertEquals("[10]", StringUtil.toString(Arrays.asList(10)));
        assertEquals("[10, 11]", StringUtil.toString(Arrays.asList(10, 11)));
        assertEquals("[10-12]", StringUtil.toString(Arrays.asList(10, 11, 12)));
        assertEquals("[1-3, 5]", StringUtil.toString(Arrays.asList(1, 2, 3, 5)));
        assertEquals("[1-3, 5, 6]", StringUtil.toString(Arrays.asList(1, 2, 3, 5, 6)));
        assertEquals("[1-3, 5-7]", StringUtil.toString(Arrays.asList(1, 2, 3, 5, 6, 7)));
        assertEquals("[1, 3-5]", StringUtil.toString(Arrays.asList(1, 3, 4, 5)));
        assertEquals("[1-3, 5, 7-9]", StringUtil.toString(Arrays.asList(1, 2, 3, 5, 7, 8, 9)));
    }
}
