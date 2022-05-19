package com.example.nedo.online;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.NavigableMap;
import java.util.TreeMap;

import org.junit.jupiter.api.Test;

public class BenchOnlineThreadTest {

    @Test
    void exampleGetTaskRandom() {
        NavigableMap<Integer, String> map = new TreeMap<>();
        map.put(20, "A");
        map.put(20 + 30, "B");
        map.put(20 + 30 + 50, "C");

        for (int i = 0; i < 20; i++) {
            int key = i;
            assertEquals("A", map.higherEntry(key).getValue(), "key=" + key);
        }
        for (int i = 0; i < 30; i++) {
            int key = 20 + i;
            assertEquals("B", map.higherEntry(key).getValue(), "key=" + key);
        }
        for (int i = 0; i < 50; i++) {
            int key = 20 + 30 + i;
            assertEquals("C", map.higherEntry(key).getValue(), "key=" + key);
        }
    }
}
