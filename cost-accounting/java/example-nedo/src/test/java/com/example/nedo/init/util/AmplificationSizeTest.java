package com.example.nedo.init.util;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class AmplificationSizeTest {

    @Test
    void testAmplificationCount() {
        {
            AmplificationSize a = new AmplificationSize(3);
            int count = 0;
            final int SIZE = 10;
            for (int i = 1; i <= SIZE; i++) {
                int actual = a.amplificationSize(i);
                assertEquals(2, actual);
                count += 1 + actual;
            }
            assertEquals(SIZE * 3, count);
        }
        {
            AmplificationSize a = new AmplificationSize(3.0);
            for (int i = 1; i <= 10; i++) {
                assertEquals(2, a.amplificationSize(i));
            }
        }

        {
            AmplificationSize a = new AmplificationSize(1.5);
            int count = 0;
            final int SIZE = 10;
            for (int i = 0; i < SIZE; i++) {
                int actual = a.amplificationSize(i + 1);
                assertEquals((i % 2 == 0) ? 1 : 0, actual);
                count += 1 + actual;
            }
            assertEquals(SIZE * 1.5, count);
        }
        {
            AmplificationSize a = new AmplificationSize(1.25);
            for (int i = 0; i < 10; i++) {
                assertEquals((i % 4 == 0) ? 1 : 0, a.amplificationSize(i + 1));
            }
        }
        {
            AmplificationSize a = new AmplificationSize(1.6);
            for (int i = 0; i < 10; i++) {
                assertEquals((i % 5 < 3) ? 1 : 0, a.amplificationSize(i + 1));
            }
        }
    }
}
