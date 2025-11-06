/*
 * Copyright 2023-2025 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.benchmark.costaccounting.init.util;

import org.apache.commons.math3.fraction.Fraction;

/**
 * データ増幅件数
 */
public class AmplificationSize {

    private final int magnification;
    private final int numerator;
    private final int denominator;

    public AmplificationSize(double magnification) {
        Fraction f = new Fraction(magnification);

        int n = f.intValue();
        this.magnification = n - 1;
        assert this.magnification >= 0;

        f = f.subtract(n);
        this.numerator = f.getNumerator();
        this.denominator = f.getDenominator();
    }

    public int amplificationSize(int id) {
        assert id >= 1;

        int n = magnification;

        int m = (id - 1) % denominator;
        if (m < numerator) {
            n++;
        }

        return n;
    }
}
