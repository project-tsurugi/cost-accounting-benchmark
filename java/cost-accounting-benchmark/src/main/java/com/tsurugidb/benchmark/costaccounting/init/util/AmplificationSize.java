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
