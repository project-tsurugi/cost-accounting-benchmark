package com.tsurugidb.benchmark.costaccounting.batch;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class StringUtil {

    public static List<Integer> toIntegerList(String arg) {
        List<Integer> list = new ArrayList<>();

        String[] ss = arg.split(",");
        for (String s : ss) {
            int n = s.indexOf('-');
            if (n >= 0) {
                int start = Integer.parseInt(s.substring(0, n).trim());
                int end = Integer.parseInt(s.substring(n + 1).trim());
                IntStream.rangeClosed(start, end).forEach(id -> list.add(id));
            } else {
                list.add(Integer.parseInt(s.trim()));
            }
        }

        return list;
    }

    public static String toString(List<Integer> list) {
        StringBuilder sb = new StringBuilder();
        sb.append('[');

        int end = 0;
        int count = 0;
        String comma = "";
        for (int i = 0; i < list.size() + 1; i++) {
            int n = Integer.MAX_VALUE;
            if (i < list.size()) {
                n = list.get(i);
                if (i > 0 && n == list.get(i - 1).intValue() + 1) {
                    end = n;
                    count++;
                    continue;
                }
            }

            if (end > 0) {
                if (count > 2) {
                    sb.append('-');
                } else {
                    sb.append(", ");
                }
                sb.append(end);
                end = 0;
            }

            if (n != Integer.MAX_VALUE) {
                sb.append(comma);
                comma = ", ";
                sb.append(n);
                count = 1;
            }
        }

        sb.append(']');
        return sb.toString();
    }
}
