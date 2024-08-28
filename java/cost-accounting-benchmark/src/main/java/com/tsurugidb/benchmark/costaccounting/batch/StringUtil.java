/*
 * Copyright 2023-2024 Project Tsurugi.
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
package com.tsurugidb.benchmark.costaccounting.batch;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class StringUtil {

    public static List<Integer> toIntegerList(String arg) {
        if (arg.trim().equalsIgnoreCase("all")) {
            return List.of();
        }

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
        return toString(list, ", ");
    }

    public static String toString(List<Integer> list, String delimiter) {
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
                    sb.append(delimiter);
                }
                sb.append(end);
                end = 0;
            }

            if (n != Integer.MAX_VALUE) {
                sb.append(comma);
                comma = delimiter;
                sb.append(n);
                count = 1;
            }
        }

        sb.append(']');
        return sb.toString();
    }
}
