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
package com.tsurugidb.benchmark.costaccounting.generate.util;

import java.io.IOException;
import java.io.Writer;

public class WriterWrapper {

    private Writer writer;
    private final String tabString;

    public WriterWrapper(Writer writer, String tab) {
        this.writer = writer;
        this.tabString = tab;
    }

    // Writer

    protected void setWriter(Writer writer) {
        this.writer = writer;
    }

    protected void writeln(int tab, String... ss) throws IOException {
        for (int i = 0; i < tab; i++) {
            writer.write(tabString);
        }
        writeln(ss);
    }

    protected void writeln(String... ss) throws IOException {
        for (String s : ss) {
            writer.write(s);
        }
        writer.write("\n");
    }

    protected void writef(String format, String... args) throws IOException {
        String s = String.format(format, (Object[]) args);
        writer.write(s);
    }
}
