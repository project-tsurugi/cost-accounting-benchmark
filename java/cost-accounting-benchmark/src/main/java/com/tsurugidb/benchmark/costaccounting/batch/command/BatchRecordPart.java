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
package com.tsurugidb.benchmark.costaccounting.batch.command;

public class BatchRecordPart {

    private final String line;
    String dbmsType;
    String option;
    String onlineCoverRate;
    String scope;
    String label;
    double elapsed;
    String vsz;
    String rss;

    public BatchRecordPart(String line) {
        this.line = line;
    }

    public String getDbmsType() {
        return dbmsType;
    }

    public String getOption() {
        return option;
    }

    public String getOnlineCoverRate() {
        return onlineCoverRate;
    }

    public String getScope() {
        return scope;
    }

    public String getLabel() {
        return label;
    }

    public double getElapsed() {
        return elapsed;
    }

    public String getVsz() {
        return vsz;
    }

    public String getRss() {
        return rss;
    }

    @Override
    public String toString() {
        return "[" + this.line + "]";
    }
}
