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
package com.tsurugidb.benchmark.costaccounting.time;

import java.util.concurrent.atomic.AtomicInteger;

import com.tsurugidb.benchmark.costaccounting.db.DbmsType;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst.IsolationLevel;

public class TimeRecord {

    public static String header() {
        return "dbmsType, option, table, size, sql, elapsed[ms], begin[ms], main[ms], commit[ms], tryCount";
    }

    private final DbmsType dbmsType;
    private final String managerName;
    private final IsolationLevel isolationLevel;
    private final String txOption;
    private final String tableName;
    private final String size;
    private final String sqlName;
    private long start;
    private long startMain;
    private long beforeCommit;
    private long end;
    private final AtomicInteger tryCount = new AtomicInteger(0);

    public TimeRecord(DbmsType dbmsType, String managerName, IsolationLevel isolationLevel, String option, String tableName, String size, String sqlName) {
        this.dbmsType = dbmsType;
        this.managerName = managerName;
        this.isolationLevel = isolationLevel;
        this.txOption = option;
        this.tableName = tableName;
        this.size = size;
        this.sqlName = sqlName;
    }

    public void start() {
        this.start = System.currentTimeMillis();
    }

    public void startMain() {
        this.startMain = System.currentTimeMillis();
        tryCount.incrementAndGet();
    }

    public void beforeCommit() {
        this.beforeCommit = System.currentTimeMillis();
    }

    public void finish() {
        this.end = System.currentTimeMillis();
    }

    public String dbmsType() {
        if (dbmsType != DbmsType.TSURUGI) {
            return dbmsType.toString();
        }
        return this.managerName;
    }

    public String option() {
        if (dbmsType != DbmsType.TSURUGI) {
            return isolationLevel.name();
        }
        return this.txOption;
    }

    public String size() {
        return this.size;
    }

    public String numberOfDifference() {
        return "-";
    }

    public long beginMillis() {
        return startMain - start;
    }

    public long mainMillis() {
        return beforeCommit - startMain;
    }

    public long commitMillis() {
        return end - beforeCommit;
    }

    public long elapsedMillis() {
        return end - start;
    }

    public int abortCount() {
        return tryCount.get() - 1;
    }

    public String getParamString() {
        var sb = new StringBuilder(64);
        sb.append("dbmsType=");
        sb.append(dbmsType());
        sb.append(", option=");
        sb.append(option());
        sb.append(", table=");
        sb.append(tableName);
        sb.append(", size=");
        sb.append(size);
        sb.append(", sql=");
        sb.append(sqlName);
        return sb.toString();
    }

    @Override
    public String toString() {
        var sb = new StringBuilder(64);
        sb.append(dbmsType());
        sb.append(",");
        sb.append(option());
        sb.append(",");
        sb.append(tableName);
        sb.append(",");
        sb.append(size());
        sb.append(",");
        sb.append(sqlName);
        sb.append(",");
        sb.append(elapsedMillis());
        sb.append(",");
        sb.append(beginMillis());
        sb.append(",");
        sb.append(mainMillis());
        sb.append(",");
        sb.append(commitMillis());
        sb.append(",");
        sb.append(tryCount);
        return sb.toString();
    }
}
