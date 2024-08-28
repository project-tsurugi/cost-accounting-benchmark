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
package com.tsurugidb.benchmark.costaccounting.debug.iceaxe_dbtest;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TsurugiSession;

public class DbSessions implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(DbSessions.class);

    private final TsurugiConnector connector;
    private final long timeout;
    private final TimeUnit timeUnit;
    private final List<TsurugiSession> sessionList = new ArrayList<>();

    public DbSessions(TsurugiConnector connector) {
        this(connector, 0, null);
    }

    public DbSessions(TsurugiConnector connector, long timeout, TimeUnit unit) {
        this.connector = connector;
        this.timeout = timeout;
        this.timeUnit = unit;
    }

    public TsurugiSession createSession() throws IOException {
        var sessionOption = TgSessionOption.of();
        if (timeUnit != null) {
            sessionOption.setTimeout(TgTimeoutKey.DEFAULT, timeout, timeUnit);
        }
        var session = connector.createSession(sessionOption);

        sessionList.add(session);
        return session;
    }

    @Override
    public void close() throws IOException {
        for (var session : sessionList) {
            try {
                session.close();
            } catch (Exception e) {
                LOG.warn("session close error", e.getMessage());
            }
        }
    }
}
