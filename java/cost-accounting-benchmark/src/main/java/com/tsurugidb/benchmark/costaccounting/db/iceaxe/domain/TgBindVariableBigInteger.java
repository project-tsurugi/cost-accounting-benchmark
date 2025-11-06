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
package com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameter;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;

public class TgBindVariableBigInteger extends TgBindVariable<BigInteger> {

    protected TgBindVariableBigInteger(String name) {
        super(name, TgDataType.DECIMAL);
    }

    @Override
    public TgBindParameter bind(BigInteger value) {
        BigDecimal v = (value != null) ? new BigDecimal(value) : null;
        return TgBindParameter.of(name(), v);
    }

    @Override
    public TgBindVariableBigInteger clone(String name) {
        return new TgBindVariableBigInteger(name);
    }
}
