/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.breaker;

import java.util.function.LongConsumer;
import java.util.function.LongUnaryOperator;

public final class SingleRamAccounting implements RamAccounting {

    private static final int BLOCK_SIZE_IN_BYTES = 1024 * 1024 * 2;
    private final LongUnaryOperator reserveBytes;
    private final LongConsumer releaseBytes;

    private long reservedBytes;
    private long usedBytes;

    public SingleRamAccounting(LongUnaryOperator reserveBytes, LongConsumer releaseBytes) {
        this.reserveBytes = reserveBytes;
        this.reservedBytes = reserveBytes.applyAsLong(BLOCK_SIZE_IN_BYTES);
        this.releaseBytes = releaseBytes;
    }

    @Override
    public void addBytes(long bytes) {
        usedBytes += bytes;
        while (reservedBytes - usedBytes < 0) {
            reservedBytes += reserveBytes.applyAsLong(BLOCK_SIZE_IN_BYTES);
        }
    }

    @Override
    public long totalBytes() {
        return reservedBytes;
    }

    @Override
    public void release() {
        this.usedBytes = 0;
    }

    @Override
    public void close() {
        releaseBytes.accept(reservedBytes);
        this.reservedBytes = 0;
        this.usedBytes = 0;
    }
}
