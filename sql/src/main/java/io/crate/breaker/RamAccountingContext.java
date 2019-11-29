/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.breaker;

import io.crate.exceptions.Exceptions;
import io.crate.execution.dsl.phases.ExecutionPhase;
import org.apache.datasketches.memory.WritableDirectHandle;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

@ThreadSafe
public class RamAccountingContext implements RamAccounting {

    // this must not be final so tests could adjust it
    // Flush every 2mb
    public static long FLUSH_BUFFER_SIZE = 1024 * 1024 * 2;

    private final String contextId;
    private final CircuitBreaker breaker;

    private final AtomicLong totalBytes = new AtomicLong(0);
    private final AtomicLong flushBuffer = new AtomicLong(0);
    private volatile boolean closed = false;
    private volatile boolean tripped = false;

    private final ArrayList<AutoCloseable> resourcesToRelease = new ArrayList<>();

    private static final Logger LOGGER = LogManager.getLogger(RamAccountingContext.class);

    public static RamAccountingContext forExecutionPhase(CircuitBreaker breaker, ExecutionPhase executionPhase) {
        String ramAccountingContextId = executionPhase.name() + ": " + executionPhase.phaseId();
        return new RamAccountingContext(ramAccountingContextId, breaker);
    }

    public RamAccountingContext(String contextId, CircuitBreaker breaker) {
        this.contextId = contextId;
        this.breaker = breaker;
    }

    /**
     * Add bytes to the context and maybe break
     *
     * @param bytes bytes to be added
     * @throws CircuitBreakingException in case the breaker tripped
     */
    @Override
    public void addBytes(long bytes) throws CircuitBreakingException {
        addBytes(bytes, true);
    }

    public WritableMemory allocateDirect(long capacityBytes) {
        WritableDirectHandle handle = WritableMemory.allocateDirect(capacityBytes);
        synchronized (resourcesToRelease) {
            resourcesToRelease.add(handle);
        }
        return handle.get();
    }

    /**
     * Add bytes to the context without breaking
     *
     * @param bytes bytes to be added
     */
    public void addBytesWithoutBreaking(long bytes) {
        addBytes(bytes, false);
    }

    private void addBytes(long bytes, boolean shouldBreak) throws CircuitBreakingException {
        if (closed || bytes == 0) {
            return;
        }
        long currentFlushBuffer = flushBuffer.addAndGet(bytes);
        if (currentFlushBuffer >= FLUSH_BUFFER_SIZE) {
            if (shouldBreak) {
                flush(currentFlushBuffer);
            } else {
                flushWithoutBreaking(currentFlushBuffer);
            }
        }
    }

    /**
     * Flush the {@code bytes} to the breaker, incrementing the total
     * bytes and adjusting the buffer.
     *
     * @param bytes long value of bytes to be flushed to the breaker
     * @throws CircuitBreakingException in case the breaker tripped
     */
    private void flush(long bytes) throws CircuitBreakingException {
        if (bytes == 0) {
            return;
        }
        try {
            breaker.addEstimateBytesAndMaybeBreak(bytes, contextId);
        } catch (CircuitBreakingException e) {
            // since we've already created the data, we need to
            // add it so closing the context re-adjusts properly
            breaker.addWithoutBreaking(bytes);
            tripped = true;
            // re-throw the original exception
            throw e;
        } finally {
            totalBytes.addAndGet(bytes);
            flushBuffer.addAndGet(-bytes);
        }
    }

    /**
     * Flush the {@code bytes} to the breaker, incrementing the total
     * bytes and adjusting the buffer.
     *
     * @param bytes long value of bytes to be flushed to the breaker
     */
    private void flushWithoutBreaking(long bytes) {
        if (bytes == 0) {
            return;
        }
        breaker.addWithoutBreaking(bytes);
        if (exceededBreaker()) {
            tripped = true;
        }
        totalBytes.addAndGet(bytes);
        flushBuffer.addAndGet(-bytes);
    }

    /**
     * Returns bytes from the buffer + bytes that have already been flushed to the breaker.
     * @return the total number of bytes that have been aggregated
     */
    public long totalBytes() {
        return flushBuffer.get() + totalBytes.get();
    }

    /**
     * Close the context and adjust the breaker.
     * A remaining flush buffer will not be flushed to avoid breaking on close.
     * (all ram operations expected to be finished at this point)
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        ArrayList<Exception> exceptions = new ArrayList<>();
        for (AutoCloseable autoCloseable : resourcesToRelease) {
            try {
                autoCloseable.close();
            } catch (Exception e) {
                exceptions.add(e);
            }
        }
        if (totalBytes.get() != 0) {
            if (LOGGER.isTraceEnabled() && totalBytes() > FLUSH_BUFFER_SIZE) {
                LOGGER.trace("context: {} bytes; breaker: {} of {} bytes", totalBytes(), breaker.getUsed(), breaker.getLimit());
            }
            breaker.addWithoutBreaking(-totalBytes.get());
        }
        totalBytes.addAndGet(flushBuffer.getAndSet(0));
        if (!exceptions.isEmpty()) {
            Exceptions.rethrowRuntimeException(exceptions.get(0));
        }
    }

    /**
     * Release all the bytes that have been flushed to the breaker so far, and the bytes that are in the buffer "to be
     * flushed" are not accounted for in the breaker anymore.
     * <p>
     * The purpose of this method is to substract everything that this context added to the breaker so far in order
     * to be reused in a multi-phase operation where a subsequent phase needs to make decisions based on the available
     * memory after the previous phase completed (and needs to be unloaded/released from the breaker)
     */
    @Override
    public void release() {
        if (totalBytes.get() != 0) {
            if (LOGGER.isTraceEnabled() && totalBytes() > FLUSH_BUFFER_SIZE) {
                LOGGER.trace("context: {} bytes; breaker: {} of {} bytes", totalBytes(), breaker.getUsed(), breaker.getLimit());
            }
            breaker.addWithoutBreaking(-totalBytes.getAndSet(0));
        }
        flushBuffer.getAndSet(0);
    }

    /**
     * Returns true if the limit of the breaker was already reached
     */
    public boolean trippedBreaker() {
        return tripped;
    }

    /**
     * Returns true if the limit of the breaker was already reached
     * but the breaker did not trip (e.g. when adding bytes without breaking)
     */
    public boolean exceededBreaker() {
        return breaker.getUsed() >= breaker.getLimit();
    }

    /**
     * Returns the configured bytes limit of the breaker
     */
    public long limit() {
        return breaker.getLimit();
    }

    /**
     * Returns the context id string.
     */
    public String contextId() {
        return contextId;
    }


    /**
     * round n up to the nearest multiple of m
     */
    public static long roundUp(long n, long m) {
        return n + (n % m);
    }

    /**
     * round n up to the nearest multiple of 8
     */
    public static long roundUp(long n) {
        return roundUp(n, 8);
    }
}
