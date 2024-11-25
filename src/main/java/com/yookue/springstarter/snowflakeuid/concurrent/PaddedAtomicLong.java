/*
 * Copyright (c) 2020 Yookue Ltd. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.yookue.springstarter.snowflakeuid.concurrent;


import java.util.concurrent.atomic.AtomicLong;


/**
 * Represents a padded {@link AtomicLong} to prevent the FalseSharing problem
 * <p>
 * The CPU cache line commonly be 64 bytes, here is a sample of cache line after padding:<br>
 * 64 bytes = 8 bytes (object reference) + 6 * 8 bytes (padded long) + 8 bytes (a long value)
 * </p>
 *
 * @author yutianbao
 */
@SuppressWarnings("unused")
public class PaddedAtomicLong extends AtomicLong {
    /**
     * Padded 6 long (48 bytes)
     */
    private volatile long p1, p2, p3, p4, p5;

    /**
     * Constructors from {@link AtomicLong}
     */
    public PaddedAtomicLong() {
        super();
    }

    public PaddedAtomicLong(long initialValue) {
        super(initialValue);
    }

    /**
     * To prevent GC optimizations for cleaning unused padded references
     */
    public long sumPaddingToPreventOptimization() {
        return p1 + p2 + p3 + p4 + p5 + 7L;
    }
}
