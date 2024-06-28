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

package com.yookue.springstarter.snowflakeuid.buffer;


import org.apache.commons.lang3.Validate;
import lombok.Getter;
import lombok.ToString;


/**
 * Allocate 64 bits for the UID generator (long)
 * <p>
 * sign (fixed 1bit) -> deltaSecond -> workerId -> sequence(within the same second)
 *
 * @author yutianbao
 */
@ToString
@SuppressWarnings("unused")
public class BitsAllocator {
    /**
     * Total 64 bits
     */
    public static final int TOTAL_BITS = 1 << 6;
    @Getter
    private final int timestampBits;
    @Getter
    private final int workerIdBits;
    @Getter
    private final int sequenceBits;
    /**
     * Max value for workId & sequence
     */
    @Getter
    private final long maxDeltaSeconds;
    @Getter
    private final long maxWorkerId;
    @Getter
    private final long maxSequence;
    /**
     * Shift for timestamp & workerId
     */
    @Getter
    private final int timestampShift;
    @Getter
    private final int workerIdShift;
    /**
     * Bits for [sign-> second-> workId-> sequence]
     */
    @Getter
    private final int signBits = 1;

    /**
     * Constructor with timestampBits, workerIdBits, sequenceBits<br>
     * The highest bit used for sign, so <code>63</code> bits for timestampBits, workerIdBits, sequenceBits
     */
    public BitsAllocator(int timestampBits, int workerIdBits, int sequenceBits) {
        // make sure allocated 64 bits
        int allocateTotalBits = signBits + timestampBits + workerIdBits + sequenceBits;
        Validate.isTrue(allocateTotalBits <= TOTAL_BITS, "Summation of timestampBits + workerIdBits + sequenceBits, must be less than 64 bits.");

        // initialize bits
        this.timestampBits = timestampBits;
        this.workerIdBits = workerIdBits;
        this.sequenceBits = sequenceBits;

        // initialize max value
        this.maxDeltaSeconds = ~(-1L << timestampBits);
        this.maxWorkerId = ~(-1L << workerIdBits);
        this.maxSequence = ~(-1L << sequenceBits);

        // initialize shift
        this.timestampShift = workerIdBits + sequenceBits;
        this.workerIdShift = sequenceBits;
    }

    /**
     * Allocate bits for UID according to delta seconds & workerId & sequence<br>
     * <b>Note that: </b>The highest bit will always be 0 for sign
     */
    public long allocate(long deltaSeconds, long workerId, long sequence) {
        return (deltaSeconds << timestampShift) | (workerId << workerIdShift) | sequence;
    }
}