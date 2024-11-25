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

package com.yookue.springstarter.snowflakeuid.composer.impl;


import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.BooleanUtils;
import org.springframework.beans.factory.InitializingBean;
import com.yookue.springstarter.snowflakeuid.buffer.BitsAllocator;
import com.yookue.springstarter.snowflakeuid.composer.UidGenerator;
import com.yookue.springstarter.snowflakeuid.composer.WorkerIdAssigner;
import com.yookue.springstarter.snowflakeuid.exception.UidGenerationException;
import com.yookue.springstarter.snowflakeuid.property.SnowflakeUidProperties;
import com.yookue.springstarter.snowflakeuid.structure.UidGeneratorStruct;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


/**
 * Composer implementation for default {@link com.yookue.springstarter.snowflakeuid.composer.UidGenerator}
 * <p>
 * The unique id has 64bits (long), default allocated as blow:<br>
 * <li>sign: The highest bit is 0
 * <li>delta seconds: The next 28 bits, represents delta seconds since a customer epoch(2016-05-20 00:00:00.000).
 * Supports about 8.7 years until to 2024-11-20 21:24:16
 * <li>worker id: The next 22 bits, represents the worker's id which assigns based on database, max id is about 420W
 * <li>sequence: The next 13 bits, represents a sequence within the same second, max for 8192/s<br><br>
 * <p>
 * The {@link DefaultUidGenerator#parseUniqueId(long)} is a tool method to parse the bits
 * <p>
 * <pre>{@code
 * +------+----------------------+----------------+-----------+
 * | sign |     delta seconds    | worker node id | sequence  |
 * +------+----------------------+----------------+-----------+
 *   1bit          28bits              22bits         13bits
 * }</pre>
 * <p>
 * You can also specify the bits by Spring property setting.
 * <li>timeBits: default as 28
 * <li>workerBits: default as 22
 * <li>seqBits: default as 13
 * <li>epochStr: Epoch date string format 'yyyy-MM-dd'. Default as '2016-05-20'
 * </p>
 * <p>
 * <b>Note that:</b> The total bits must be 64 -1
 * </p>
 *
 * @author yutianbao
 * @author wujun
 */
@RequiredArgsConstructor
@Getter(value = AccessLevel.PROTECTED)
@Slf4j
@SuppressWarnings({"unused", "WeakerAccess", "LoggingSimilarMessage"})
public class DefaultUidGenerator implements UidGenerator, InitializingBean {
    private final SnowflakeUidProperties uidProperties;
    private final WorkerIdAssigner idAssigner;

    /**
     * Bit分配器,Stable fields after spring bean initializing
     */
    private BitsAllocator bitsAllocator;
    private long workerId;

    /**
     * Volatile fields caused by nextId()
     */
    private long sequence = 0L;
    private long lastSecond = -1L;

    @Override
    public void afterPropertiesSet() throws Exception {
        // initialize worker id
        workerId = idAssigner.generateWorkerId();
        // initialize bits allocator
        bitsAllocator = new BitsAllocator(uidProperties.getTimeBits(), uidProperties.getWorkerBits(), uidProperties.getSeqBits());
        if (workerId > bitsAllocator.getMaxWorkerId()) {
            if (log.isWarnEnabled()) {
                log.warn("WorkerId '{}' exceeds max workerId '{}'", workerId, bitsAllocator.getMaxWorkerId());
            }
            workerId = workerId % bitsAllocator.getMaxWorkerId();
            if (log.isDebugEnabled()) {
                log.debug("Reset new workerId to '{}'", workerId);
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("Initialized bits (1, {}, {}, {}) for workerId '{}'", uidProperties.getTimeBits(), uidProperties.getWorkerBits(), uidProperties.getSeqBits(), workerId);
        }
    }

    @Override
    public long getUniqueId() throws UidGenerationException {
        try {
            return nextUniqueId();
        } catch (Exception ignored) {
        }
        throw new UidGenerationException();
    }

    @Override
    public UidGeneratorStruct parseUniqueId(long uniqueId) {
        if (uniqueId > 0L) {
            long totalBits = BitsAllocator.TOTAL_BITS;
            long workerIdBits = bitsAllocator.getWorkerIdBits(), sequenceBits = bitsAllocator.getSequenceBits();
            // parse UID
            long workerId = (uniqueId << (totalBits - workerIdBits - sequenceBits)) >>> (totalBits - workerIdBits);
            long sequence = (uniqueId << (totalBits - sequenceBits)) >>> (totalBits - sequenceBits);
            long deltaSeconds = uniqueId >>> (workerIdBits + sequenceBits);
            Date thatTime = new Date(TimeUnit.SECONDS.toMillis(uidProperties.getEpochSeconds() + deltaSeconds));
            return new UidGeneratorStruct(uniqueId, workerId, sequence, thatTime);
        }
        return null;
    }

    /**
     * Get UID
     *
     * @throws UidGenerationException in the case: Clock moved backwards; Exceeds the max timestamp
     */
    protected synchronized long nextUniqueId() throws Exception {
        long currentSecond = getCurrentSecond();
        // Clock moved backwards, refuse to generate uid
        if (currentSecond < lastSecond) {
            long refusedSeconds = lastSecond - currentSecond;
            if (BooleanUtils.isTrue(uidProperties.getBackwardEnabled())) {
                if (refusedSeconds <= uidProperties.getMaxBackwardSeconds()) {
                    log.error("Clock moved backwards. waiting for {} seconds", refusedSeconds);
                    while (currentSecond < lastSecond) {
                        currentSecond = getCurrentSecond();
                    }
                } else {
                    workerId = idAssigner.generateWorkerId();
                    if (log.isWarnEnabled()) {
                        log.warn("Clock moved backwards. Assigned new workerId '{}'", workerId);
                    }
                    if (workerId > bitsAllocator.getMaxWorkerId()) {
                        if (log.isWarnEnabled()) {
                            log.warn("WorkerId '{}' exceeds max workerId '{}'", workerId, bitsAllocator.getMaxWorkerId());
                        }
                        workerId = workerId % bitsAllocator.getMaxWorkerId();
                        if (log.isDebugEnabled()) {
                            log.debug("Reset new workerId to '{}'", workerId);
                        }
                    }
                }
            } else {
                throw new UidGenerationException("Clock moved backwards. Refusing for {} seconds", refusedSeconds);
            }
        }
        // At the same second, increase sequence
        if (currentSecond == lastSecond) {
            sequence = (sequence + 1) & bitsAllocator.getMaxSequence();
            // Exceed the max sequence, we wait the next second to generate uid
            if (sequence == 0) {
                currentSecond = getNextSecond(lastSecond);
            }
            // At the different second, sequence restart from zero
        } else {
            sequence = 0L;
        }
        lastSecond = currentSecond;
        // Allocate bits for UID
        return bitsAllocator.allocate(currentSecond - uidProperties.getEpochSeconds(), workerId, sequence);
    }

    /**
     * Get next second
     */
    private long getNextSecond(long lastTimestamp) {
        long timestamp = getCurrentSecond();
        while (timestamp <= lastTimestamp) {
            timestamp = getCurrentSecond();
        }
        return timestamp;
    }

    /**
     * Get current second
     */
    private long getCurrentSecond() {
        long currentSecond = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
        if (currentSecond - uidProperties.getEpochSeconds() > bitsAllocator.getMaxDeltaSeconds()) {
            throw new UidGenerationException("Timestamp bits is exhausted. Refusing UID generate. Now is {}", currentSecond);
        }
        return currentSecond;
    }
}
