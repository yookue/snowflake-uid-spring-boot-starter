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


import java.util.ArrayList;
import java.util.List;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.springframework.beans.factory.DisposableBean;
import com.yookue.springstarter.snowflakeuid.buffer.BufferedPaddingExecutor;
import com.yookue.springstarter.snowflakeuid.buffer.RejectedPutBufferHandler;
import com.yookue.springstarter.snowflakeuid.buffer.RejectedTakeBufferHandler;
import com.yookue.springstarter.snowflakeuid.buffer.RingBuffer;
import com.yookue.springstarter.snowflakeuid.composer.WorkerIdAssigner;
import com.yookue.springstarter.snowflakeuid.exception.UidGenerationException;
import com.yookue.springstarter.snowflakeuid.property.SnowflakeUidProperties;
import lombok.extern.slf4j.Slf4j;


/**
 * Composer implementation for a cacheable {@link com.yookue.springstarter.snowflakeuid.composer.UidGenerator}
 * <p>
 * Extends from {@link DefaultUidGenerator}, based on a lock free {@link RingBuffer}
 * <p>
 * The spring properties you can specify as below:<br>
 * <li><b>boostPower:</b> RingBuffer size boost for a power of 2, Sample: boostPower is 3, it means the buffer size
 * will be <code>(BitsAllocator#getMaxSequence() + 1) &lt;&lt; boostPower</code>
 * <li><b>paddingFactor:</b> Represents a percent value of (0 - 100). When the count of rest available UIDs reach the
 * threshold, it will trigger padding buffer. Default as{@link RingBuffer#DEFAULT_PADDING_PERCENT}
 * Sample: paddingFactor=20, bufferSize=1000 -&gt; threshold=1000 * 20 /100, padding buffer will be triggered when tail-cursor &lt; threshold
 * <li><b>scheduleInterval:</b> Padding buffer in a schedule, specify padding buffer interval, Unit as second
 * <li><b>putBufferHandler:</b> Policy for rejected put buffer. Default as discard put request, just do logging
 * <li><b>takeBufferHandler:</b> Policy for rejected take buffer. Default as throwing up an exception
 *
 * @author yutianbao
 * @author wujun
 * @author David Hsing
 */
@Slf4j
@SuppressWarnings({"unused", "JavadocReference", "WeakerAccess"})
public class CacheableUidGenerator extends DefaultUidGenerator implements DisposableBean {
    /**
     * 拒绝策略: 当环已满, 无法继续填充时
     * 默认无需指定, 将丢弃 Put 操作
     */
    private RejectedPutBufferHandler putHandler;

    /**
     * 拒绝策略: 当环已空, 无法继续获取时
     * 默认无需指定, 抛出 UidGenerationException 异常
     */
    private RejectedTakeBufferHandler takeHandler;

    private RingBuffer ringBuffer;
    private BufferedPaddingExecutor paddingExecutor;

    public CacheableUidGenerator(@Nonnull SnowflakeUidProperties properties, @Nonnull WorkerIdAssigner assigner) {
        super(properties, assigner);
    }

    public CacheableUidGenerator(@Nonnull SnowflakeUidProperties properties, @Nonnull WorkerIdAssigner assigner, @Nullable RejectedPutBufferHandler putHandler, @Nullable RejectedTakeBufferHandler takeHandler) {
        super(properties, assigner);
        this.putHandler = putHandler;
        this.takeHandler = takeHandler;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        // initialize workerId & bitsAllocator
        super.afterPropertiesSet();
        // initialize RingBuffer & RingBufferPaddingExecutor
        initRingBuffer();
    }

    @Override
    public long getUniqueId() {
        try {
            if (ringBuffer != null) {
                return ringBuffer.take();
            }
        } catch (Exception ignored) {
        }
        throw new UidGenerationException();
    }

    @Override
    public void destroy() {
        if (paddingExecutor != null) {
            paddingExecutor.shutdown();
        }
    }

    /**
     * Get the UIDs in the same specified second under the max sequence
     *
     * @return UID list, size of BitsAllocator#getMaxSequence() + 1
     */
    protected List<Long> nextIdsForOneSecond(long currentSecond) {
        // Initialize result list size of (max sequence + 1)
        int listSize = (int) super.getBitsAllocator().getMaxSequence() + 1;
        List<Long> uidList = new ArrayList<>(listSize);
        // Allocate the first sequence of the second, the others can be calculated with the offset
        long firstSeqUid = super.getBitsAllocator().allocate(currentSecond - super.getUidProperties().getEpochSeconds(), super.getWorkerId(), 0L);
        for (int offset = 0; offset < listSize; offset++) {
            uidList.add(firstSeqUid + offset);
        }
        return uidList;
    }

    /**
     * Initialize RingBuffer & RingBufferPaddingExecutor
     */
    private void initRingBuffer() {
        // initialize RingBuffer
        int bufferSize = ((int) super.getBitsAllocator().getMaxSequence() + 1) << super.getUidProperties().getBoostPower();
        ringBuffer = new RingBuffer(bufferSize, super.getUidProperties().getPaddingFactor());
        if (log.isInfoEnabled()) {
            log.info("Initialized ring buffer size '{}', paddingFactor '{}'", bufferSize, super.getUidProperties().getPaddingFactor());
        }
        // initialize RingBufferPaddingExecutor
        boolean usingSchedule = ObjectUtils.compare(super.getUidProperties().getScheduleInterval(), NumberUtils.LONG_ZERO) > 0;
        paddingExecutor = new BufferedPaddingExecutor(ringBuffer, this::nextIdsForOneSecond, usingSchedule);
        if (usingSchedule) {
            paddingExecutor.setScheduleInterval(super.getUidProperties().getScheduleInterval());
        }
        if (log.isInfoEnabled()) {
            log.info("Initialized BufferedPaddingExecutor. Using schedule '{}', interval '{}'", usingSchedule, super.getUidProperties().getScheduleInterval());
        }
        // set rejected put/take handle policy
        ringBuffer.setBufferPaddingExecutor(paddingExecutor);
        if (putHandler != null) {
            ringBuffer.setRejectedPutHandler(putHandler);
        }
        if (takeHandler != null) {
            ringBuffer.setRejectedTakeHandler(takeHandler);
        }
        // fill in all slots of the RingBuffer
        paddingExecutor.paddingBuffer();
        // start buffer padding threads
        paddingExecutor.start();
        if (log.isInfoEnabled()) {
            log.info("Initialized ring buffer successful");
        }
    }
}
