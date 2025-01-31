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


import java.util.concurrent.atomic.AtomicLong;
import jakarta.annotation.Nonnull;
import org.springframework.util.Assert;
import com.yookue.springstarter.snowflakeuid.concurrent.PaddedAtomicLong;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;


/**
 * Represents a ring buffer based on array
 * <p>
 * Using array could improve read element performance due to the CUP cache line. To prevent
 * the side effect of False Sharing, {@link PaddedAtomicLong} is using on 'tail' and 'cursor'
 * <p>
 * A ring buffer is consisted of:
 * <li><b>slots:</b> each element of the array is a slot, which is set with a UID
 * <li><b>flags:</b> flag array corresponding the same index with the slots, indicates whether it can take or put slot
 * <li><b>tail:</b> a sequence of the max slot position to produce
 * <li><b>cursor:</b> a sequence of the min slot position to consume
 * </p>
 *
 * @author yutianbao
 */
@Slf4j
@ToString(onlyExplicitlyIncluded = true)
@SuppressWarnings({"unused", "WeakerAccess"})
public class RingBuffer {
    private static final int DEFAULT_PADDING_PERCENT = 50;

    /**
     * Constants
     */
    private static final int START_POINT = -1;
    private static final long CAN_PUT_FLAG = 0L;
    private static final long CAN_TAKE_FLAG = 1L;

    /**
     * The size of RingBuffer's slots, each slot hold a UID
     */
    @Getter
    @ToString.Include
    private final int bufferSize;

    private final long indexMask;
    private final long[] slots;
    private final PaddedAtomicLong[] flags;

    /**
     * Tail: last position sequence to produce
     */
    @Getter
    @ToString.Include
    private final AtomicLong tail = new PaddedAtomicLong(START_POINT);

    /**
     * Cursor: current position sequence to consume
     */
    @Getter
    @ToString.Include
    private final AtomicLong cursor = new PaddedAtomicLong(START_POINT);

    /**
     * Threshold for trigger padding buffer
     */
    @ToString.Include
    private final int paddingThreshold;

    /**
     * Reject put/take buffer handle policy
     */
    @Setter
    private RejectedPutBufferHandler rejectedPutHandler = this::rejectedPutBuffer;

    @Setter
    private RejectedTakeBufferHandler rejectedTakeHandler = this::rejectedTakeBuffer;

    /**
     * Executor of padding buffer
     */
    @Setter
    private BufferedPaddingExecutor bufferPaddingExecutor;

    /**
     * Constructor with buffer size
     *
     * @param bufferSize must be positive and a power of 2
     */
    public RingBuffer(int bufferSize) {
        this(bufferSize, DEFAULT_PADDING_PERCENT);
    }

    /**
     * Constructor with buffer size and padding factor
     *
     * @param bufferSize must be positive and a power of 2
     * @param paddingFactor percent in (0 - 100). When the count of rest available UIDs reach the threshold, it will trigger padding buffer<br>
     * Sample: paddingFactor=20, bufferSize=1000 -&gt; threshold=1000 * 20 /100,
     * padding buffer will be triggered when tail-cursor &lt; threshold
     */
    public RingBuffer(int bufferSize, int paddingFactor) {
        // check buffer size is positive & a power of 2; padding factor in (0, 100)
        Assert.isTrue(bufferSize > 0L, "RingBuffer size must be positive");
        Assert.isTrue(Integer.bitCount(bufferSize) == 1, "RingBuffer size must be a power of 2");
        Assert.isTrue(paddingFactor > 0 && paddingFactor < 100, "RingBuffer size must be positive");

        this.bufferSize = bufferSize;
        this.indexMask = bufferSize - 1;
        this.slots = new long[bufferSize];
        this.flags = initFlags(bufferSize);

        this.paddingThreshold = bufferSize * paddingFactor / 100;
    }

    /**
     * Put an UID in the ring and tail moved
     * <br>
     * We use 'synchronized' to guarantee the UID fill in slot and publish new tail sequence as atomic operations
     * <br>
     *
     * <b>Note that: </b> It is recommended to put UID in a serialize way, we once batch generate a series UIDs and put
     * the one by one into the buffer, so it is unnecessary put in multi-threads
     *
     * @return false means that the buffer is full, apply {@link RejectedPutBufferHandler}
     */
    public synchronized boolean put(long uid) {
        Assert.isTrue(uid > 0L, "UID must be positive");

        long currentTail = tail.get();
        long currentCursor = cursor.get();

        // tail catches the cursor, means that you can't put any cause of RingBuffer is full
        long distance = currentTail - (currentCursor == START_POINT ? 0 : currentCursor);
        if (distance == bufferSize - 1) {
            rejectedPutHandler.rejectPutBuffer(this, uid);
            return false;
        }

        // 1. pre-check whether the flag is CAN_PUT_FLAG
        int nextTailIndex = calSlotIndex(currentTail + 1);
        if (flags[nextTailIndex].get() != CAN_PUT_FLAG) {
            rejectedPutHandler.rejectPutBuffer(this, uid);
            return false;
        }

        // 2. put UID in the next slot
        // 3. update next slot's flag to CAN_TAKE_FLAG
        // 4. publish tail with sequence increase by one
        slots[nextTailIndex] = uid;
        flags[nextTailIndex].set(CAN_TAKE_FLAG);
        tail.incrementAndGet();

        // The atomicity of operations above, guarantees by 'synchronized'. In another word,
        // the take operation can't consume the UID we just put, until the tail is published(tail.incrementAndGet())
        return true;
    }

    /**
     * Take an UID of the ring at the next cursor, this is a lock free operation by using atomic cursor
     * <p>
     * Before getting the UID, we also check whether reach the padding threshold,
     * the padding buffer operation will be triggered in another thread<br>
     * If there is no more available UID to be taken, the specified {@link RejectedTakeBufferHandler} will be applied
     * </p>
     *
     * @return UID
     *
     * @throws IllegalStateException if the cursor moved back
     */
    public long take() {
        // spin get next available cursor
        long currentCursor = cursor.get();
        long nextCursor = cursor.updateAndGet(old -> old == tail.get() ? old : old + 1);

        // check for safety consideration, it never occurs
        Assert.isTrue(nextCursor >= currentCursor, "Cursor can't move back.");

        // trigger padding in an async-mode if reach the threshold
        long currentTail = tail.get();
        if (currentTail - nextCursor < paddingThreshold) {
            if (log.isDebugEnabled()) {
                log.debug("Reach the padding threshold '{}'. tail '{}', cursor '{}', rest '{}'.", paddingThreshold, currentTail, nextCursor, currentTail - nextCursor);
            }
            bufferPaddingExecutor.asyncPadding();
        }

        // cursor catch the tail, means that there is no more available UID to take
        if (nextCursor == currentCursor) {
            rejectedTakeHandler.rejectTakeBuffer(this);
        }

        // 1. check next slot flag is CAN_TAKE_FLAG
        int nextCursorIndex = calSlotIndex(nextCursor);
        Assert.isTrue(flags[nextCursorIndex].get() == CAN_TAKE_FLAG, "Cursor not in take-able status.");

        // 2. get UID from next slot
        // 3. set next slot flag as CAN_PUT_FLAG.
        long uid = slots[nextCursorIndex];
        flags[nextCursorIndex].set(CAN_PUT_FLAG);

        // Note that: Step 2,3 can not swap. If we set flag before get value of slot, the producer may overwrite the
        // slot with a new UID, and this may cause the consumer take the UID twice after walk a round the ring
        return uid;
    }

    /**
     * Calculate slot index with the slot sequence (sequence % bufferSize)
     */
    protected int calSlotIndex(long sequence) {
        return (int) (sequence & indexMask);
    }

    /**
     * Discard policy for {@link RejectedPutBufferHandler}, we just do logging
     */
    protected void rejectedPutBuffer(RingBuffer ringBuffer, long uid) {
        if (log.isWarnEnabled()) {
            log.warn("Rejected putting ringBuffer '{}' to UID '{}'.", ringBuffer, uid);
        }
    }

    /**
     * Policy for {@link RejectedTakeBufferHandler}, throws {@link RuntimeException} after logging
     */
    protected void rejectedTakeBuffer(@Nonnull RingBuffer ringBuffer) {
        if (log.isWarnEnabled()) {
            log.warn("Rejected taking ringBuffer '{}'.", ringBuffer);
        }
        throw new RuntimeException(String.format("Rejected taking ringBuffer. '%s'.", ringBuffer));
    }

    /**
     * Initialize flags as CAN_PUT_FLAG
     */
    private PaddedAtomicLong[] initFlags(int bufferSize) {
        Assert.isTrue(bufferSize > 0L, "Buffer size must be positive");

        if (bufferSize > 0) {
            PaddedAtomicLong[] atomicLongs = new PaddedAtomicLong[bufferSize];
            for (int i = 0; i < bufferSize; i++) {
                atomicLongs[i] = new PaddedAtomicLong(CAN_PUT_FLAG);
            }
            return atomicLongs;
        }
        return null;
    }
}
