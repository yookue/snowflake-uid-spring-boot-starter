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


import javax.annotation.Nonnull;


/**
 * Represents a put buffer handler for {@link com.yookue.springstarter.snowflakeuid.buffer.RingBuffer}
 * <p>
 * If tail catches the cursor it means that the ring buffer is full, any more buffers put request will be rejected
 * <p>
 * Specify the policy to handle the reject
 *
 * @author yutianbao
 */
@FunctionalInterface
@SuppressWarnings("unused")
public interface RejectedPutBufferHandler {
    /**
     * Reject put buffer request
     */
    void rejectPutBuffer(@Nonnull RingBuffer ringBuffer, long uid);
}