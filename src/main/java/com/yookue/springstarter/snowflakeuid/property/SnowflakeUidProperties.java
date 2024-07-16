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

package com.yookue.springstarter.snowflakeuid.property;


import java.io.Serializable;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.Assert;
import com.yookue.commonplexus.javaseutil.util.UtilDateWraps;
import com.yookue.springstarter.snowflakeuid.config.SnowflakeUidAutoConfiguration;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;


/**
 * Properties of snowflake UID generator
 *
 * @author wujun
 * @author David Hsing
 */
@ConfigurationProperties(prefix = SnowflakeUidAutoConfiguration.PROPERTIES_PREFIX)
@Getter
@ToString
public class SnowflakeUidProperties implements Serializable, InitializingBean {
    /**
     * Enable this starter, default is {@code true}
     */
    @Setter
    private Boolean enabled;

    /**
     * Time increment value occupancy digits<br/>
     * The incremental value of the current time relative to the time basis point, in seconds
     */
    @Setter
    private Integer timeBits = 33;

    /**
     * Number of digits occupied by the working machine ID
     */
    @Setter
    private Integer workerBits = 20;

    /**
     * Number of digits occupied by the serial number
     */
    @Setter
    private Integer seqBits = 10;

    /**
     * The epoch base point date
     */
    @Setter
    private String epochPoint = "2024-01-01";    // $NON-NLS-1$

    /**
     * Number of milliseconds corresponding to the epoch base point date
     * <p>
     * For example, 2019-02-20 represents 1550592000000
     */
    private Long epochSeconds = 0L;

    /**
     * Indicates whether allow clock go backward or not
     */
    @Setter
    private Boolean backwardEnabled = true;

    /**
     * The max seconds of clock going backward
     */
    @Setter
    private Long maxBackwardSeconds = 1L;

    /**
     * Capacity expansion parameter for RingBuffer size<br/>
     * Turns this bigger could increase the throughput for UID generation<br/>
     * For {@link com.yookue.springstarter.snowflakeuid.composer.impl.CacheableUidGenerator} only
     * <p>
     * For example, defaults 3, represents
     * <pre><code>
     *     bufferSize = 8192    =&gt;    bufferSize = 8192 &lt;&lt; 3 = 65536
     * </code></pre>
     */
    @Setter
    private Integer boostPower = 3;

    /**
     * Specifies when to fill the UID into the RingBuffer, in percentage (0, 100)<br/>
     * The RingBuffer is automatically padded when the remaining of UIDs available on the loop is less than 512<br/>
     * For {@link com.yookue.springstarter.snowflakeuid.composer.impl.CacheableUidGenerator} only
     * <p>
     * For example
     * <pre><code>
     *     bufferSize = 1024, paddingFactor = 50    =&gt;    threshold = 1024 * 50 / 100 = 512
     * </code></pre>
     */
    @Setter
    private Integer paddingFactor = 50;

    /**
     * Another RingBuffer fill timing, in the Schedule thread, periodically checks the filling, in seconds<br/>
     * Defaults null, means not using the Schedule thread<br/>
     * For {@link com.yookue.springstarter.snowflakeuid.composer.impl.CacheableUidGenerator} only
     */
    @Setter
    private Long scheduleInterval;

    @Override
    public void afterPropertiesSet() {
        Assert.isTrue(timeBits + workerBits + seqBits < 64, "Summation of timeBits + workerBits + seqBits, must be less than 64 bits.");
        Date epochDate = UtilDateWraps.parseDateGuessing(epochPoint);
        if (epochDate != null) {
            epochSeconds = TimeUnit.MILLISECONDS.toSeconds(epochDate.getTime());
        }
    }
}
