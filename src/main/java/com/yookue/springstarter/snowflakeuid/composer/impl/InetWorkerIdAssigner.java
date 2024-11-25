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


import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import com.yookue.commonplexus.javaseutil.util.InetAddressWraps;
import com.yookue.commonplexus.springutil.util.ApplicationContextWraps;
import com.yookue.springstarter.snowflakeuid.composer.WorkerIdAssigner;
import com.yookue.springstarter.snowflakeuid.exception.UidGenerationException;
import com.yookue.springstarter.snowflakeuid.property.SnowflakeUidProperties;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;


/**
 * Composer implementation for {@link com.yookue.springstarter.snowflakeuid.composer.WorkerIdAssigner} by INet
 *
 * @author David Hsing
 */
@RequiredArgsConstructor
@Slf4j
public class InetWorkerIdAssigner implements WorkerIdAssigner, ApplicationContextAware, InitializingBean {
    private final SnowflakeUidProperties uidProperties;
    private Long inet4Port;

    @Setter
    protected ApplicationContext applicationContext;

    @Override
    public void afterPropertiesSet() throws Exception {
        inet4Port = InetAddressWraps.toLongByInet4Port(InetAddressWraps.getLocalIpAddress(), ApplicationContextWraps.getLocalServerPort(applicationContext));
    }

    public long generateWorkerId() throws UidGenerationException {
        if (inet4Port == null) {
            return 0L;
        }
        int shift = 64 - uidProperties.getWorkerBits();
        long workerId = (inet4Port << shift) >>> shift;
        if (log.isDebugEnabled()) {
            log.debug("Generated worker id = '{}'", workerId);
        }
        return workerId;
    }
}
