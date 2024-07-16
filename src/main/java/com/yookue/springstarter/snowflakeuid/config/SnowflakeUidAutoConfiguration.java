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

package com.yookue.springstarter.snowflakeuid.config;


import jakarta.annotation.Nonnull;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigureOrder;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Primary;
import org.springframework.core.Ordered;
import com.yookue.springstarter.snowflakeuid.buffer.RejectedPutBufferHandler;
import com.yookue.springstarter.snowflakeuid.buffer.RejectedTakeBufferHandler;
import com.yookue.springstarter.snowflakeuid.composer.UidGenerator;
import com.yookue.springstarter.snowflakeuid.composer.WorkerIdAssigner;
import com.yookue.springstarter.snowflakeuid.composer.impl.CacheableUidGenerator;
import com.yookue.springstarter.snowflakeuid.composer.impl.DefaultUidGenerator;
import com.yookue.springstarter.snowflakeuid.composer.impl.InetWorkerIdAssigner;
import com.yookue.springstarter.snowflakeuid.property.SnowflakeUidProperties;


/**
 * Configuration for snowflake UID generator
 *
 * @author David Hsing
 * @reference "https://github.com/baidu/uid-generator"
 */
@ConditionalOnProperty(prefix = SnowflakeUidAutoConfiguration.PROPERTIES_PREFIX, name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(value = SnowflakeUidProperties.class)
@AutoConfigureOrder(value = Ordered.LOWEST_PRECEDENCE - 1000)
@SuppressWarnings({"JavadocDeclaration", "JavadocLinkAsPlainText"})
public class SnowflakeUidAutoConfiguration {
    public static final String PROPERTIES_PREFIX = "spring.snowflake-uid";    // $NON-NLS-1$
    public static final String INET_WORKER_ID_ASSIGNER = "inetWorkerIdAssigner";    // $NON-NLS-1$
    public static final String DEFAULT_UID_GENERATOR = "defaultUidGenerator";    // $NON-NLS-1$
    public static final String CACHEABLE_UID_GENERATOR = "cacheableUidGenerator";    // $NON-NLS-1$

    @Bean(name = INET_WORKER_ID_ASSIGNER)
    @ConditionalOnMissingBean(name = INET_WORKER_ID_ASSIGNER)
    public WorkerIdAssigner inetWorkerIdAssigner(@Nonnull SnowflakeUidProperties properties) {
        return new InetWorkerIdAssigner(properties);
    }

    @Primary
    @Bean(name = DEFAULT_UID_GENERATOR)
    @ConditionalOnBean(name = INET_WORKER_ID_ASSIGNER)
    @ConditionalOnMissingBean(name = DEFAULT_UID_GENERATOR)
    public UidGenerator defaultUidGenerator(@Nonnull SnowflakeUidProperties properties, @Qualifier(value = INET_WORKER_ID_ASSIGNER) @Nonnull WorkerIdAssigner assigner) {
        return new DefaultUidGenerator(properties, assigner);
    }

    @Bean(name = CACHEABLE_UID_GENERATOR)
    @ConditionalOnBean(name = INET_WORKER_ID_ASSIGNER)
    @ConditionalOnMissingBean(name = CACHEABLE_UID_GENERATOR)
    @Lazy
    public UidGenerator cacheableUidGenerator(@Nonnull SnowflakeUidProperties properties, @Qualifier(value = INET_WORKER_ID_ASSIGNER) @Nonnull WorkerIdAssigner assigner, @Nonnull ObjectProvider<RejectedPutBufferHandler> putHandlers, @Nonnull ObjectProvider<RejectedTakeBufferHandler> takeHandlers) {
        return new CacheableUidGenerator(properties, assigner, putHandlers.getIfAvailable(), takeHandlers.getIfAvailable());
    }
}
