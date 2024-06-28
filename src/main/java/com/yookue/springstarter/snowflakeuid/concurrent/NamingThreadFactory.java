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


import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;


/**
 * Named thread in ThreadFactory
 * <p>
 * If there is no specified name for thread, it will auto-detect using the invoker classname instead
 *
 * @author yutianbao
 */
@Slf4j
@SuppressWarnings({"unused", "WeakerAccess"})
public class NamingThreadFactory implements ThreadFactory {
    /**
     * Sequences for multi thread name prefix
     */
    private final ConcurrentHashMap<String, AtomicLong> sequences;

    /**
     * Thread name pre
     */
    @Getter
    @Setter
    private String name;

    /**
     * Is daemon thread
     */
    @Getter
    @Setter
    private boolean daemon;

    /**
     * UncaughtExceptionHandler
     */
    @Getter
    @Setter
    private UncaughtExceptionHandler uncaughtExceptionHandler;

    /**
     * Constructors
     */
    public NamingThreadFactory() {
        this(null, false, null);
    }

    public NamingThreadFactory(@Nullable String name) {
        this(name, false, null);
    }

    public NamingThreadFactory(@Nullable String name, boolean daemon) {
        this(name, daemon, null);
    }

    public NamingThreadFactory(@Nullable String name, boolean daemon, @Nullable UncaughtExceptionHandler handler) {
        this.name = name;
        this.daemon = daemon;
        this.uncaughtExceptionHandler = handler;
        this.sequences = new ConcurrentHashMap<>();
    }

    @Override
    public Thread newThread(@Nonnull Runnable runnable) {
        Thread thread = new Thread(runnable);
        thread.setDaemon(this.daemon);
        // If there is no specified name for thread, it will auto detect using the invoker classname instead.
        // Notice that auto detect may cause some performance overhead
        String prefix = StringUtils.defaultIfBlank(this.name, getInvoker());
        thread.setName(prefix + "-" + getSequence(prefix));    // $NON-NLS-1$
        // no specified uncaughtExceptionHandler, just do logging.
        if (this.uncaughtExceptionHandler != null) {
            thread.setUncaughtExceptionHandler(this.uncaughtExceptionHandler);
        } else {
            thread.setUncaughtExceptionHandler((t, ex) -> log.error("unhandled exception in thread: {}-{}", t.getId(), t.getName(), ex));
        }
        return thread;
    }

    /**
     * Get the method invoker's class name
     */
    private String getInvoker() {
        Exception ex = new Exception();
        StackTraceElement[] elements = ex.getStackTrace();
        if (ArrayUtils.getLength(elements) > 2) {
            return ClassUtils.getShortClassName(elements[2].getClassName());
        }
        return getClass().getSimpleName();
    }

    /**
     * Get sequence for different naming prefix
     */
    private long getSequence(@Nonnull String invoker) {
        if (StringUtils.isNotBlank(invoker)) {
            AtomicLong atomicLong = this.sequences.get(invoker);
            if (atomicLong == null) {
                atomicLong = new AtomicLong(0);
                AtomicLong previous = this.sequences.putIfAbsent(invoker, atomicLong);
                if (previous != null) {
                    atomicLong = previous;
                }
            }
            return atomicLong.incrementAndGet();
        }
        return -1L;
    }
}
