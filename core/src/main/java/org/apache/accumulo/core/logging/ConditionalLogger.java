/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.logging;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.Marker;
import org.slf4j.event.Level;
import org.slf4j.helpers.AbstractLogger;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * Logger that wraps another Logger and only emits a log message once per the supplied duration.
 *
 */
public abstract class ConditionalLogger extends AbstractLogger {

  private static final long serialVersionUID = 1L;

  public static class DeduplicatingLogger extends ConditionalLogger {

    private static final long serialVersionUID = 1L;

    public DeduplicatingLogger(Logger log, Duration threshold) {
      super(log, new BiFunction<>() {

        private final Cache<String,List<Object>> cache =
            Caffeine.newBuilder().expireAfterWrite(threshold).weakKeys().weakValues().build();

        @Override
        public Boolean apply(String msg, List<Object> args) {

          // WeakKeys will perform == check, this should work?
          List<Object> storedArgs = cache.getIfPresent(msg);

          if (storedArgs == null || !storedArgs.equals(args)) {
            cache.put(msg, args);
            return true;
          }
          return false;
        }

      });
    }

  }

  private final Logger delegate;
  private final BiFunction<String,List<Object>,Boolean> condition;

  protected ConditionalLogger(Logger log, BiFunction<String,List<Object>,Boolean> condition) {
    // this.delegate = new DelegateWrapper(log);
    this.delegate = log;
    this.condition = condition;
  }

  @Override
  public boolean isTraceEnabled() {
    return this.delegate.isTraceEnabled();
  }

  @Override
  public boolean isTraceEnabled(Marker marker) {
    return this.delegate.isTraceEnabled(marker);
  }

  @Override
  public boolean isDebugEnabled() {
    return this.delegate.isDebugEnabled();
  }

  @Override
  public boolean isDebugEnabled(Marker marker) {
    return this.delegate.isDebugEnabled(marker);
  }

  @Override
  public boolean isInfoEnabled() {
    return this.delegate.isInfoEnabled();
  }

  @Override
  public boolean isInfoEnabled(Marker marker) {
    return this.delegate.isInfoEnabled(marker);
  }

  @Override
  public boolean isWarnEnabled() {
    return this.delegate.isWarnEnabled();
  }

  @Override
  public boolean isWarnEnabled(Marker marker) {
    return this.delegate.isWarnEnabled(marker);
  }

  @Override
  public boolean isErrorEnabled() {
    return this.delegate.isErrorEnabled();
  }

  @Override
  public boolean isErrorEnabled(Marker marker) {
    return this.delegate.isErrorEnabled(marker);
  }

  @Override
  public String getName() {
    return this.delegate.getName();
  }

  @Override
  protected String getFullyQualifiedCallerName() {
    return this.delegate.getName();
  }

  @Override
  protected void handleNormalizedLoggingCall(Level level, Marker marker, String messagePattern,
      Object[] arguments, Throwable throwable) {

    if (arguments == null) {
      arguments = new Object[0];
    }
    if (condition.apply(messagePattern, Arrays.asList(arguments))) {
      delegate.atLevel(level).addMarker(marker).setCause(throwable).log(messagePattern, arguments);
    }

  }

}
