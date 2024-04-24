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
import org.slf4j.spi.LoggingEventBuilder;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * Logger that wraps another Logger and only emits a log message once per the supplied duration.
 *
 */
public class ConditionalLogger implements Logger {

  public static class DeduplicatingLogger extends ConditionalLogger {

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

  /*
   * ConditionalLogger cannot extend AbstractLogger because it implements Serializable and the
   * Supplier member is not Serializable. The supplied Logger is wrapped by the DelegateWrapper, and
   * the ConditionalLogger delegates to the DelegateWrapper. The DelegateWrapper evaluates the
   * condition to determine whether or not logging should be performed.
   */

  private class DelegateWrapper extends AbstractLogger {

    private static final long serialVersionUID = 1L;

    private final Logger delegate;

    public DelegateWrapper(Logger delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean isTraceEnabled() {
      return delegate.isTraceEnabled();
    }

    @Override
    public boolean isTraceEnabled(Marker marker) {
      return delegate.isTraceEnabled(marker);
    }

    @Override
    public boolean isDebugEnabled() {
      return delegate.isDebugEnabled();
    }

    @Override
    public boolean isDebugEnabled(Marker marker) {
      return delegate.isDebugEnabled(marker);
    }

    @Override
    public boolean isInfoEnabled() {
      return delegate.isInfoEnabled();
    }

    @Override
    public boolean isInfoEnabled(Marker marker) {
      return delegate.isInfoEnabled(marker);
    }

    @Override
    public boolean isWarnEnabled() {
      return delegate.isWarnEnabled();
    }

    @Override
    public boolean isWarnEnabled(Marker marker) {
      return delegate.isWarnEnabled(marker);
    }

    @Override
    public boolean isErrorEnabled() {
      return delegate.isErrorEnabled();
    }

    @Override
    public boolean isErrorEnabled(Marker marker) {
      return delegate.isErrorEnabled(marker);
    }

    @Override
    protected String getFullyQualifiedCallerName() {
      return delegate.getName();
    }

    @Override
    protected void handleNormalizedLoggingCall(Level level, Marker marker, String messagePattern,
        Object[] arguments, Throwable throwable) {
      if (condition.apply(messagePattern, Arrays.asList(arguments))) {
        delegate.atLevel(level).addMarker(marker).setCause(throwable).log(messagePattern,
            arguments);
      }
    }

  }

  private final DelegateWrapper delegate;
  private final BiFunction<String,List<Object>,Boolean> condition;

  public ConditionalLogger(Logger log, BiFunction<String,List<Object>,Boolean> condition) {
    this.delegate = new DelegateWrapper(log);
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
  public LoggingEventBuilder makeLoggingEventBuilder(Level level) {
    return this.delegate.makeLoggingEventBuilder(level);
  }

  @Override
  public LoggingEventBuilder atLevel(Level level) {
    return this.delegate.atLevel(level);
  }

  @Override
  public boolean isEnabledForLevel(Level level) {
    return this.delegate.isEnabledForLevel(level);
  }

  @Override
  public void trace(String msg) {
    this.delegate.trace(msg);
  }

  @Override
  public void trace(String format, Object arg) {
    this.delegate.trace(format, arg);
  }

  @Override
  public void trace(String format, Object arg1, Object arg2) {
    this.delegate.trace(format, arg1, arg2);
  }

  @Override
  public void trace(String format, Object... arguments) {
    this.delegate.trace(format, arguments);
  }

  @Override
  public void trace(String msg, Throwable t) {
    this.delegate.trace(msg, t);
  }

  @Override
  public LoggingEventBuilder atTrace() {
    return this.delegate.atTrace();
  }

  @Override
  public void trace(Marker marker, String msg) {
    this.delegate.trace(marker, msg);
  }

  @Override
  public void trace(Marker marker, String format, Object arg) {
    this.delegate.trace(marker, format, arg);
  }

  @Override
  public void trace(Marker marker, String format, Object arg1, Object arg2) {
    this.delegate.trace(marker, format, arg1, arg2);
  }

  @Override
  public void trace(Marker marker, String format, Object... argArray) {
    this.delegate.trace(marker, format, argArray);
  }

  @Override
  public void trace(Marker marker, String msg, Throwable t) {
    this.delegate.trace(marker, msg, t);
  }

  @Override
  public void debug(String msg) {
    this.delegate.debug(msg);
  }

  @Override
  public void debug(String format, Object arg) {
    this.delegate.debug(format, arg);
  }

  @Override
  public void debug(String format, Object arg1, Object arg2) {
    this.delegate.debug(format, arg1, arg2);
  }

  @Override
  public void debug(String format, Object... arguments) {
    this.delegate.debug(format, arguments);
  }

  @Override
  public void debug(String msg, Throwable t) {
    this.delegate.debug(msg, t);
  }

  @Override
  public void debug(Marker marker, String msg) {
    this.delegate.debug(marker, msg);
  }

  @Override
  public void debug(Marker marker, String format, Object arg) {
    this.delegate.debug(marker, format, arg);
  }

  @Override
  public void debug(Marker marker, String format, Object arg1, Object arg2) {
    this.delegate.debug(marker, format, arg1, arg2);
  }

  @Override
  public void debug(Marker marker, String format, Object... arguments) {
    this.delegate.debug(marker, format, arguments);
  }

  @Override
  public void debug(Marker marker, String msg, Throwable t) {
    this.delegate.debug(marker, msg, t);
  }

  @Override
  public LoggingEventBuilder atDebug() {
    return this.delegate.atDebug();
  }

  @Override
  public void info(String msg) {
    this.delegate.info(msg);
  }

  @Override
  public void info(String format, Object arg) {
    this.delegate.info(format, arg);
  }

  @Override
  public void info(String format, Object arg1, Object arg2) {
    this.delegate.info(format, arg1, arg2);
  }

  @Override
  public void info(String format, Object... arguments) {
    this.delegate.info(format, arguments);
  }

  @Override
  public void info(String msg, Throwable t) {
    this.delegate.info(msg, t);
  }

  @Override
  public void info(Marker marker, String msg) {
    this.delegate.info(marker, msg);
  }

  @Override
  public void info(Marker marker, String format, Object arg) {
    this.delegate.info(marker, format, arg);
  }

  @Override
  public void info(Marker marker, String format, Object arg1, Object arg2) {
    this.delegate.info(marker, format, arg1, arg2);
  }

  @Override
  public void info(Marker marker, String format, Object... arguments) {
    this.delegate.info(marker, format, arguments);
  }

  @Override
  public void info(Marker marker, String msg, Throwable t) {
    this.delegate.info(marker, msg, t);
  }

  @Override
  public LoggingEventBuilder atInfo() {
    return this.delegate.atInfo();
  }

  @Override
  public void warn(String msg) {
    this.delegate.warn(msg);
  }

  @Override
  public void warn(String format, Object arg) {
    this.delegate.warn(format, arg);
  }

  @Override
  public void warn(String format, Object... arguments) {
    this.delegate.warn(format, arguments);
  }

  @Override
  public void warn(String format, Object arg1, Object arg2) {
    this.delegate.warn(format, arg1, arg2);
  }

  @Override
  public void warn(String msg, Throwable t) {
    this.delegate.warn(msg, t);
  }

  @Override
  public void warn(Marker marker, String msg) {
    this.delegate.warn(marker, msg);
  }

  @Override
  public void warn(Marker marker, String format, Object arg) {
    this.delegate.warn(marker, format, arg);
  }

  @Override
  public void warn(Marker marker, String format, Object arg1, Object arg2) {
    this.delegate.warn(marker, format, arg1, arg2);
  }

  @Override
  public void warn(Marker marker, String format, Object... arguments) {
    this.delegate.warn(marker, format, arguments);
  }

  @Override
  public void warn(Marker marker, String msg, Throwable t) {
    this.delegate.warn(marker, msg, t);
  }

  @Override
  public LoggingEventBuilder atWarn() {
    return this.delegate.atWarn();
  }

  @Override
  public void error(String msg) {
    this.delegate.error(msg);
  }

  @Override
  public void error(String format, Object arg) {
    this.delegate.error(format, arg);
  }

  @Override
  public void error(String format, Object arg1, Object arg2) {
    this.delegate.error(format, arg1, arg2);
  }

  @Override
  public void error(String format, Object... arguments) {
    this.delegate.error(format, arguments);
  }

  @Override
  public void error(String msg, Throwable t) {
    this.delegate.error(msg, t);
  }

  @Override
  public void error(Marker marker, String msg) {
    this.delegate.error(marker, msg);
  }

  @Override
  public void error(Marker marker, String format, Object arg) {
    this.delegate.error(marker, format, arg);
  }

  @Override
  public void error(Marker marker, String format, Object arg1, Object arg2) {
    this.delegate.error(marker, format, arg1, arg2);
  }

  @Override
  public void error(Marker marker, String format, Object... arguments) {
    this.delegate.error(marker, format, arguments);
  }

  @Override
  public void error(Marker marker, String msg, Throwable t) {
    this.delegate.error(marker, msg, t);
  }

  @Override
  public LoggingEventBuilder atError() {
    return this.delegate.atError();
  }

}
