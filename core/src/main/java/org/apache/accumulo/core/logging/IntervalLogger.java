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

import org.slf4j.Logger;
import org.slf4j.Marker;
import org.slf4j.event.Level;
import org.slf4j.helpers.AbstractLogger;

/**
 * Logger that wraps another Logger and only emits a log message once per the supplied duration.
 *
 */
public class IntervalLogger extends AbstractLogger {

  private static final long serialVersionUID = 1L;

  private final Logger delegate;
  private final long intervalNanos;

  private volatile long last = 0L;

  public IntervalLogger(Logger log, Duration interval) {
    this.delegate = log;
    this.intervalNanos = interval.toNanos();
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
    if (System.nanoTime() - last > intervalNanos) {
      delegate.atLevel(level).addMarker(marker).setCause(throwable).log(messagePattern, arguments);
      last = System.nanoTime();
    }
  }

}
