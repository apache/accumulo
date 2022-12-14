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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.event.Level;
import org.slf4j.helpers.AbstractLogger;
import org.slf4j.spi.LoggingEventBuilder;

public class ScanUserDataLogger extends AbstractLogger {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(ScanUserDataLogger.class);
  private static final ThreadLocal<String> USER_DATA = new ThreadLocal<>();

  public static void logTrace(Logger classLogger, String userData, String msg, Object... objects) {
    if (classLogger != null) {
      classLogger.trace(msg, objects);
    }
    if (!LOG.isTraceEnabled()) {
      return;
    }
    USER_DATA.set(userData);
    LOG.trace(msg, objects);
  }

  public static void logDebug(Logger classLogger, String userData, String msg, Object... objects) {
    if (classLogger != null) {
      classLogger.debug(msg, objects);
    }
    if (!LOG.isDebugEnabled()) {
      return;
    }
    USER_DATA.set(userData);
    LOG.debug(msg, objects);
  }

  public static void logInfo(Logger classLogger, String userData, String msg, Object... objects) {
    if (classLogger != null) {
      classLogger.info(msg, objects);
    }
    if (!LOG.isInfoEnabled()) {
      return;
    }
    USER_DATA.set(userData);
    LOG.info(msg, objects);
  }

  public static void logWarn(Logger classLogger, String userData, String msg, Object... objects) {
    if (classLogger != null) {
      classLogger.warn(msg, objects);
    }
    if (!LOG.isWarnEnabled()) {
      return;
    }
    USER_DATA.set(userData);
    LOG.warn(msg, objects);
  }

  public static void logError(Logger classLogger, String userData, String msg, Object... objects) {
    if (classLogger != null) {
      classLogger.error(msg, objects);
    }
    USER_DATA.set(userData);
    LOG.error(msg, objects);
  }

  @Override
  public boolean isTraceEnabled() {
    return LOG.isTraceEnabled();
  }

  @Override
  public boolean isTraceEnabled(Marker marker) {
    return LOG.isTraceEnabled(marker);
  }

  @Override
  public boolean isDebugEnabled() {
    return LOG.isDebugEnabled();
  }

  @Override
  public boolean isDebugEnabled(Marker marker) {
    return LOG.isDebugEnabled(marker);
  }

  @Override
  public boolean isInfoEnabled() {
    return LOG.isInfoEnabled();
  }

  @Override
  public boolean isInfoEnabled(Marker marker) {
    return LOG.isInfoEnabled(marker);
  }

  @Override
  public boolean isWarnEnabled() {
    return LOG.isWarnEnabled();
  }

  @Override
  public boolean isWarnEnabled(Marker marker) {
    return LOG.isWarnEnabled(marker);
  }

  @Override
  public boolean isErrorEnabled() {
    return LOG.isErrorEnabled();
  }

  @Override
  public boolean isErrorEnabled(Marker marker) {
    return LOG.isErrorEnabled(marker);
  }

  @Override
  protected String getFullyQualifiedCallerName() {
    return null;
  }

  @Override
  protected void handleNormalizedLoggingCall(Level level, Marker marker, String messagePattern,
      Object[] arguments, Throwable throwable) {

    final String userData = USER_DATA.get();
    USER_DATA.set(null);

    LoggingEventBuilder builder = LOG.atLevel(level);
    if (marker != null) {
      builder.addMarker(marker);
    }
    if (throwable != null) {
      builder.setCause(throwable);
    }
    builder.setMessage("({}) " + messagePattern);
    builder.addArgument(userData);
    if (arguments != null) {
      for (Object arg : arguments) {
        builder.addArgument(arg);
      }
    }
    builder.log();
  }

}
