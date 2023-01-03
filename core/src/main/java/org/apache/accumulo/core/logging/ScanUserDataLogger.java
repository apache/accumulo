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
import org.slf4j.event.Level;

public class ScanUserDataLogger {

  private static final Logger LOG = LoggerFactory.getLogger(ScanUserDataLogger.class);
  private static final String FORMAT = "(%s) %s";

  /**
   * Utility method that will log the msg and args to the calling class' logger (classLogger) if the
   * classLogger argument is supplied. The classLogger argument would be null in the case where we
   * don't want to log the same thing twice.
   *
   * @param level slf4j log level
   * @param classLogger calling class' slf4j logger
   * @param userData scan userData value
   * @param msg log message
   * @param args log message argument
   */
  public static void log(Level level, Logger classLogger, String userData, String msg,
      Object... args) {
    if (classLogger != null && classLogger.isEnabledForLevel(level)) {
      classLogger.atLevel(level).log(msg, args);
    }
    if (LOG.isEnabledForLevel(level)) {
      if (args == null) {
        LOG.atLevel(level).log(String.format(FORMAT, userData, msg));
      } else {
        LOG.atLevel(level).log(String.format(FORMAT, userData, msg), args);
      }
    }
  }
}
