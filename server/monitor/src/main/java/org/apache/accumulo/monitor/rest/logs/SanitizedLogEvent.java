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
package org.apache.accumulo.monitor.rest.logs;

/**
 * A log event type (with counts) suitable for display on the monitor at the REST endpoint,
 * {@link LogResource#getRecentLogs()}.
 */
public class SanitizedLogEvent {

  // Variable names become JSON keys
  public final long timestamp;
  public final String application;
  public final String logger;
  public final String level;
  public final String message;
  public final String stacktrace;
  public final int count;

  public SanitizedLogEvent(SingleLogEvent event, int count) {
    this.timestamp = event.timestamp;
    this.application = sanitize(event.application);
    this.logger = sanitize(event.logger);
    this.level = sanitize(event.level);
    String msg = sanitize(event.message);
    // truncate long messages
    if (msg.length() > 300) {
      msg = msg.substring(0, 300).trim();
    }
    this.message = msg;
    this.stacktrace = sanitize(event.stacktrace);
    this.count = count;
  }

  private String sanitize(String s) {
    if (s == null) {
      return null;
    }
    StringBuilder text = new StringBuilder();
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      int type = Character.getType(c);
      boolean notPrintable = type == Character.UNASSIGNED || type == Character.LINE_SEPARATOR
          || type == Character.NON_SPACING_MARK || type == Character.PRIVATE_USE;
      text.append(notPrintable ? '?' : c);
    }
    return text.toString().trim().replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">",
        "&gt;");
  }

}
