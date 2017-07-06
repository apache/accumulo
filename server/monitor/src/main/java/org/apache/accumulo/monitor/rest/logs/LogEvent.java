/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.monitor.rest.logs;

/**
 *
 * A single message logged
 *
 * @since 2.0.0
 *
 */
public class LogEvent {

  // Variable names become JSON keys
  public long timestamp;
  public Object application;
  public int count;
  public String level, message;

  public LogEvent() {}

  /**
   * Stores a new log event
   *
   * @param timestamp
   *          log event timestamp
   * @param application
   *          log event application
   * @param count
   *          log event count
   * @param level
   *          log event level
   * @param message
   *          log event message
   */
  public LogEvent(long timestamp, Object application, int count, String level, String message) {
    this.timestamp = timestamp;
    this.application = application;
    this.count = count;
    this.level = level;
    this.message = message;
  }
}
