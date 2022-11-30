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
package org.apache.accumulo.server.util.time;

/**
 * Provide time from a local source and a hint from a time source.
 *
 * RelativeTime and BaseRelativeTime are separated to provide unit tests of the core functionality
 * of Relative timekeeping.
 */
public class BaseRelativeTime implements ProvidesTime {

  private long diff = 0;
  private long lastReportedTime = 0;
  ProvidesTime local;

  BaseRelativeTime(ProvidesTime real, long lastReportedTime) {
    this.local = real;
    this.lastReportedTime = lastReportedTime;
  }

  BaseRelativeTime(ProvidesTime real) {
    this(real, 0);
  }

  @Override
  public synchronized long currentTime() {
    long localNow = local.currentTime();
    long result = localNow + diff;
    if (result < lastReportedTime) {
      return lastReportedTime;
    }
    lastReportedTime = result;
    return result;
  }

  public synchronized void updateTime(long advice) {
    long localNow = local.currentTime();
    long diff = advice - localNow;
    // smooth in 20% of the change, not the whole thing.
    this.diff = (this.diff * 4 / 5) + diff / 5;
  }

}
