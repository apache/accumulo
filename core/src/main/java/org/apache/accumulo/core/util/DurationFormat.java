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
package org.apache.accumulo.core.util;

public class DurationFormat {
  private final String str;

  public DurationFormat(long time, String space) {
    String dash = "-";
    long ms, sec, min, hr, day, yr;

    if (time == 0) {
      str = dash;
      return;
    }

    ms = time % 1000;
    time /= 1000;

    if (time == 0) {
      str = String.format("%dms", ms);
      return;
    }

    sec = time % 60;
    time /= 60;

    if (time == 0) {
      str = String.format("%ds" + space + "%dms", sec, ms);
      return;
    }

    min = time % 60;
    time /= 60;

    if (time == 0) {
      str = String.format("%dm" + space + "%ds", min, sec);
      return;
    }

    hr = time % 24;
    time /= 24;

    if (time == 0) {
      str = String.format("%dh" + space + "%dm", hr, min);
      return;
    }

    day = time % 365;
    time /= 365;

    if (time == 0) {
      str = String.format("%dd" + space + "%dh", day, hr);
      return;
    }
    yr = time;

    str = String.format("%dy" + space + "%dd", yr, day);
  }

  @Override
  public String toString() {
    return str;
  }
}
