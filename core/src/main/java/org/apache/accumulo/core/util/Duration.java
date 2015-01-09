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
package org.apache.accumulo.core.util;

public class Duration {

  public static String format(long time) {
    return format(time, "&nbsp;");
  }

  public static String format(long time, String space) {
    return format(time, space, "&mdash;");
  }

  public static String format(long time, String space, String zero) {
    long ms, sec, min, hr, day, yr;
    ms = sec = min = hr = day = yr = -1;
    if (time == 0)
      return zero;
    ms = time % 1000;
    time /= 1000;
    if (time == 0)
      return String.format("%dms", ms);
    sec = time % 60;
    time /= 60;
    if (time == 0)
      return String.format("%ds" + space + "%dms", sec, ms);
    min = time % 60;
    time /= 60;
    if (time == 0)
      return String.format("%dm" + space + "%ds", min, sec);
    hr = time % 24;
    time /= 24;
    if (time == 0)
      return String.format("%dh" + space + "%dm", hr, min);
    day = time % 365;
    time /= 365;
    if (time == 0)
      return String.format("%dd" + space + "%dh", day, hr);
    yr = time;
    return String.format("%dy" + space + "%dd", yr, day);

  }

}
