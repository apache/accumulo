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
package org.apache.accumulo.monitor.util.celltypes;

import org.apache.accumulo.core.util.Duration;

public class DurationType extends NumberType<Long> {
  private Long errMin;
  private Long errMax;

  public DurationType() {
    this(null, null);
  }

  public DurationType(Long errMin, Long errMax) {
    this.errMin = errMin;
    this.errMax = errMax;
  }

  @Override
  public String format(Object obj) {
    if (obj == null)
      return "-";
    Long millis = (Long) obj;
    if (errMin != null && errMax != null)
      return seconds(millis, errMin, errMax);
    return Duration.format(millis);
  }

  private static String seconds(long secs, long errMin, long errMax) {
    String numbers = Duration.format(secs);
    if (secs < errMin || secs > errMax)
      return "<span class='error'>" + numbers + "</span>";
    return numbers;
  }

}
