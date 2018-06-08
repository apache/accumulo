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
package org.apache.accumulo.tserver.session;

import java.util.Comparator;

import org.apache.accumulo.core.spi.scan.ScanInfo;
import org.apache.accumulo.core.spi.scan.ScanInfo.Stats;

//TODO bad name
//TODO no docs!!!
//TODO location
public class DefaultSessionComparator implements Comparator<ScanInfo> {

  @Override
  public int compare(ScanInfo si1, ScanInfo si2) {
    // TODO not sure if I converted this correctly!!!

    final long startTimeFirst = si1.getCreationTime();
    final long startTimeSecond = si2.getCreationTime();

    // use the lowest max idle time

    final long currentTime = ScanInfo.getCurrentTime();

    // TODO should this call getIdleTimeStats(currtime) ??
    long maxIdle1 = si1.getIdleTimeStats().map(Stats::max).orElse(0L);
    long maxIdle2 = si2.getIdleTimeStats().map(Stats::max).orElse(0L);

    final long maxIdle = maxIdle1 < maxIdle2 ? maxIdle1 : maxIdle2;

    /*
     * Multiply by -1 so that we have a sensical comparison. This means that if comparison < 0,
     * sessionA is newer. If comparison > 0, this means that session B is newer
     */
    int comparison = -1 * Long.compare(startTimeFirst, startTimeSecond);

    if (si1.getLastRunTime().isPresent() && si2.getLastRunTime().isPresent()) {
      if (comparison >= 0) {
        long idleTimeA = currentTime - si1.getLastRunTime().getAsLong();

        /*
         * If session B is newer, let's make sure that we haven't reached the max idle time, where
         * we have to begin aging A
         */
        if (idleTimeA > maxIdle1) {
          comparison = -1 * Long.valueOf(idleTimeA - maxIdle).intValue();
        }
      } else {
        long idleTimeB = currentTime - si2.getLastRunTime().getAsLong();

        /*
         * If session A is newer, let's make sure that B hasn't reached the max idle time, where we
         * have to begin aging A
         */
        if (idleTimeB > maxIdle1) {
          comparison = 1 * Long.valueOf(idleTimeB - maxIdle).intValue();
        }
      }
    }

    return comparison;
  }

}
