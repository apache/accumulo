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

public class DefaultSessionComparator extends SessionComparator {

  @Override
  public int compareSession(Session sessionA, Session sessionB) {

    final long startTimeFirst = sessionA.startTime;
    final long startTimeSecond = sessionB.startTime;

    // use the lowest max idle time
    final long maxIdle = sessionA.maxIdleAccessTime < sessionB.maxIdleAccessTime
        ? sessionA.maxIdleAccessTime
        : sessionB.maxIdleAccessTime;

    final long currentTime = System.currentTimeMillis();

    /*
     * Multiply by -1 so that we have a sensical comparison. This means that if comparison < 0,
     * sessionA is newer. If comparison > 0, this means that session B is newer
     */
    int comparison = -1 * Long.compare(startTimeFirst, startTimeSecond);

    if (!(sessionA.lastExecTime == -1 && sessionB.lastExecTime == -1)) {
      if (comparison >= 0) {
        long idleTimeA = currentTime - sessionA.lastExecTime;

        /*
         * If session B is newer, let's make sure that we haven't reached the max idle time, where
         * we have to begin aging A
         */
        if (idleTimeA > sessionA.maxIdleAccessTime) {
          comparison = -1 * Long.valueOf(idleTimeA - maxIdle).intValue();
        }
      } else {
        long idleTimeB = currentTime - sessionB.lastExecTime;

        /*
         * If session A is newer, let's make sure that B hasn't reached the max idle time, where we
         * have to begin aging A
         */
        if (idleTimeB > sessionA.maxIdleAccessTime) {
          comparison = 1 * Long.valueOf(idleTimeB - maxIdle).intValue();
        }
      }
    }

    return comparison;
  }

}
