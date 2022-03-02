/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.spi.scan;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.security.SecureRandom;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;

import org.apache.accumulo.core.data.TabletId;

import com.google.common.hash.Hashing;

public class DefaultEcScanManager implements ScanServerDispatcher {

  private static final ScanServerDispatcherResults NO_SCAN_SERVER_RESULT =
      new ScanServerDispatcherResults() {
        @Override
        public Action getAction(TabletId tablet) {
          return Action.USE_TABLET_SERVER;
        }

        @Override
        public String getScanServer(TabletId tablet) {
          return null;
        }

        @Override
        public Duration getDelay(String server) {
          // TODO is delay needed if there were prev errors?
          return Duration.ZERO;
        }
      };

  private static final SecureRandom RANDOM = new SecureRandom();
  private static final long INITIAL_SLEEP_TIME = 100L;
  private static final long MAX_SLEEP_TIME = 300000L;
  private final int INITIAL_SERVERS = 3;
  private final int MAX_DEPTH = 3;

  @Override
  public ScanServerDispatcherResults determineActions(DispatcherParameters params) {

    if (params.getScanServers().isEmpty()) {
      return NO_SCAN_SERVER_RESULT;
    }

    Map<TabletId,String> serversMap = new HashMap<>();
    Map<String,Long> sleepTimes = new HashMap<>();

    for (TabletId tablet : params.getTablets()) {

      SortedSet<ScanAttempt> attempts = params.getScanAttempts().forTablet(tablet);

      long sleepTime = 0;
      String serverToUse;

      if (!attempts.isEmpty() && attempts.last().getResult() == ScanAttempt.Result.SUCCESS
          && params.getScanServers().contains(attempts.last().getServer())) {
        // Stick with what was chosen last time
        serverToUse = attempts.last().getServer();
      } else {
        int hashCode = hashTablet(tablet);

        // TODO handle io errors
        int busyAttempts = (int) attempts.stream()
            .filter(scanAttempt -> scanAttempt.getResult() == ScanAttempt.Result.BUSY).count();

        int numServers;

        if (busyAttempts < MAX_DEPTH) {
          numServers = (int) Math.round(INITIAL_SERVERS
              * Math.pow(params.getOrderedScanServers().size() / (double) INITIAL_SERVERS,
                  busyAttempts / (double) MAX_DEPTH));
        } else {
          numServers = params.getOrderedScanServers().size();
        }

        int serverIndex =
            (hashCode + RANDOM.nextInt(numServers)) % params.getOrderedScanServers().size();
        serverToUse = params.getOrderedScanServers().get(serverIndex);

        if (busyAttempts > MAX_DEPTH) {
          sleepTime = (long) (INITIAL_SLEEP_TIME * Math.pow(2, busyAttempts - (MAX_DEPTH + 1)));
          sleepTime = Math.min(sleepTime, MAX_SLEEP_TIME);
        }
      }
      serversMap.put(tablet, serverToUse);
      sleepTimes.merge(serverToUse, sleepTime, Long::max);

    }

    return new ScanServerDispatcherResults() {
      @Override
      public Action getAction(TabletId tablet) {
        return Action.USE_SCAN_SERVER;
      }

      @Override
      public String getScanServer(TabletId tablet) {
        return serversMap.get(tablet);
      }

      @Override
      public Duration getDelay(String server) {
        return Duration.of(sleepTimes.getOrDefault(server, 0L), ChronoUnit.MILLIS);
      }
    };
  }

  private int hashTablet(TabletId tablet) {
    var hasher = Hashing.murmur3_32_fixed().newHasher();

    hasher.putString(tablet.getTable().canonical(), UTF_8);

    if (tablet.getEndRow() != null) {
      hasher.putBytes(tablet.getEndRow().getBytes(), 0, tablet.getEndRow().getLength());
    }

    if (tablet.getPrevEndRow() != null) {
      hasher.putBytes(tablet.getPrevEndRow().getBytes(), 0, tablet.getPrevEndRow().getLength());
    }

    return hasher.hash().asInt();
  }
}
