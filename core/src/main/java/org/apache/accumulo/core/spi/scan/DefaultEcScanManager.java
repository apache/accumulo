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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import org.apache.accumulo.core.data.TabletId;

import com.google.common.hash.Hashing;

public class DefaultEcScanManager implements ScanServerDispatcher {

  private static final SecureRandom RANDOM = new SecureRandom();
  private static final long INITIAL_SLEEP_TIME = 100L;
  private static final long MAX_SLEEP_TIME = 300000L;
  private final int INITIAL_SERVERS = 3;
  private final int MAX_DEPTH = 3;

  Duration defaultBusyTimeout;

  private List<String> orderedScanServers;

  @Override
  public void init(InitParameters params) {
    orderedScanServers = new ArrayList<>(params.getScanServers());
    Collections.sort(orderedScanServers);
    defaultBusyTimeout = Duration.of(33, ChronoUnit.MILLIS);
    ;
  }

  private String getLastSuccessfulScanServer(SortedSet<ScanAttempt> attempts) {
    if (attempts.isEmpty())
      return null;
    var last = attempts.last();
    if (last.getResult() != ScanAttempt.Result.SUCCESS)
      return null;
    var action = last.getAction();
    if (action instanceof UseScanServerAction) {
      return ((UseScanServerAction) action).getServer();
    } else {
      return null;
    }

  }

  @Override
  public Actions determineActions(DispatcherParameters params) {

    if (orderedScanServers.isEmpty()) {
      return Actions.from(List.of(new UseTserverAction(params.getTablets())));
    }

    Map<String,Long> sleepTimes = new HashMap<>();
    Map<String,List<TabletId>> serversTablets = new HashMap<>();

    for (TabletId tablet : params.getTablets()) {

      SortedSet<ScanAttempt> attempts = params.getScanAttempts().forTablet(tablet);

      long sleepTime = 0;
      String serverToUse = getLastSuccessfulScanServer(attempts);

      if (serverToUse == null) {
        int hashCode = hashTablet(tablet);

        // TODO handle io errors
        int busyAttempts = (int) attempts.stream()
            .filter(scanAttempt -> scanAttempt.getResult() == ScanAttempt.Result.BUSY).count();

        int numServers;

        if (busyAttempts < MAX_DEPTH) {
          numServers = (int) Math.round(
              INITIAL_SERVERS * Math.pow(orderedScanServers.size() / (double) INITIAL_SERVERS,
                  busyAttempts / (double) MAX_DEPTH));
        } else {
          numServers = orderedScanServers.size();
        }

        int serverIndex =
            Math.abs(hashCode + RANDOM.nextInt(numServers)) % orderedScanServers.size();
        serverToUse = orderedScanServers.get(serverIndex);

        if (busyAttempts > MAX_DEPTH) {
          sleepTime = (long) (INITIAL_SLEEP_TIME * Math.pow(2, busyAttempts - (MAX_DEPTH + 1)));
          sleepTime = Math.min(sleepTime, MAX_SLEEP_TIME);
        }
      }

      serversTablets.computeIfAbsent(serverToUse, k -> new ArrayList<>()).add(tablet);
      sleepTimes.merge(serverToUse, sleepTime, Long::max);
    }

    ArrayList<Action> actions = new ArrayList<>();

    serversTablets.forEach((server, tablets) -> {
      long sleepTime = sleepTimes.getOrDefault(server, 0L);
      Duration busyTimeout = defaultBusyTimeout;
      if (sleepTime > 0) {
        busyTimeout = Duration.of(sleepTime, ChronoUnit.MILLIS);
      }
      actions.add(new UseScanServerAction(server, tablets, Duration.ZERO, busyTimeout));
    });

    return Actions.from(actions);
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
