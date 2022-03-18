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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.accumulo.core.data.TabletId;

import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.hash.Hashing;

public class DefaultScanServerDispatcher implements ScanServerDispatcher {

  private static final SecureRandom RANDOM = new SecureRandom();

  private Duration initialBusyTimeout;
  private Duration maxBusyTimeout;

  private int initialServers;
  private int maxDepth;

  private Supplier<List<String>> orderedScanServersSupplier;

  @Override
  public void init(InitParameters params) {
    // avoid constantly resorting the scan servers, just do it periodically in case they change
    orderedScanServersSupplier = Suppliers.memoizeWithExpiration(() -> {
      List<String> oss = new ArrayList<>(params.getScanServers().get());
      Collections.sort(oss);
      return Collections.unmodifiableList(oss);
    }, 100, TimeUnit.MILLISECONDS);

    var opts = params.getOptions();

    initialServers = Integer.parseInt(opts.getOrDefault("initialServers", "3"));
    maxDepth = Integer.parseInt(opts.getOrDefault("maxDepth", "3"));
    initialBusyTimeout = Duration.parse(opts.getOrDefault("initialBusyTimeout", "PT0.033S"));
    maxBusyTimeout = Duration.parse(opts.getOrDefault("maxBusyTimeout", "PT30M"));

    Preconditions.checkArgument(initialServers > 0, "initialServers must be positive : %s",
        initialServers);
    Preconditions.checkArgument(maxDepth > 0, "maxDepth must be positive : %s", maxDepth);
    Preconditions.checkArgument(initialBusyTimeout.compareTo(Duration.ZERO) > 0,
        "initialBusyTimeout must be positive %s", initialBusyTimeout);
    Preconditions.checkArgument(maxBusyTimeout.compareTo(Duration.ZERO) > 0,
        "maxBusyTimeout must be positive %s", maxBusyTimeout);
  }

  @Override
  public Actions determineActions(DispatcherParameters params) {

    // only get this once and use it for the entire method so that the method uses a consistent
    // snapshot
    List<String> orderedScanServers = orderedScanServersSupplier.get();

    if (orderedScanServers.isEmpty()) {
      return new Actions() {
        @Override
        public String getScanServer(TabletId tabletId) {
          return null;
        }

        @Override
        public Duration getDelay() {
          return Duration.ZERO;
        }

        @Override
        public Duration getBusyTimeout() {
          return Duration.ZERO;
        }
      };
    }

    Map<TabletId,String> serversToUse = new HashMap<>();

    long maxBusyAttempts = 0;

    for (TabletId tablet : params.getTablets()) {

      // TODO handle io errors
      long busyAttempts = params.getAttempts(tablet).stream()
          .filter(sa -> sa.getResult() == ScanAttempt.Result.BUSY).count();

      maxBusyAttempts = Math.max(maxBusyAttempts, busyAttempts);

      String serverToUse = null;

      int hashCode = hashTablet(tablet);

      int numServers;

      if (busyAttempts < maxDepth) {
        numServers = (int) Math
            .round(initialServers * Math.pow(orderedScanServers.size() / (double) initialServers,
                busyAttempts / (double) maxDepth));
      } else {
        numServers = orderedScanServers.size();
      }

      int serverIndex = Math.abs(hashCode + RANDOM.nextInt(numServers)) % orderedScanServers.size();

      // TODO could check if errors were seen on this server in past attempts
      serverToUse = orderedScanServers.get(serverIndex);

      serversToUse.put(tablet, serverToUse);
    }

    long busyTimeout = initialBusyTimeout.toMillis();

    if (maxBusyAttempts > maxDepth) {
      busyTimeout = (long) (busyTimeout * Math.pow(2, maxBusyAttempts - (maxDepth + 1)));
      busyTimeout = Math.min(busyTimeout, maxBusyTimeout.toMillis());
    }

    Duration busyTO = Duration.ofMillis(busyTimeout);

    return new Actions() {
      @Override
      public String getScanServer(TabletId tabletId) {
        return serversToUse.get(tabletId);
      }

      @Override
      public Duration getDelay() {
        return Duration.ZERO;
      }

      @Override
      public Duration getBusyTimeout() {
        return busyTO;
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
