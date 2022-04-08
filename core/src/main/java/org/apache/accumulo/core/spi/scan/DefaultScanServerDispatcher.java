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
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.accumulo.core.data.TabletId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.Sets;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

/**
 * The default Accumulo dispatcher for scan servers. This dispatcher will hash tablets to a few
 * random scan servers (defaults to 3). So a given tablet will always go to the same 3 scan servers.
 * When scan servers are busy, this dispatcher will rapidly expand the number of scan servers it
 * randomly chooses from for a given tablet. With the default settings and 1000 scan servers that
 * are busy, this dispatcher would randomly choose from 3, 21, 144, and then 1000 scan servers.
 * After getting to a point where we are raondomly choosing from all scan server, if busy is still
 * being observed then this dispatcher will start to exponentially increase the busy timeout. If all
 * scan servers are busy then its best to just go to one and wait for your scan to run, which is why
 * the busy timeout increases exponentially when it seems like everything is busy.
 *
 * <p>
 * The following options are accepted in {@link #init(InitParameters)}
 * </p>
 *
 * <ul>
 * <li><b>initialServers</b> the initial number of servers to randomly choose from for a given
 * tablet. Defaults to 3.</li>
 * <li><b>initialBusyTimeout</b>The initial busy timeout to use when contacting a scan servers. If
 * the scan does start running within the busy timeout then another scan server can be tried.
 * Defaults to PT0.033S see {@link Duration#parse(CharSequence)}</li>
 * <li><b>maxBusyTimeout</b>When busy is repeatedly seen, then the busy timeout will be increased
 * exponentially. This setting controls the maximum busyTimeout. Defaults to PT30M</li>
 * <li><b>maxDepth</b>When busy is observed the number of servers to randomly chose from is
 * expanded. This setting controls how many busy observations it will take before we choose from all
 * servers.</li>
 * </ul>
 *
 *
 */
public class DefaultScanServerDispatcher implements ScanServerDispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultScanServerDispatcher.class);

  private static final SecureRandom RANDOM = new SecureRandom();

  protected Duration initialBusyTimeout;
  protected Duration maxBusyTimeout;

  protected int initialServers;
  protected int maxDepth;

  private Supplier<List<String>> orderedScanServersSupplier;

  private static final Set<String> OPT_NAMES =
      Set.of("initialServers", "maxDepth", "initialBusyTimeout", "maxBusyTimeout");

  @Override
  public void init(InitParameters params) {
    // avoid constantly resorting the scan servers, just do it periodically in case they change
    orderedScanServersSupplier = Suppliers.memoizeWithExpiration(() -> {
      List<String> oss = new ArrayList<>(params.getScanServers().get());
      Collections.sort(oss);
      return Collections.unmodifiableList(oss);
    }, 100, TimeUnit.MILLISECONDS);

    var opts = params.getOptions();

    var diff = Sets.difference(opts.keySet(), OPT_NAMES);

    Preconditions.checkArgument(diff.isEmpty(), "Unknown options %s", diff);

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

    LOG.debug(
        "DefaultScanServerDispatcher configured with initialServers: {}"
            + ", maxDepth: {}, initialBusyTimeout: {}, maxBusyTimeout: {}",
        initialServers, maxDepth, initialBusyTimeout, maxBusyTimeout);
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

      var hashCode = hashTablet(tablet);

      int numServers;

      if (busyAttempts < maxDepth) {
        numServers = (int) Math
            .round(initialServers * Math.pow(orderedScanServers.size() / (double) initialServers,
                busyAttempts / (double) maxDepth));
      } else {
        numServers = orderedScanServers.size();
      }

      int serverIndex =
          (Math.abs(hashCode.asInt()) + RANDOM.nextInt(numServers)) % orderedScanServers.size();

      // TODO could check if errors were seen on this server in past attempts
      serverToUse = orderedScanServers.get(serverIndex);

      serversToUse.put(tablet, serverToUse);
    }

    long busyTimeout = initialBusyTimeout.toMillis();

    if (maxBusyAttempts > maxDepth) {
      busyTimeout = (long) (busyTimeout * Math.pow(8, maxBusyAttempts - (maxDepth + 1)));
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

  private HashCode hashTablet(TabletId tablet) {
    var hasher = Hashing.murmur3_128().newHasher();

    if (tablet.getEndRow() != null) {
      hasher.putBytes(tablet.getEndRow().getBytes(), 0, tablet.getEndRow().getLength());
    } else {
      hasher.putByte((byte) 5);
    }

    if (tablet.getPrevEndRow() != null) {
      hasher.putBytes(tablet.getPrevEndRow().getBytes(), 0, tablet.getPrevEndRow().getLength());
    } else {
      hasher.putByte((byte) 7);
    }

    hasher.putString(tablet.getTable().canonical(), UTF_8);

    return hasher.hash();
  }
}
