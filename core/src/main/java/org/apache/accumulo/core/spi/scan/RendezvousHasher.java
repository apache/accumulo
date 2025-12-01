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
package org.apache.accumulo.core.spi.scan;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.hadoop.io.Text;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import com.google.common.base.Preconditions;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

/**
 * Computes and caches the computations of rendezvous hashes to map tablets to scan servers.
 */
// intentionally package private so that it is not part of SPI, this is just internal utility code
class RendezvousHasher {
  private static class ServerHash implements Comparable<ServerHash> {
    final HashCode hash;
    final String server;

    private ServerHash(HashCode hash, String server) {
      this.hash = hash;
      this.server = server;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof ServerHash) {
        return compareTo((ServerHash) o) == 0;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return hash.asInt();
    }

    @Override
    public int compareTo(ServerHash o) {
      return Arrays.compare(hash.asBytes(), o.hash.asBytes());
    }
  }

  private static class CacheKey {
    final Mode mode;
    final String group;
    final TabletId tablet;
    final int desiredServers;
    final String salt;

    private CacheKey(Mode mode, String group, TabletId tablet, int numServers, String salt) {
      this.mode = mode;
      this.group = group;
      this.tablet = tablet;
      this.desiredServers = numServers;
      this.salt = salt;
    }

    @Override
    public int hashCode() {
      return Objects.hash(mode, group, tablet, desiredServers, salt);
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof CacheKey) {
        var ock = (CacheKey) o;
        return mode == ock.mode && group.equals(ock.group) && tablet.equals(ock.tablet)
            && desiredServers == ock.desiredServers && Objects.equals(salt, ock.salt);
      }

      return false;
    }
  }

  private final ScanServersSnapshot snapshot;
  private final Cache<CacheKey,List<String>> cache;

  RendezvousHasher(ScanServersSnapshot snapshot, int maxCacheSize) {
    Weigher<CacheKey,List<String>> weigher = (cacheKey, servers) -> {

      // for the list of servers the length of the strings is not considered because the server
      // strings may be pointed to by multiple cache entries, so only considering the pointer size
      return 64 + cacheKey.tablet.getTable().canonical().length()
          + weigh(cacheKey.tablet.getEndRow()) + weigh(cacheKey.tablet.getPrevEndRow())
          + servers.size() * 8;
    };
    this.snapshot = snapshot;
    cache = Caffeine.newBuilder().weigher(weigher).maximumWeight(maxCacheSize).build();
  }

  enum Mode {
    SERVER, HOST
  }

  /**
   * Finds "desiredServers" number of servers for a given tablet using rendezvous hashing. For a
   * given tablet+salt the same subset of servers will always be computed from the same set of all
   * servers. The set of servers computed for a tablet+salt is likely to be stable as the set of all
   * servers changes, but may change sometimes. This allows many clients to usually choose the same
   * set of scan servers for a given tablet even as the set of scan servers changes over time.
   */
  List<String> rendezvous(Mode mode, String group, TabletId tablet, String salt,
      int desiredServers) {
    if (mode == Mode.SERVER && desiredServers >= getSnapshot().getServersForGroup(group).size()) {
      // no need to compute rendezvous hash because all servers are wanted
      return getSnapshot().getServersForGroupAsList(group);
    }

    if (mode == Mode.HOST && desiredServers >= getSnapshot().getHostsForGroup(group).size()) {
      // no need to compute rendezvous hash because all hosts are wanted
      return getSnapshot().getHostsForGroup(group);
    }

    var cacheKey = new CacheKey(mode, group, tablet, desiredServers, salt);

    // Because the computations are derived from an immutable snapshot, it is safe to cache the
    // computations result as it should always be the same.
    return cache.get(cacheKey, this::rendezvousHash);
  }

  /**
   * @return the snapshot of scan servers used to create this hasher
   */
  ScanServersSnapshot getSnapshot() {
    return snapshot;
  }

  private List<String> rendezvousHash(CacheKey ck) {
    switch (ck.mode) {
      case HOST:
        return rendezvousHashHost(ck);
      case SERVER:
        return rendezvousHashServers(ck);
      default:
        throw new IllegalArgumentException(ck.mode.name());
    }
  }

  private List<String> rendezvousHashServers(CacheKey ck) {
    Preconditions.checkState(ck.desiredServers > 0, "%s", ck.desiredServers);

    if (ck.desiredServers == 1) {
      // optimization that does minimal work for finding a single server
      ServerHash minServerHash = findMinHash(ck);

      if (minServerHash == null) {
        return List.of();
      } else {
        return List.of(minServerHash.server);
      }
    } else {
      // Tracks the maximum minimums seen. Used to find the least M hashes w/o sorting all N hashes.
      // This algorithm should be O(N*log2(M)) which is a bit better than O(N*log2(N)) when sorting
      // everything.
      var pq = new PriorityQueue<ServerHash>(ck.desiredServers, Comparator.reverseOrder());
      Iterator<String> iter = getSnapshot().getServersForGroup(ck.group).iterator();

      // populate the priority queue with the first hashes, these are initial set of minimum hashes
      while (iter.hasNext() && pq.size() < ck.desiredServers) {
        var server = iter.next();
        var hc = hash(ck.tablet, server, ck.salt);
        pq.add(new ServerHash(hc, server));
      }

      // look through the rest of the hashes finding the rest of the minimum hashes
      while (iter.hasNext()) {
        var server = iter.next();
        var hc = hash(ck.tablet, server, ck.salt);
        var serverHash = new ServerHash(hc, server);
        if (serverHash.compareTo(pq.peek()) < 0) {
          // This hash is less than the maximum minimum seen so replace it.
          pq.poll(); // Remove from priority queue before adding to avoid increasing its array size
                     // and causing an internal reallocation
          pq.add(serverHash);
        }
      }

      List<String> found = new ArrayList<>(pq.size());
      pq.forEach(serverHash -> found.add(serverHash.server));
      // return an immutable list as it will be cached and potentially shared many times
      return List.copyOf(found);
    }
  }

  /**
   * Finds a set of host using rendezvous hashing compensating for host that have different numbers
   * of servers. When choosing a host the goal of this code is to select it based on its relative
   * number of servers. So if TS is the total number of servers and a host has HS servers then the
   * probability of that host being chosen for any tablet should be HS/TS. For example if there are
   * 30 hosts and 295 servers running across all the hosts then for a host with 3 servers its
   * probability of being chosen should be 3/295=1.02% across all tablets. If the host with 3
   * servers were chosen uniformly from the 30 host then it would be chosen 1/30=3.33% of the time
   * instead of 1.02% which would cause its scan servers to have higher load than other scan
   * servers.
   *
   * <p>
   * The implementation for achieving this goal is to sort all servers (not host) by hash and then
   * look for N unique host in the first part of the sorted list. If a host has fewer servers than
   * other host then it will show up less frequently in the beginning of the list on average for
   * different tablets. This roughly achieves the goal of this code, in limited testing it seems to
   * skew slightly higher than the desired probability. This approach could benefit from more
   * testing and looking at the statistics, suspect the greedy nature of the algorithm may throw
   * things off a bit.
   */
  private List<String> rendezvousHashHost(CacheKey ck) {

    Set<String> allServers = getSnapshot().getServersForGroup(ck.group);
    if (ck.desiredServers == 1) {
      // optimization for finding a single host, avoids sorting everything and allocates fewer
      // objects
      ServerHash minServerHash = findMinHash(ck);

      if (minServerHash == null) {
        return List.of();
      } else {
        return List.of(HostAndPort.fromString(minServerHash.server).getHost());
      }
    } else {
      List<ServerHash> allHashes = new ArrayList<>(allServers.size());
      for (String server : allServers) {
        var hc = hash(ck.tablet, server, ck.salt);
        var serverHash = new ServerHash(hc, server);
        allHashes.add(serverHash);
      }

      // TODO can this be optimized to avoid sorting all servers? might be a good follow on
      allHashes.sort(Comparator.naturalOrder());

      Set<String> hostsSeen = new HashSet<>();
      Iterator<ServerHash> iter = allHashes.iterator();
      while (hostsSeen.size() < ck.desiredServers && iter.hasNext()) {
        String host = HostAndPort.fromString(iter.next().server).getHost();
        hostsSeen.add(host);
      }

      // return an immutable list as it will be cached and potentially shared many times
      return List.copyOf(hostsSeen);
    }
  }

  private ServerHash findMinHash(CacheKey ck) {
    ServerHash minServerHash = null;
    // This code only exists as a performance optimization to avoid sorting all data for the case of
    // finding a single server. It could be shorter if using streams, but would probably be much
    // slower which would defeat its reason for existence.
    Iterator<String> iter = getSnapshot().getServersForGroup(ck.group).iterator();

    // this initial check avoids doing a null check in the subsequent while loop
    if (iter.hasNext()) {
      String server = iter.next();
      var hc = hash(ck.tablet, server, ck.salt);
      minServerHash = new ServerHash(hc, server);
    }

    while (iter.hasNext()) {
      String server = iter.next();
      var hc = hash(ck.tablet, server, ck.salt);
      var serverHash = new ServerHash(hc, server);
      if (serverHash.compareTo(minServerHash) < 0) {
        minServerHash = serverHash;
      }
    }

    return minServerHash;
  }

  private static int weigh(Text t) {
    if (t == null) {
      return 8;
    } else {
      return 8 + t.getBytes().length;
    }
  }

  private HashCode hash(TabletId tablet, String server, String salt) {
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

    hasher.putString(server, UTF_8);

    if (salt != null && !salt.isEmpty()) {
      hasher.putString(salt, UTF_8);
    }

    return hasher.hash();
  }
}
