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
package org.apache.accumulo.core.fate.zookeeper;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.accumulo.core.util.LazySingletons.RANDOM;

import java.io.IOException;
import java.math.BigInteger;
import java.net.UnknownHostException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooUtil {

  private static final Logger log = LoggerFactory.getLogger(ZooUtil.class);

  private ZooUtil() {}

  private static class ZooSessionWatcher implements Watcher {

    private static final Logger watcherLog = LoggerFactory.getLogger(ZooSessionWatcher.class);
    private final AtomicReference<KeeperState> lastState = new AtomicReference<>(null);
    private final String clientName;

    public ZooSessionWatcher(String clientName) {
      this.clientName = clientName + "_" + UUID.randomUUID();
    }

    @Override
    public void process(WatchedEvent event) {
      final var newState = event.getState();
      var oldState = lastState.getAndUpdate(s -> newState);
      if (oldState == null) {
        watcherLog.debug("ZooKeeper[{}] state changed to {}", clientName, newState);
      } else if (newState != oldState) {
        watcherLog.debug("ZooKeeper[{}] state changed from {} to {}", clientName, oldState,
            newState);
      }
    }
  }

  public enum NodeExistsPolicy {
    SKIP, OVERWRITE, FAIL
  }

  public enum NodeMissingPolicy {
    SKIP, CREATE, FAIL
  }

  // used for zookeeper stat print formatting
  private static final DateTimeFormatter fmt =
      DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss 'UTC' yyyy");

  public static class LockID {
    public long eid;
    public String path;
    public String node;

    public LockID(String root, String serializedLID) {
      String[] sa = serializedLID.split("\\$");
      int lastSlash = sa[0].lastIndexOf('/');

      if (sa.length != 2 || lastSlash < 0) {
        throw new IllegalArgumentException("Malformed serialized lock id " + serializedLID);
      }

      if (lastSlash == 0) {
        path = root;
      } else {
        path = root + "/" + sa[0].substring(0, lastSlash);
      }
      node = sa[0].substring(lastSlash + 1);
      eid = new BigInteger(sa[1], 16).longValue();
    }

    public LockID(String path, String node, long eid) {
      this.path = path;
      this.node = node;
      this.eid = eid;
    }

    public String serialize(String root) {

      return path.substring(root.length()) + "/" + node + "$" + Long.toHexString(eid);
    }

    @Override
    public String toString() {
      return " path = " + path + " node = " + node + " eid = " + Long.toHexString(eid);
    }
  }

  // Need to use Collections.unmodifiableList() instead of List.of() or List.copyOf(), because
  // ImmutableCollections.contains() doesn't handle nulls properly (JDK-8265905) and ZooKeeper (as
  // of 3.8.1) calls acl.contains((Object) null) which throws a NPE when passed an immutable
  // collection
  public static final List<ACL> PRIVATE =
      Collections.unmodifiableList(new ArrayList<>(Ids.CREATOR_ALL_ACL));

  public static final List<ACL> PUBLIC;
  static {
    var publicTmp = new ArrayList<>(PRIVATE);
    publicTmp.add(new ACL(Perms.READ, Ids.ANYONE_ID_UNSAFE));
    PUBLIC = Collections.unmodifiableList(publicTmp);
  }

  public static String getRoot(final InstanceId instanceId) {
    return Constants.ZROOT + "/" + instanceId;
  }

  /**
   * This method will delete a node and all its children.
   */
  public static void recursiveDelete(ZooKeeper zooKeeper, String zPath, NodeMissingPolicy policy)
      throws KeeperException, InterruptedException {
    if (policy == NodeMissingPolicy.CREATE) {
      throw new IllegalArgumentException(policy.name() + " is invalid for this operation");
    }
    try {
      // delete children
      for (String child : zooKeeper.getChildren(zPath, null)) {
        recursiveDelete(zooKeeper, zPath + "/" + child, NodeMissingPolicy.SKIP);
      }

      // delete self
      zooKeeper.delete(zPath, -1);
    } catch (KeeperException e) {
      // new child appeared; try again
      if (e.code() == Code.NOTEMPTY) {
        recursiveDelete(zooKeeper, zPath, policy);
      }
      if (policy == NodeMissingPolicy.SKIP && e.code() == Code.NONODE) {
        return;
      }
      throw e;
    }
  }

  /**
   * For debug: print the ZooKeeper Stat with value labels for a more user friendly string. The
   * format matches the zookeeper cli stat command.
   *
   * @param stat Zookeeper Stat structure
   * @return a formatted string.
   */
  public static String printStat(final Stat stat) {

    if (stat == null) {
      return "null";
    }

    return "\ncZxid = " + String.format("0x%x", stat.getCzxid()) + "\nctime = "
        + getFmtTime(stat.getCtime()) + "\nmZxid = " + String.format("0x%x", stat.getMzxid())
        + "\nmtime = " + getFmtTime(stat.getMtime()) + "\npZxid = "
        + String.format("0x%x", stat.getPzxid()) + "\ncversion = " + stat.getCversion()
        + "\ndataVersion = " + stat.getVersion() + "\naclVersion = " + stat.getAversion()
        + "\nephemeralOwner = " + String.format("0x%x", stat.getEphemeralOwner())
        + "\ndataLength = " + stat.getDataLength() + "\nnumChildren = " + stat.getNumChildren();
  }

  private static String getFmtTime(final long epoch) {
    OffsetDateTime timestamp =
        OffsetDateTime.ofInstant(Instant.ofEpochMilli(epoch), ZoneOffset.UTC);
    return fmt.format(timestamp);
  }

  public static void digestAuth(ZooKeeper zoo, String secret) {
    zoo.addAuthInfo("digest", ("accumulo:" + secret).getBytes(UTF_8));
  }

  /**
   * Construct a new ZooKeeper client, retrying indefinitely if it doesn't work right away. The
   * caller is responsible for closing instances returned from this method.
   *
   * @param clientName a convenient name for logging its connection state changes
   * @param conf a convenient carrier of ZK connection information using Accumulo properties
   */
  public static ZooKeeper connect(String clientName, AccumuloConfiguration conf) {
    return ZooUtil.connect(clientName, conf.get(Property.INSTANCE_ZK_HOST),
        (int) conf.getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT),
        conf.get(Property.INSTANCE_SECRET));
  }

  /**
   * Construct a new ZooKeeper client, retrying indefinitely if it doesn't work right away. The
   * caller is responsible for closing instances returned from this method.
   *
   * @param clientName a convenient name for logging its connection state changes
   * @param connectString in the form of host1:port1,host2:port2/chroot/path
   * @param timeout in milliseconds
   * @param instanceSecret instance secret (may be null)
   */
  public static ZooKeeper connect(String clientName, String connectString, int timeout,
      String instanceSecret) {
    log.debug("Connecting to {} with timeout {} with auth", connectString, timeout);
    final int TIME_BETWEEN_CONNECT_CHECKS_MS = 100;
    int connectTimeWait = Math.min(10_000, timeout);
    boolean tryAgain = true;
    long sleepTime = 100;
    ZooKeeper zk = null;

    var watcher = new ZooSessionWatcher(clientName);

    long startTime = System.nanoTime();

    while (tryAgain) {
      try {
        zk = new ZooKeeper(connectString, timeout, watcher);
        // it may take some time to get connected to zookeeper if some of the servers are down
        for (int i = 0; i < connectTimeWait / TIME_BETWEEN_CONNECT_CHECKS_MS && tryAgain; i++) {
          if (zk.getState().isConnected()) {
            if (instanceSecret != null) {
              ZooUtil.digestAuth(zk, instanceSecret);
            }
            tryAgain = false;
          } else {
            UtilWaitThread.sleep(TIME_BETWEEN_CONNECT_CHECKS_MS);
          }
        }

      } catch (IOException e) {
        if (e instanceof UnknownHostException) {
          /*
           * Make sure we wait at least as long as the JVM TTL for negative DNS responses
           */
          int ttl = AddressUtil.getAddressCacheNegativeTtl((UnknownHostException) e);
          sleepTime = Math.max(sleepTime, (ttl + 1) * 1000L);
        }
        log.warn("Connection to zooKeeper failed, will try again in "
            + String.format("%.2f secs", sleepTime / 1000.0), e);
      } finally {
        if (tryAgain && zk != null) {
          try {
            zk.close();
          } catch (InterruptedException e) {
            throw new AssertionError(
                "ZooKeeper.close() shouldn't throw this; it exists only for backwards compatibility",
                e);
          }
          zk = null;
        }
      }

      long stopTime = System.nanoTime();
      long duration = NANOSECONDS.toMillis(stopTime - startTime);

      if (duration > 2L * timeout) {
        throw new IllegalStateException("Failed to connect to zookeeper (" + connectString
            + ") within 2x zookeeper timeout period " + timeout);
      }

      if (tryAgain) {
        if (2L * timeout < duration + sleepTime + connectTimeWait) {
          sleepTime = 2L * timeout - duration - connectTimeWait;
        }
        if (sleepTime < 0) {
          connectTimeWait -= sleepTime;
          sleepTime = 0;
        }
        UtilWaitThread.sleep(sleepTime);
        if (sleepTime < 10000) {
          sleepTime = sleepTime + (long) (sleepTime * RANDOM.get().nextDouble());
        }
      }
    }

    return zk;
  }

  public static void close(ZooKeeper zk) {
    try {
      if (zk != null) {
        zk.close();
      }
    } catch (InterruptedException e) {
      throw new AssertionError(
          "ZooKeeper.close() shouldn't throw this; it exists only for backwards compatibility", e);
    }
  }

  /**
   * Given a zooCache and instanceId, look up the instance name.
   */
  public static String getInstanceName(ZooKeeper zk, InstanceId instanceId) {
    requireNonNull(zk);
    var instanceIdBytes = requireNonNull(instanceId).canonical().getBytes(UTF_8);
    for (String name : getInstanceNames(zk)) {
      var bytes = getInstanceIdBytesFromName(zk, name);
      if (Arrays.equals(bytes, instanceIdBytes)) {
        return name;
      }
    }
    return null;
  }

  private static List<String> getInstanceNames(ZooKeeper zk) {
    try {
      return new ZooReader(zk).getChildren(Constants.ZROOT + Constants.ZINSTANCES);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted reading instance names from ZooKeeper", e);
    } catch (KeeperException e) {
      throw new IllegalStateException("Failed to read instance names from ZooKeeper", e);
    }
  }

  private static byte[] getInstanceIdBytesFromName(ZooKeeper zk, String name) {
    try {
      return new ZooReader(zk)
          .getData(Constants.ZROOT + Constants.ZINSTANCES + "/" + requireNonNull(name));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(
          "Interrupted reading InstanceId from ZooKeeper for instance named " + name, e);
    } catch (KeeperException e) {
      log.warn("Failed to read InstanceId from ZooKeeper for instance named {}", name, e);
      return null;
    }
  }

  public static Map<String,InstanceId> getInstanceMap(ZooKeeper zk) {
    Map<String,InstanceId> idMap = new TreeMap<>();
    getInstanceNames(zk).forEach(name -> {
      byte[] instanceId = getInstanceIdBytesFromName(zk, name);
      if (instanceId != null) {
        idMap.put(name, InstanceId.of(new String(instanceId, UTF_8)));
      }
    });
    return idMap;
  }

  public static InstanceId getInstanceId(ZooKeeper zk, String name) {
    byte[] data = getInstanceIdBytesFromName(zk, name);
    if (data == null) {
      throw new IllegalStateException("Instance name " + name + " does not exist in ZooKeeper. "
          + "Run \"accumulo org.apache.accumulo.server.util.ListInstances\" to see a list.");
    }
    String instanceIdString = new String(data, UTF_8);
    try {
      // verify that the instanceId found via the name actually exists
      if (new ZooReader(zk).getData(Constants.ZROOT + "/" + instanceIdString) == null) {
        throw new IllegalStateException("InstanceId " + instanceIdString
            + " pointed to by the name " + name + " does not exist in ZooKeeper");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted verifying InstanceId " + instanceIdString
          + " pointed to by instance named " + name + " actually exists in ZooKeeper", e);
    } catch (KeeperException e) {
      throw new IllegalStateException("Failed to verify InstanceId " + instanceIdString
          + " pointed to by instance named " + name + " actually exists in ZooKeeper", e);
    }
    return InstanceId.of(instanceIdString);
  }

}
