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
package org.apache.accumulo.core.zookeeper;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.accumulo.core.util.LazySingletons.RANDOM;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.core.util.Timer;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.Watcher.WatcherType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * A ZooKeeper client facade that maintains a ZooKeeper delegate instance. If the delegate instance
 * loses its session, it is replaced with a new instance to establish a new session. Any Watchers
 * registered on a session will need to monitor for the session expired event triggered from the old
 * delegate, and must be reset on the new session if you intend them to monitor any further events.
 * That is no different than if you created a new ZooKeeper instance directly after the first one
 * expired.
 */
public class ZooSession implements AutoCloseable {

  public static class ZKUtil {
    public static void deleteRecursive(ZooSession zk, final String pathRoot)
        throws InterruptedException, KeeperException {
      org.apache.zookeeper.ZKUtil.deleteRecursive(zk.verifyConnected(), pathRoot);
    }

    public static void visitSubTreeDFS(ZooSession zk, final String path, boolean watch,
        StringCallback cb) throws KeeperException, InterruptedException {
      org.apache.zookeeper.ZKUtil.visitSubTreeDFS(zk.verifyConnected(), path, watch, cb);
    }
  }

  private static class ZooSessionWatcher implements Watcher {

    private final String connectionName;
    private final AtomicReference<KeeperState> lastState = new AtomicReference<>(null);

    public ZooSessionWatcher(String connectionName) {
      this.connectionName = connectionName;
    }

    @Override
    public void process(WatchedEvent event) {
      final var newState = event.getState();
      var oldState = lastState.getAndUpdate(s -> newState);
      if (oldState == null) {
        log.debug("{} state changed to {}", connectionName, newState);
      } else if (newState != oldState) {
        log.debug("{} state changed from {} to {}", connectionName, oldState, newState);
      }
    }
  }

  private static final Logger log = LoggerFactory.getLogger(ZooSession.class);

  private static void closeZk(ZooKeeper zk) {
    if (zk != null) {
      try {
        zk.close();
      } catch (InterruptedException e) {
        // ZooKeeper doesn't actually throw this; it's just there for backwards compatibility
        Thread.currentThread().interrupt();
      }
    }
  }

  private static void digestAuth(ZooKeeper zoo, String secret) {
    zoo.addAuthInfo("digest", ("accumulo:" + requireNonNull(secret)).getBytes(UTF_8));
  }

  private static class ZookeeperAndCounter {
    final ZooKeeper zookeeper;
    final long connectionCount;

    private ZookeeperAndCounter(ZooKeeper zookeeper, long connectionCount) {
      Preconditions.checkArgument(connectionCount >= 0);
      this.zookeeper = Objects.requireNonNull(zookeeper);
      this.connectionCount = connectionCount;
    }
  }

  private boolean closed = false;
  private final String connectString;
  private final AtomicReference<ZookeeperAndCounter> delegate = new AtomicReference<>();
  private final String instanceSecret;
  private final String sessionName;
  private final int timeout;
  private final ZooReaderWriter zrw;

  /**
   * Construct a new ZooKeeper client, retrying indefinitely if it doesn't work right away. The
   * caller is responsible for closing instances returned from this method.
   *
   * @param clientName a convenient name for logging its connection state changes
   * @param conf a convenient carrier of ZK connection information using Accumulo properties
   */
  public ZooSession(String clientName, AccumuloConfiguration conf) {
    this(clientName, conf.get(Property.INSTANCE_ZK_HOST),
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
  public ZooSession(String clientName, String connectString, int timeout, String instanceSecret) {
    // information needed to construct a ZooKeeper instance and add authentication
    this.connectString = connectString;
    this.timeout = timeout;
    this.instanceSecret = instanceSecret;

    // information for logging which instance of ZooSession this is
    this.sessionName =
        String.format("%s[%s_%s]", getClass().getSimpleName(), clientName, UUID.randomUUID());
    this.zrw = new ZooReaderWriter(this);
  }

  private ZooKeeper verifyConnected() {
    var zkac = delegate.get();
    if (zkac != null && zkac.zookeeper.getState().isAlive()) {
      return zkac.zookeeper;
    } else {
      return reconnect().zookeeper;
    }
  }

  private synchronized ZookeeperAndCounter reconnect() {
    if (closed) {
      throw new IllegalStateException(sessionName + " was closed");
    }

    ZookeeperAndCounter zkac;
    if ((zkac = delegate.get()) != null && zkac.zookeeper.getState().isAlive()) {
      return zkac;
    }

    final long nextCounter = (zkac == null ? 0 : zkac.connectionCount) + 1;
    zkac = null;

    var reconnectName = String.format("%s#%s", sessionName, nextCounter);
    log.debug("{} (re-)connecting to {} with timeout {}{}", reconnectName, connectString, timeout,
        instanceSecret == null ? "" : " with auth");
    final int TIME_BETWEEN_CONNECT_CHECKS_MS = 100;
    int connectTimeWait = Math.min(10_000, timeout);
    boolean tryAgain = true;
    long sleepTime = 100;

    Timer timer = Timer.startNew();

    ZooKeeper zk = null;

    while (tryAgain) {
      try {
        zk = new ZooKeeper(connectString, timeout, new ZooSessionWatcher(reconnectName));
        // it may take some time to get connected to zookeeper if some of the servers are down
        for (int i = 0; i < connectTimeWait / TIME_BETWEEN_CONNECT_CHECKS_MS && tryAgain; i++) {
          if (zk.getState().isConnected()) {
            if (instanceSecret != null) {
              digestAuth(zk, instanceSecret);
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
          closeZk(zk);
          zk = null;
        }
      }

      long duration = timer.elapsed(MILLISECONDS);

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

    zkac = new ZookeeperAndCounter(zk, nextCounter);
    delegate.set(zkac);
    return zkac;
  }

  public void addAuthInfo(String scheme, byte[] auth) {
    verifyConnected().addAuthInfo(scheme, auth);
  }

  public String create(final String path, byte[] data, List<ACL> acl, CreateMode createMode)
      throws KeeperException, InterruptedException {
    return verifyConnected().create(path, data, acl, createMode);
  }

  public void delete(final String path, int version) throws InterruptedException, KeeperException {
    verifyConnected().delete(path, version);
  }

  public Stat exists(final String path, Watcher watcher)
      throws KeeperException, InterruptedException {
    return verifyConnected().exists(path, watcher);
  }

  public List<ACL> getACL(final String path, Stat stat)
      throws KeeperException, InterruptedException {
    return verifyConnected().getACL(path, stat);
  }

  public List<String> getChildren(final String path, Watcher watcher)
      throws KeeperException, InterruptedException {
    return verifyConnected().getChildren(path, watcher);
  }

  public byte[] getData(final String path, Watcher watcher, Stat stat)
      throws KeeperException, InterruptedException {
    return verifyConnected().getData(path, watcher, stat);
  }

  public long getSessionId() {
    return verifyConnected().getSessionId();
  }

  public int getSessionTimeout() {
    return verifyConnected().getSessionTimeout();
  }

  public void removeWatches(String path, Watcher watcher, WatcherType watcherType, boolean local)
      throws InterruptedException, KeeperException {
    verifyConnected().removeWatches(path, watcher, watcherType, local);
  }

  public Stat setData(final String path, byte[] data, int version)
      throws KeeperException, InterruptedException {
    return verifyConnected().setData(path, data, version);
  }

  public void sync(final String path, VoidCallback cb, Object ctx) {
    verifyConnected().sync(path, cb, ctx);
  }

  public long addPersistentRecursiveWatchers(Set<String> paths, Watcher watcher)
      throws KeeperException, InterruptedException {
    ZookeeperAndCounter localZkac = reconnect();

    Set<String> remainingPaths = new HashSet<>(paths);
    while (true) {
      try {
        Iterator<String> remainingPathsIter = remainingPaths.iterator();
        while (remainingPathsIter.hasNext()) {
          String path = remainingPathsIter.next();
          localZkac.zookeeper.addWatch(path, watcher, AddWatchMode.PERSISTENT_RECURSIVE);
          remainingPathsIter.remove();
        }

        return localZkac.connectionCount;
      } catch (KeeperException e) {
        log.error("Error setting persistent watcher in ZooKeeper, retrying...", e);
        ZookeeperAndCounter currentZkac = reconnect();
        // If ZooKeeper object is different, then reset the localZK variable
        // and start over.
        if (localZkac != currentZkac) {
          localZkac = currentZkac;
          remainingPaths = new HashSet<>(paths);
        }
      }
    }
  }

  @Override
  public synchronized void close() {
    if (!closed) {
      var zkac = delegate.getAndSet(null);
      if (zkac != null) {
        closeZk(zkac.zookeeper);
      }
      closed = true;
    }
  }

  public void addAccumuloDigestAuth(String auth) {
    digestAuth(verifyConnected(), auth);
  }

  public ZooReader asReader() {
    return zrw;
  }

  public ZooReaderWriter asReaderWriter() {
    return zrw;
  }

  /**
   * Connection counter is incremented internal when ZooSession creates a new ZooKeeper client.
   * Clients of ZooSession can use this counter as a way to determine if a new ZooKeeper connection
   * has been created.
   *
   * @return connection counter
   */
  public long getConnectionCounter() {
    var zkac = delegate.get();
    if (delegate.get() == null) {
      // If null then this is closed or in the process of opening. If closed reconnect will throw an
      // exception. If in the process of opening, then reconnect will wait for that to finish.
      return reconnect().connectionCount;
    }
    return zkac.connectionCount;
  }

}
