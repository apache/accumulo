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
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.io.IOException;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.singletons.SingletonManager;
import org.apache.accumulo.core.singletons.SingletonService;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooSession {

  public static class ZooSessionShutdownException extends RuntimeException {

    public ZooSessionShutdownException(String msg) {
      super(msg);
    }

    private static final long serialVersionUID = 1L;

  }

  private static final Logger log = LoggerFactory.getLogger(ZooSession.class);

  private static class ZooSessionInfo {
    public ZooSessionInfo(ZooKeeper zooKeeper) {
      this.zooKeeper = zooKeeper;
    }

    ZooKeeper zooKeeper;
  }

  private static Map<String,ZooSessionInfo> sessions = new HashMap<>();

  private static final SecureRandom random = new SecureRandom();

  static {
    SingletonManager.register(new SingletonService() {

      @Override
      public boolean isEnabled() {
        return ZooSession.isEnabled();
      }

      @Override
      public void enable() {
        ZooSession.enable();
      }

      @Override
      public void disable() {
        ZooSession.disable();
      }
    });
  }

  private static String sessionKey(String keepers, int timeout, String scheme, byte[] auth) {
    return keepers + ":" + timeout + ":" + (scheme == null ? "" : scheme) + ":"
        + (auth == null ? "" : new String(auth, UTF_8));
  }

  private static class ZooWatcher implements Watcher {

    @Override
    public void process(WatchedEvent event) {
      if (event.getState() == KeeperState.Expired) {
        log.debug("Session expired; {}", event);
      }
    }

  }

  /**
   * @param host comma separated list of zk servers
   * @param timeout in milliseconds
   * @param scheme authentication type, e.g. 'digest', may be null
   * @param auth authentication-scheme-specific token, may be null
   * @param watcher ZK notifications, may be null
   */
  static ZooKeeper connect(String host, int timeout, String scheme, byte[] auth, Watcher watcher) {
    final int TIME_BETWEEN_CONNECT_CHECKS_MS = 100;
    int connectTimeWait = Math.min(10_000, timeout);
    boolean tryAgain = true;
    long sleepTime = 100;
    ZooKeeper zooKeeper = null;

    long startTime = System.nanoTime();

    while (tryAgain) {
      try {
        zooKeeper = new ZooKeeper(host, timeout, watcher);
        // it may take some time to get connected to zookeeper if some of the servers are down
        for (int i = 0; i < connectTimeWait / TIME_BETWEEN_CONNECT_CHECKS_MS && tryAgain; i++) {
          if (zooKeeper.getState().equals(States.CONNECTED)) {
            if (auth != null) {
              ZooUtil.auth(zooKeeper, scheme, auth);
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
        if (tryAgain && zooKeeper != null) {
          try {
            zooKeeper.close();
            zooKeeper = null;
          } catch (InterruptedException e) {
            log.warn("interrupted", e);
          }
        }
      }

      long stopTime = System.nanoTime();
      long duration = NANOSECONDS.toMillis(stopTime - startTime);

      if (duration > 2L * timeout) {
        throw new RuntimeException("Failed to connect to zookeeper (" + host
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
          sleepTime = sleepTime + (long) (sleepTime * random.nextDouble());
        }
      }
    }

    return zooKeeper;
  }

  public static ZooKeeper getAuthenticatedSession(String zooKeepers, int timeout, String scheme,
      byte[] auth) {
    return getSession(zooKeepers, timeout, scheme, auth);
  }

  public static ZooKeeper getAnonymousSession(String zooKeepers, int timeout) {
    return getSession(zooKeepers, timeout, null, null);
  }

  private static synchronized ZooKeeper getSession(String zooKeepers, int timeout, String scheme,
      byte[] auth) {

    if (sessions == null) {
      throw new ZooSessionShutdownException(
          "The Accumulo singleton that that tracks zookeeper session is disabled.  This is likely "
              + "caused by all AccumuloClients being closed or garbage collected.");
    }

    String sessionKey = sessionKey(zooKeepers, timeout, scheme, auth);

    // a read-only session can use a session with authorizations, so cache a copy for it w/out auths
    String readOnlySessionKey = sessionKey(zooKeepers, timeout, null, null);
    ZooSessionInfo zsi = sessions.get(sessionKey);
    if (zsi != null && zsi.zooKeeper.getState() == States.CLOSED) {
      log.debug("Removing closed ZooKeeper session to {}", zooKeepers);
      if (auth != null && sessions.get(readOnlySessionKey) == zsi) {
        sessions.remove(readOnlySessionKey);
      }
      zsi = null;
      sessions.remove(sessionKey);
    }

    if (zsi == null) {
      ZooWatcher watcher = new ZooWatcher();
      log.debug("Connecting to {} with timeout {} with auth", zooKeepers, timeout);
      zsi = new ZooSessionInfo(connect(zooKeepers, timeout, scheme, auth, watcher));
      sessions.put(sessionKey, zsi);
      if (auth != null && !sessions.containsKey(readOnlySessionKey)) {
        sessions.put(readOnlySessionKey, zsi);
      }
    }
    return zsi.zooKeeper;
  }

  private static synchronized boolean isEnabled() {
    return sessions != null;
  }

  private static synchronized void enable() {
    if (sessions != null) {
      return;
    }

    sessions = new HashMap<>();
  }

  private static synchronized void disable() {
    if (sessions == null) {
      return;
    }

    for (ZooSessionInfo zsi : sessions.values()) {
      try {
        zsi.zooKeeper.close();
      } catch (Exception e) {
        log.debug("Error closing zookeeper during shutdown", e);
      }
    }

    sessions = null;
  }
}
