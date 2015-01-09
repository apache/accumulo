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
package org.apache.accumulo.fate.zookeeper;

import static com.google.common.base.Charsets.UTF_8;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.fate.util.AddressUtil;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;

public class ZooSession {

  public static class ZooSessionShutdownException extends RuntimeException {

    private static final long serialVersionUID = 1L;

  }

  private static final Logger log = Logger.getLogger(ZooSession.class);

  private static class ZooSessionInfo {
    public ZooSessionInfo(ZooKeeper zooKeeper, ZooWatcher watcher) {
      this.zooKeeper = zooKeeper;
    }

    ZooKeeper zooKeeper;
  }

  private static Map<String,ZooSessionInfo> sessions = new HashMap<String,ZooSessionInfo>();

  private static String sessionKey(String keepers, int timeout, String scheme, byte[] auth) {
    return keepers + ":" + timeout + ":" + (scheme == null ? "" : scheme) + ":" + (auth == null ? "" : new String(auth, UTF_8));
  }

  private static class ZooWatcher implements Watcher {

    @Override
    public void process(WatchedEvent event) {
      if (event.getState() == KeeperState.Expired) {
        log.debug("Session expired, state of current session : " + event.getState());
      }
    }

  }

  /**
   * @param host
   *          comma separated list of zk servers
   * @param timeout
   *          in milliseconds
   * @param scheme
   *          authentication type, e.g. 'digest', may be null
   * @param auth
   *          authentication-scheme-specific token, may be null
   * @param watcher
   *          ZK notifications, may be null
   */
  public static ZooKeeper connect(String host, int timeout, String scheme, byte[] auth, Watcher watcher) {
    final int TIME_BETWEEN_CONNECT_CHECKS_MS = 100;
    int connectTimeWait = Math.min(10 * 1000, timeout);
    boolean tryAgain = true;
    long sleepTime = 100;
    ZooKeeper zooKeeper = null;

    long startTime = System.currentTimeMillis();

    while (tryAgain) {
      try {
        zooKeeper = new ZooKeeper(host, timeout, watcher);
        // it may take some time to get connected to zookeeper if some of the servers are down
        for (int i = 0; i < connectTimeWait / TIME_BETWEEN_CONNECT_CHECKS_MS && tryAgain; i++) {
          if (zooKeeper.getState().equals(States.CONNECTED)) {
            if (auth != null)
              zooKeeper.addAuthInfo(scheme, auth);
            tryAgain = false;
          } else
            UtilWaitThread.sleep(TIME_BETWEEN_CONNECT_CHECKS_MS);
        }

      } catch (IOException e) {
        if (e instanceof UnknownHostException) {
          /*
           * Make sure we wait atleast as long as the JVM TTL for negative DNS responses
           */
          sleepTime = Math.max(sleepTime, (AddressUtil.getAddressCacheNegativeTtl((UnknownHostException) e) + 1) * 1000);
        }
        log.warn("Connection to zooKeeper failed, will try again in " + String.format("%.2f secs", sleepTime / 1000.0), e);
      } finally {
        if (tryAgain && zooKeeper != null)
          try {
            zooKeeper.close();
            zooKeeper = null;
          } catch (InterruptedException e) {
            log.warn("interrupted", e);
          }
      }

      if (System.currentTimeMillis() - startTime > 2 * timeout) {
        throw new RuntimeException("Failed to connect to zookeeper (" + host + ") within 2x zookeeper timeout period " + timeout);
      }

      if (tryAgain) {
        if (startTime + 2 * timeout < System.currentTimeMillis() + sleepTime + connectTimeWait)
          sleepTime = startTime + 2 * timeout - System.currentTimeMillis() - connectTimeWait;
        if (sleepTime < 0) {
          connectTimeWait -= sleepTime;
          sleepTime = 0;
        }
        UtilWaitThread.sleep(sleepTime);
        if (sleepTime < 10000)
          sleepTime = sleepTime + (long) (sleepTime * Math.random());
      }
    }

    return zooKeeper;
  }

  public static synchronized ZooKeeper getSession(String zooKeepers, int timeout) {
    return getSession(zooKeepers, timeout, null, null);
  }

  public static synchronized ZooKeeper getSession(String zooKeepers, int timeout, String scheme, byte[] auth) {

    if (sessions == null)
      throw new ZooSessionShutdownException();

    String sessionKey = sessionKey(zooKeepers, timeout, scheme, auth);

    // a read-only session can use a session with authorizations, so cache a copy for it w/out auths
    String readOnlySessionKey = sessionKey(zooKeepers, timeout, null, null);
    ZooSessionInfo zsi = sessions.get(sessionKey);
    if (zsi != null && zsi.zooKeeper.getState() == States.CLOSED) {
      log.debug("Removing closed ZooKeeper session to " + zooKeepers);
      if (auth != null && sessions.get(readOnlySessionKey) == zsi)
        sessions.remove(readOnlySessionKey);
      zsi = null;
      sessions.remove(sessionKey);
    }

    if (zsi == null) {
      ZooWatcher watcher = new ZooWatcher();
      log.debug("Connecting to " + zooKeepers + " with timeout " + timeout + " with auth");
      zsi = new ZooSessionInfo(connect(zooKeepers, timeout, scheme, auth, watcher), watcher);
      sessions.put(sessionKey, zsi);
      if (auth != null && !sessions.containsKey(readOnlySessionKey))
        sessions.put(readOnlySessionKey, zsi);
    }
    return zsi.zooKeeper;
  }

  public static synchronized void shutdown() {
    if (sessions == null)
      return;

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
