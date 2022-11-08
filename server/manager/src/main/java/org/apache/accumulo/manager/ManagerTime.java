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
package org.apache.accumulo.manager;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.util.threads.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keep a persistent roughly monotone view of how long a manager has been overseeing this cluster.
 */
public class ManagerTime {
  private static final Logger log = LoggerFactory.getLogger(ManagerTime.class);

  private final String zPath;
  private final ZooReaderWriter zk;
  private final Manager manager;

  /**
   * Difference between time stored in ZooKeeper and System.nanoTime() when we last read from
   * ZooKeeper.
   */
  private final AtomicLong skewAmount;

  public ManagerTime(Manager manager, AccumuloConfiguration conf) throws IOException {
    this.zPath = manager.getZooKeeperRoot() + Constants.ZMANAGER_TICK;
    this.zk = manager.getContext().getZooReaderWriter();
    this.manager = manager;

    try {
      zk.putPersistentData(zPath, "0".getBytes(UTF_8), NodeExistsPolicy.SKIP);
      skewAmount =
          new AtomicLong(Long.parseLong(new String(zk.getData(zPath), UTF_8)) - System.nanoTime());
    } catch (Exception ex) {
      throw new IOException("Error updating manager time", ex);
    }

    ThreadPools.watchCriticalScheduledTask(
        ThreadPools.getServerThreadPools().createGeneralScheduledExecutorService(conf)
            .scheduleWithFixedDelay(Threads.createNamedRunnable("Manager time keeper", () -> run()),
                0, SECONDS.toMillis(10), MILLISECONDS));
  }

  /**
   * How long has this cluster had a Manager?
   *
   * @return Approximate total duration this cluster has had a Manager, in milliseconds.
   */
  public long getTime() {
    return NANOSECONDS.toMillis(System.nanoTime() + skewAmount.get());
  }

  public void run() {
    switch (manager.getManagerState()) {
      // If we don't have the lock, periodically re-read the value in ZooKeeper, in case there's
      // another manager we're
      // shadowing for.
      case INITIAL:
      case STOP:
        try {
          long zkTime = Long.parseLong(new String(zk.getData(zPath), UTF_8));
          skewAmount.set(zkTime - System.nanoTime());
        } catch (Exception ex) {
          if (log.isDebugEnabled()) {
            log.debug("Failed to retrieve manager tick time", ex);
          }
        }
        break;
      // If we do have the lock, periodically write our clock to ZooKeeper.
      case HAVE_LOCK:
      case SAFE_MODE:
      case NORMAL:
      case UNLOAD_METADATA_TABLETS:
      case UNLOAD_ROOT_TABLET:
        try {
          zk.putPersistentData(zPath,
              Long.toString(System.nanoTime() + skewAmount.get()).getBytes(UTF_8),
              NodeExistsPolicy.OVERWRITE);
        } catch (Exception ex) {
          if (log.isDebugEnabled()) {
            log.debug("Failed to update manager tick time", ex);
          }
        }
    }
  }
}
