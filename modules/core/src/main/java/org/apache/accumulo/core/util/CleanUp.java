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
package org.apache.accumulo.core.util;

import java.util.Set;

import org.apache.accumulo.core.client.impl.ThriftTransportPool;
import org.apache.accumulo.fate.zookeeper.ZooSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class CleanUp {

  private static final Logger log = LoggerFactory.getLogger(CleanUp.class);

  /**
   * kills all threads created by internal Accumulo singleton resources. After this method is called, no accumulo client will work in the current classloader.
   */
  public static void shutdownNow() {
    ThriftTransportPool.getInstance().shutdown();
    ZooSession.shutdown();
    waitForZooKeeperClientThreads();
  }

  /**
   * As documented in https://issues.apache.org/jira/browse/ZOOKEEPER-1816, ZooKeeper.close() is a non-blocking call. This method will wait on the ZooKeeper
   * internal threads to exit.
   */
  private static void waitForZooKeeperClientThreads() {
    Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
    for (Thread thread : threadSet) {
      // find ZooKeeper threads that were created in the same ClassLoader as the current thread.
      if (thread.getClass().getName().startsWith("org.apache.zookeeper.ClientCnxn")
          && thread.getContextClassLoader().equals(Thread.currentThread().getContextClassLoader())) {

        // wait for the thread the die
        while (thread.isAlive()) {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            log.error("{}", e.getMessage(), e);
          }
        }
      }
    }
  }
}
