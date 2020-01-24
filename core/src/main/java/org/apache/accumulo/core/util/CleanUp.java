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
package org.apache.accumulo.core.util;

import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.singletons.SingletonManager;
import org.apache.accumulo.core.singletons.SingletonManager.Mode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connector uses static resource that create threads and use memory. The only way to clean up these
 * static resource related to Connectors created using ZookeeperInstance is to use this class.
 *
 * <p>
 * This class is not needed when only using {@link AccumuloClient}. The new AccumuloClient API that
 * replaces Connector is closable. For code that only uses AccumuloClient, when all AccumuloClients
 * are closed resources are cleaned up. Connectors that are derived from an AccumuloClient do not
 * necessitate the use of this code.
 *
 * @deprecated since 2.0.0 Use only {@link AccumuloClient} instead. Also, make sure you close the
 *             AccumuloClient instances.
 */
@Deprecated
public class CleanUp {

  private static final Logger log = LoggerFactory.getLogger(CleanUp.class);

  /**
   * kills all threads created by internal Accumulo singleton resources. After this method is
   * called, no Connector will work in the current classloader.
   */
  public static void shutdownNow() {
    SingletonManager.setMode(Mode.CLIENT);
    waitForZooKeeperClientThreads();
  }

  /**
   * As documented in https://issues.apache.org/jira/browse/ZOOKEEPER-1816, ZooKeeper.close() is a
   * non-blocking call. This method will wait on the ZooKeeper internal threads to exit.
   */
  private static void waitForZooKeeperClientThreads() {
    Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
    for (Thread thread : threadSet) {
      // find ZooKeeper threads that were created in the same ClassLoader as the current thread.
      if (thread.getClass().getName().startsWith("org.apache.zookeeper.ClientCnxn") && thread
          .getContextClassLoader().equals(Thread.currentThread().getContextClassLoader())) {

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
