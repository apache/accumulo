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
package org.apache.accumulo.master.metrics.fate;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperTestingServer {

  private static final Logger log = LoggerFactory.getLogger(ZookeeperTestingServer.class);

  private TestingServer zkServer;
  private final ZooKeeper zoo;

  public ZookeeperTestingServer() {

    try {

      Path tmpDir = Files.createTempDirectory("zk_test");

      CountDownLatch connectionLatch = new CountDownLatch(1);

      // using a random port. The test server allows for auto port
      // generation, but not with specifying the tmp dir path too.
      // so, generate our own.
      boolean started = false;
      int retry = 0;
      while (!started && retry++ < 3) {

        try {

          zkServer = new TestingServer(getPort(), tmpDir.toFile());
          zkServer.start();

          started = true;
        } catch (Exception ex) {
          log.trace("zookeeper test server start failed attempt {}", retry);
        }
      }

      log.info("c:{}", zkServer.getConnectString());

      zoo = new ZooKeeper(zkServer.getConnectString(), 5_000, new Watcher() {
        public void process(WatchedEvent we) {
          if (we.getState() == Watcher.Event.KeeperState.SyncConnected) {
            connectionLatch.countDown();
          }
        }
      });

      connectionLatch.await();

    } catch (Exception ex) {
      throw new IllegalStateException("Failed to start testing zookeeper", ex);
    }

  }

  /**
   * Returns an random integer between 50_000 and 65_000 (typical ephemeral port range for linux is
   * listed as 49,152 to 65,535
   *
   * @return a random port with the linux ephemeral port range.
   */
  private int getPort() {
    final int minPort = 50_000;
    final int maxPort = 65_000;
    Random r = new Random();
    return r.nextInt((maxPort - minPort) + 1) + minPort;
  }

  public ZooKeeper getZooKeeper() {
    return zoo;
  }

  public String getConn() {
    return zkServer.getConnectString();
  }

  public void initPaths(String s) {
    try {

      String[] paths = s.split("/");

      String slash = "/";
      String path = "";

      for (String p : paths) {
        if (p.length() > 0) {
          path = path + slash + p;
          log.debug("building default paths, creating node {}", path);
          zoo.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
      }

    } catch (Exception ex) {
      throw new IllegalStateException("Failed to create accumulo initial paths: " + s, ex);
    }
  }

  public void close() throws IOException {
    if (zkServer != null) {
      zkServer.stop();
    }
  }

}
