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
package org.apache.accumulo.test.zookeeper;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.server.util.PortUtils;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Uses Apache Curator to create a running zookeeper server for internal tests. The zookeeper port
 * is randomly assigned in case multiple instances are created by concurrent tests.
 */
public class ZooKeeperTestingServer implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(ZooKeeperTestingServer.class);
  public static final String SECRET = "secret";

  private TestingServer zkServer;
  private final ZooKeeper zoo;

  /**
   * Instantiate a running zookeeper server - this call will block until the server is ready for
   * client connections. It will try three times, with a 5 second pause to connect.
   */
  public ZooKeeperTestingServer(File tmpDir) {
    this(tmpDir, PortUtils.getRandomFreePort());
  }

  private ZooKeeperTestingServer(File tmpDir, int port) {

    Preconditions.checkArgument(tmpDir.isDirectory());

    try {

      CountDownLatch connectionLatch = new CountDownLatch(1);

      // using a random port. The test server allows for auto port
      // generation, but not with specifying the tmp dir path too.
      // so, generate our own.
      boolean started = false;
      int retry = 0;
      while (!started && retry++ < 3) {

        try {

          zkServer = new TestingServer(port, tmpDir);
          zkServer.start();

          started = true;
        } catch (Exception ex) {
          log.trace("zookeeper test server start failed attempt {}", retry);
        }
      }

      log.info("zookeeper connection string:'{}'", zkServer.getConnectString());

      zoo = new ZooKeeper(zkServer.getConnectString(), 5_000, watchedEvent -> {
        if (watchedEvent.getState() == Watcher.Event.KeeperState.SyncConnected) {
          connectionLatch.countDown();
        }
      });

      connectionLatch.await();

    } catch (Exception ex) {
      throw new IllegalStateException("Failed to start testing zookeeper", ex);
    }

  }

  public ZooKeeper getZooKeeper() {
    return zoo;
  }

  public ZooReaderWriter getZooReaderWriter() {
    return new ZooReader(getConn(), 30000).asWriter(SECRET);
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
        if (!p.isEmpty()) {
          path = path + slash + p;
          log.debug("building default paths, creating node {}", path);
          zoo.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
      }

    } catch (Exception ex) {
      throw new IllegalStateException("Failed to create accumulo initial paths: " + s, ex);
    }
  }

  @Override
  public void close() throws IOException {
    if (zkServer != null) {
      zkServer.stop();
    }
  }

}
