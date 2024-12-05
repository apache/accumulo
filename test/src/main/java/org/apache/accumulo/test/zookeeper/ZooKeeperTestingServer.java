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
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.util.PortUtils;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.Watcher;
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

  /**
   * Instantiate a running zookeeper server - this call will block until the server is ready for
   * client connections. It will try three times, with a 5 second pause to connect.
   */
  public ZooKeeperTestingServer(final File tmpDir) {
    Preconditions.checkArgument(tmpDir.isDirectory());
    final int port = PortUtils.getRandomFreePort();
    try {
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
    } catch (Exception ex) {
      throw new IllegalStateException("Failed to start testing zookeeper", ex);
    }
  }

  @FunctionalInterface
  public interface ZooKeeperConstructor<T extends ZooKeeper> {
    public T construct(String connectString, int sessionTimeout, Watcher watcher)
        throws IOException;
  }

  /**
   * Create a new instance of a ZooKeeper client that is already connected to the testing server
   * using the provided constructor that accepts the connection string, the timeout, and a watcher
   * used by this class to wait for the client to connect. This can be used to construct a subclass
   * of the ZooKeeper client that implements non-standard behavior for a test.
   */
  public <T extends ZooKeeper> T newClient(ZooKeeperConstructor<T> f)
      throws IOException, InterruptedException {
    var connectionLatch = new CountDownLatch(1);
    var zoo = f.construct(zkServer.getConnectString(), 30_000, watchedEvent -> {
      if (watchedEvent.getState() == Watcher.Event.KeeperState.SyncConnected) {
        connectionLatch.countDown();
      }
    });
    connectionLatch.await();
    ZooUtil.digestAuth(zoo, SECRET);
    return zoo;
  }

  /**
   * Create a new instance of a standard ZooKeeper client that is already connected to the testing
   * server.
   */
  public ZooKeeper newClient() throws IOException, InterruptedException {
    return newClient(ZooKeeper::new);
  }

  public ZooReaderWriter getZooReaderWriter() {
    return new ZooReader(zkServer.getConnectString(), 30000).asWriter(SECRET);
  }

  @Override
  public void close() throws IOException {
    if (zkServer != null) {
      zkServer.stop();
    }
  }

}
