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

import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.accumulo.server.util.PortUtils;
import org.apache.curator.test.TestingServer;
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
  public interface ZooSessionConstructor<T extends ZooSession> {
    public T construct(String clientName, String connectString, int timeout, String instanceSecret)
        throws IOException;
  }

  /**
   * Create a new instance of a ZooKeeper client that is already connected to the testing server
   * using the provided constructor that accepts the connection string (appended with the provided
   * root path), the timeout, and a watcher used by this class to wait for the client to connect.
   * This can be used to construct a subclass of the ZooKeeper client that implements non-standard
   * behavior for a test.
   */
  public <T extends ZooSession> T newClient(ZooSessionConstructor<T> f, String root)
      throws IOException, InterruptedException {
    return f.construct(ZooKeeperTestingServer.class.getSimpleName(),
        zkServer.getConnectString() + root, 30_000, SECRET);
  }

  /**
   * Create a new instance of a ZooKeeper client that is already connected to the testing server
   * using the provided constructor that accepts the connection string, the timeout, and a watcher
   * used by this class to wait for the client to connect. This can be used to construct a subclass
   * of the ZooKeeper client that implements non-standard behavior for a test.
   */
  public <T extends ZooSession> T newClient(ZooSessionConstructor<T> f)
      throws IOException, InterruptedException {
    return newClient(f, "");
  }

  /**
   * Create a new instance of a standard ZooKeeper client that is already connected to the testing
   * server. The caller is responsible for closing the object.
   */
  public ZooSession newClient() throws IOException, InterruptedException {
    return newClient(ZooSession::new);
  }

  /**
   * Create a new instance of a standard ZooKeeper client that is already connected to the testing
   * server and "chroot-ed" to the provided root path. The caller is responsible for closing the
   * object.
   */
  public ZooSession newClient(String root) throws IOException, InterruptedException {
    return newClient(ZooSession::new, root);
  }

  @Override
  public void close() throws IOException {
    if (zkServer != null) {
      zkServer.stop();
    }
  }

}
