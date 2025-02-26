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
package org.apache.accumulo.test.conf.store;

import static org.apache.accumulo.core.conf.Property.GC_PORT;
import static org.apache.accumulo.core.conf.Property.MANAGER_CLIENTPORT;
import static org.apache.accumulo.core.conf.Property.TSERV_CLIENTPORT;
import static org.apache.accumulo.core.conf.Property.TSERV_NATIVEMAP_ENABLED;
import static org.apache.accumulo.core.conf.Property.TSERV_SCAN_MAX_OPENFILES;
import static org.apache.accumulo.harness.AccumuloITBase.ZOOKEEPER_TESTING_SERVER;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.accumulo.core.zookeeper.ZooSession.ZKUtil;
import org.apache.accumulo.server.conf.codec.VersionedPropCodec;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.accumulo.server.conf.store.impl.PropCacheCaffeineImpl;
import org.apache.accumulo.server.conf.store.impl.PropStoreWatcher;
import org.apache.accumulo.server.conf.store.impl.ReadyMonitor;
import org.apache.accumulo.server.conf.store.impl.ZooPropLoader;
import org.apache.accumulo.test.zookeeper.ZooKeeperTestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(ZOOKEEPER_TESTING_SERVER)
public class PropCacheCaffeineImplZkIT {

  private static final Logger log = LoggerFactory.getLogger(PropCacheCaffeineImplZkIT.class);

  private static ZooKeeperTestingServer testZk = null;
  private static ZooReaderWriter zrw;
  private static ZooSession zk;

  private final TableId tIdA = TableId.of("A");
  private final TableId tIdB = TableId.of("B");

  @TempDir
  private static File tempDir;

  @BeforeAll
  public static void setupZk() throws Exception {
    testZk = new ZooKeeperTestingServer(tempDir);
    zk = testZk.newClient();
    zrw = zk.asReaderWriter();
  }

  @AfterAll
  public static void shutdownZK() throws Exception {
    try {
      zk.close();
    } finally {
      testZk.close();
    }
  }

  @BeforeEach
  public void setupZnodes() throws Exception {
    zrw.mkdirs(Constants.ZCONFIG);
    zk.create(Constants.ZTABLES, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(Constants.ZTABLES + "/" + tIdA.canonical(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);
    zk.create(Constants.ZTABLES + "/" + tIdA.canonical() + "/conf", new byte[0],
        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

    zk.create(Constants.ZTABLES + "/" + tIdB.canonical(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);
    zk.create(Constants.ZTABLES + "/" + tIdB.canonical() + "/conf", new byte[0],
        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
  }

  @AfterEach
  public void cleanupZnodes() throws Exception {
    ZKUtil.deleteRecursive(zk, "/");
  }

  @Test
  public void init() throws Exception {
    Map<String,String> props = new HashMap<>();
    props.put(TSERV_CLIENTPORT.getKey(), "1234");
    props.put(TSERV_NATIVEMAP_ENABLED.getKey(), "false");
    props.put(TSERV_SCAN_MAX_OPENFILES.getKey(), "2345");
    props.put(MANAGER_CLIENTPORT.getKey(), "3456");
    props.put(GC_PORT.getKey(), "4567");
    VersionedProperties vProps = new VersionedProperties(props);

    // directly create prop node - simulate existing properties.
    var propStoreKey = TablePropKey.of(tIdA);
    var created = zrw.putPersistentData(propStoreKey.getPath(),
        VersionedPropCodec.getDefault().toBytes(vProps), ZooUtil.NodeExistsPolicy.FAIL);

    assertTrue(created, "expected properties to be created");

    ReadyMonitor readyMonitor = new ReadyMonitor("test", zk.getSessionTimeout());

    PropStoreWatcher propStoreWatcher = new PropStoreWatcher(readyMonitor);

    var propLoader = new ZooPropLoader(zk, VersionedPropCodec.getDefault(), propStoreWatcher);
    PropCacheCaffeineImpl cache = new PropCacheCaffeineImpl.Builder(propLoader).build();

    VersionedProperties readProps = cache.get(propStoreKey);

    if (readProps == null) {
      fail("Received null for versioned properties");
    } else {
      log.debug("Props read from cache: {}", readProps.print(true));
    }

  }
}
