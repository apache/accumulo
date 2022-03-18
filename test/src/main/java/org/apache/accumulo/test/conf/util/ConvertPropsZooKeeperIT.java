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
package org.apache.accumulo.test.conf.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.Constants.ZCONFIG;
import static org.apache.accumulo.core.Constants.ZNAMESPACES;
import static org.apache.accumulo.core.Constants.ZNAMESPACE_CONF;
import static org.apache.accumulo.core.Constants.ZTABLES;
import static org.apache.accumulo.core.Constants.ZTABLE_CONF;
import static org.apache.accumulo.harness.AccumuloITBase.ZOOKEEPER_TESTING_SERVER;
import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;

import java.io.File;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.impl.ZooPropStore;
import org.apache.accumulo.server.conf.util.ConfigConverter;
import org.apache.accumulo.test.conf.store.PropStoreZooKeeperIT;
import org.apache.accumulo.test.zookeeper.ZooKeeperTestingServer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
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
public class ConvertPropsZooKeeperIT {

  private static final Logger log = LoggerFactory.getLogger(PropStoreZooKeeperIT.class);
  private static final InstanceId INSTANCE_ID = InstanceId.of(UUID.randomUUID());
  private static ZooKeeperTestingServer testZk = null;
  private static ZooKeeper zooKeeper;
  private static ZooReaderWriter zrw;
  private static ServerContext context;

  @TempDir
  private static File tempDir;

  @BeforeAll
  public static void setupZk() {

    // using default zookeeper port - we don't have a full configuration
    testZk = new ZooKeeperTestingServer(tempDir);
    zooKeeper = testZk.getZooKeeper();
    zooKeeper.addAuthInfo("digest", "accumulo:test".getBytes(UTF_8));

    zrw = testZk.getZooReaderWriter();
    testZk.initPaths(ZooUtil.getRoot(INSTANCE_ID));

    context = createNiceMock(ServerContext.class);
    expect(context.getZooReaderWriter()).andReturn(zrw).anyTimes();

    replay(context);
    ZooPropStore propStore = new ZooPropStore.Builder(context).build();
    reset(context);

    expect(context.getZooKeepersSessionTimeOut()).andReturn(zooKeeper.getSessionTimeout())
        .anyTimes();
    expect(context.getInstanceID()).andReturn(INSTANCE_ID).anyTimes();
    expect(context.getZooReaderWriter()).andReturn(zrw).anyTimes();
    expect(context.getPropStore()).andReturn(propStore).anyTimes();

  }

  @AfterAll
  public static void shutdownZK() throws Exception {
    testZk.close();
  }

  @BeforeEach
  public void setupZnodes() {
    try {
      zrw.putPersistentData(ZooUtil.getRoot(INSTANCE_ID) + Constants.ZCONFIG, new byte[0],
          ZooUtil.NodeExistsPolicy.SKIP);
    } catch (KeeperException ex) {
      log.trace("Issue during zk initialization, skipping", ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted during zookeeper path initialization", ex);
    }
  }

  @AfterEach
  public void cleanupZnodes() {
    try {
      ZKUtil.deleteRecursive(zooKeeper, "/accumulo");
    } catch (KeeperException | InterruptedException ex) {
      throw new IllegalStateException("Failed to clean-up test zooKeeper nodes.", ex);
    }
  }

  @Test
  public void convertAndDeleteTest() {

    replay(context);

    log.warn("CCCConext: {}", context.getInstanceID());

    FullZkConfigProps props = new FullZkConfigProps();
    props.populate();

    // namespaces [+accumulo, +default, 1]

    // tables [!0, +r, +rep, 2, 3]

    ConfigConverter upgrade = new ConfigConverter(context);
    upgrade.convertSys();
    upgrade.convertNamespace();
    upgrade.convertTables();

  }

  private static class FullZkConfigProps {

    private final String sysConfigRootPath =
        ZooUtil.getRoot(context.getInstanceID()) + ZCONFIG + "/";
    private final String nsConfigRootPath = ZooUtil.getRoot(context.getInstanceID()) + ZNAMESPACES;
    private final String tableConfigRootPath = ZooUtil.getRoot(context.getInstanceID()) + ZTABLES;

    private final String sysSplitThresholdVal = "123M";
    private final String sysGcPortVal = "19123";

    private final String nsSplitThresholdVal = "245M";

    public void populate() {
      populateSys();
      populateNamespace();
      populateTables();
    }

    private void populateSys() {
      try {

        zrw.putPersistentData(sysConfigRootPath + Property.TABLE_SPLIT_THRESHOLD.getKey(),
            sysSplitThresholdVal.getBytes(UTF_8), ZooUtil.NodeExistsPolicy.FAIL);

        zrw.putPersistentData(sysConfigRootPath + Property.GC_PORT.getKey(),
            sysGcPortVal.getBytes(UTF_8), ZooUtil.NodeExistsPolicy.FAIL);

        zrw.putPersistentData(sysConfigRootPath + "invalid.fake.property",
            "test_fake_value".getBytes(UTF_8), ZooUtil.NodeExistsPolicy.FAIL);

      } catch (KeeperException | InterruptedException ex) {
        throw new IllegalStateException("Failed to populate sys in full config in ZooKeeper", ex);
      }
    }

    // namespaces [+accumulo, +default, 1]
    private void populateNamespace() {
      try {

        zrw.putPersistentData(nsConfigRootPath, new byte[0], ZooUtil.NodeExistsPolicy.SKIP);

        // +accumulo
        zrw.putPersistentData(nsConfigRootPath + "/" + Namespace.ACCUMULO.id().canonical(),
            new byte[0], ZooUtil.NodeExistsPolicy.SKIP);
        zrw.putPersistentData(
            nsConfigRootPath + "/" + Namespace.ACCUMULO.id().canonical() + ZNAMESPACE_CONF,
            new byte[0], ZooUtil.NodeExistsPolicy.SKIP);

        // +default
        zrw.putPersistentData(nsConfigRootPath + "/" + Namespace.DEFAULT.id().canonical(),
            new byte[0], ZooUtil.NodeExistsPolicy.SKIP);
        zrw.putPersistentData(
            nsConfigRootPath + "/" + Namespace.DEFAULT.id().canonical() + ZNAMESPACE_CONF,
            new byte[0], ZooUtil.NodeExistsPolicy.SKIP);

        // ns 1
        var ns1 = NamespaceId.of("1");

        zrw.putPersistentData(nsConfigRootPath + "/" + ns1.canonical(), new byte[0],
            ZooUtil.NodeExistsPolicy.SKIP);
        zrw.putPersistentData(nsConfigRootPath + "/" + ns1.canonical() + ZNAMESPACE_CONF,
            new byte[0], ZooUtil.NodeExistsPolicy.SKIP);

        zrw.putPersistentData(nsConfigRootPath + Property.TABLE_SPLIT_THRESHOLD.getKey(),
            nsSplitThresholdVal.getBytes(UTF_8), ZooUtil.NodeExistsPolicy.SKIP);

      } catch (KeeperException | InterruptedException ex) {
        throw new IllegalStateException("Failed to populate namespaces in full config in ZooKeeper",
            ex);
      }

    }

    // tables [!0, +r, +rep, 2, 3]
    private void populateTables() {
      try {
        zrw.putPersistentData(tableConfigRootPath, new byte[0], ZooUtil.NodeExistsPolicy.SKIP);

        // !0
        zrw.putPersistentData(nsConfigRootPath + "/" + MetadataTable.ID.canonical(), new byte[0],
            ZooUtil.NodeExistsPolicy.SKIP);
        zrw.putPersistentData(nsConfigRootPath + "/" + MetadataTable.ID.canonical() + ZTABLE_CONF,
            new byte[0], ZooUtil.NodeExistsPolicy.SKIP);
        // +r
        // +rep
        // 2
        // 3

      } catch (KeeperException | InterruptedException ex) {
        throw new IllegalStateException("Failed to populate namespaces in full config in ZooKeeper",
            ex);
      }
    }
  }
}
