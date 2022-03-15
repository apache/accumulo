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
package org.apache.accumulo.server.conf.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.DeprecatedPropertyUtil;
import org.apache.accumulo.core.data.AbstractId;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.PropCacheId;
import org.apache.accumulo.server.conf.store.impl.ZooPropStore;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

public class ConfigConverterTest {

  private static final Logger log = LoggerFactory.getLogger(ConfigConverterTest.class);

  private final InstanceId IID = InstanceId.of(UUID.randomUUID());
  private final String zkBasePath = Constants.ZROOT + "/" + IID;
  private final String zkSysConfig = zkBasePath + Constants.ZCONFIG;
  private final String zkNamespaces = zkBasePath + Constants.ZNAMESPACES;

  // mocks
  private ServerContext context;
  private ZooReaderWriter zrw;
  private ZooPropStore propStore;

  @BeforeEach
  public void initMock() {

    context = createMock(ServerContext.class);
    zrw = createMock(ZooReaderWriter.class);
    propStore = createMock(ZooPropStore.class);

    expect(context.getInstanceID()).andReturn(IID).anyTimes();
    expect(context.getZooReaderWriter()).andReturn(zrw).anyTimes();
    expect(context.getPropStore()).andReturn(propStore).anyTimes();

  }

  @Test
  public void emptySysTest() throws Exception {
    var sysPropId = PropCacheId.forSystem(IID);

    expect(zrw.getChildren(eq(zkSysConfig))).andReturn(List.of()).anyTimes();
    propStore.create(eq(sysPropId), anyObject());
    expectLastCall().anyTimes();

    expect(propStore.get(eq(sysPropId))).andReturn(new VersionedProperties(Map.of()));

    replay(context, zrw, propStore);

    ConfigConverter upgrade = new ConfigConverter(context);
    log.info("Converted: {}", upgrade);

    upgrade.convertSys();

    verify(context, zrw, propStore);

  }

  @Test
  public void blankNamespace() throws Exception {

    expect(zrw.getChildren(eq(zkNamespaces))).andReturn(List.of()).anyTimes();

    replay(context, zrw, propStore);

    ConfigConverter upgrade = new ConfigConverter(context);
    log.info("Converted: {}", upgrade);

    upgrade.convertNamespace();

    verify(context, zrw, propStore);

  }

  /**
   * Mock a complete property configuration in ZooKeeper with system, namespace and table
   * configuration.
   *
   * @param instanceId
   *          the instance id for the test
   * @param zrw
   *          a mock ZooReaderWriter.
   */
  private void populateFullConfig(final InstanceId instanceId, final ZooReaderWriter zrw) {
    reset(zrw, propStore);

    var zkTables = zkBasePath + Constants.ZTABLES;

    try {
      // system config
      expect(zrw.getChildren(eq(zkSysConfig)))
          .andReturn(List.of("table.split.threshold", "gc.port.client", PropCacheId.PROP_NODE_NAME))
          .anyTimes();
      expect(zrw.getData(eq(zkSysConfig + "/table.split.threshold")))
          .andReturn("512M".getBytes(UTF_8)).anyTimes(); // a valid table prop
      expect(zrw.getData(eq(zkSysConfig + "/gc.port.client"))).andReturn("9898".getBytes(UTF_8))
          .anyTimes(); // a fixed prop

      propStore.create(eq(PropCacheId.forSystem(instanceId)), anyObject());
      expectLastCall().anyTimes();

      expect(propStore.get(eq(PropCacheId.forSystem(instanceId))))
          .andReturn(new VersionedProperties(
              Map.of("table.split.threshold", "512M", "gc.port.client", "9898")));

      // namespaces +accumulo, +default, 1
      expect(zrw.getChildren(eq(zkNamespaces))).andReturn(List.of("+accumulo", "+default", "1"))
          .anyTimes();

      mockZkProp(zkNamespaces, NamespaceId.of("+accumulo"), Constants.ZNAMESPACE_CONF, Map.of());
      mockZkProp(zkNamespaces, NamespaceId.of("+default"), Constants.ZNAMESPACE_CONF, Map.of());
      mockZkProp(zkNamespaces, NamespaceId.of("1"), Constants.ZNAMESPACE_CONF,
          Map.of("table.split.threshold", "768M"));

      propStore.create(eq(PropCacheId.forNamespace(instanceId, NamespaceId.of("+default"))),
          anyObject());
      expectLastCall().anyTimes();

      // tables !0, +r, +rep, 2, 3, 4
      expect(zrw.getChildren(eq(zkTables))).andReturn(List.of("!0", "+r", "+rep", "2", "3", "4"))
          .anyTimes();
      mockZkProp(zkTables, TableId.of("!0"), Constants.ZTABLE_CONF, Map.of());
      mockZkProp(zkTables, TableId.of("+r"), Constants.ZTABLE_CONF, Map.of());
      mockZkProp(zkTables, TableId.of("+rep"), Constants.ZTABLE_CONF, Map.of());

      Map<String,
          String> defaultTableProps = Map.of("table.constraint.1",
              "org.apache.accumulo.core.data.constraints.DefaultKeySizeConstraint",
              "table.iterator.majc.vers",
              "20,org.apache.accumulo.core.iterators.user.VersioningIterator",
              "table.iterator.majc.vers.opt.maxVersions", "1", "table.iterator.minc.vers",
              "20,org.apache.accumulo.core.iterators.user.VersioningIterator",
              "table.iterator.minc.vers.opt.maxVersions", "1", "table.iterator.scan.vers",
              "20,org.apache.accumulo.core.iterators.user.VersioningIterator",
              "table.iterator.scan.vers.opt.maxVersions", "1");

      Map<String,String> customProps = new HashMap<>(defaultTableProps);
      customProps.put("table.split.threshold", "123M");

      // table with a custom prop
      mockZkProp(zkTables, TableId.of("2"), Constants.ZTABLE_CONF, customProps);

      // defaults after table create
      mockZkProp(zkTables, TableId.of("3"), Constants.ZTABLE_CONF, defaultTableProps);

      // test empty
      mockZkProp(zkTables, TableId.of("4"), Constants.ZTABLE_CONF, Map.of());

    } catch (KeeperException | InterruptedException ex) {
      throw new IllegalStateException("Mocking ZooKeeper configuration failed", ex);
    }
  }

  private void mockZkProp(final String base, final AbstractId<?> id, final String conf,
      Map<String,String> props) throws InterruptedException, KeeperException {
    var confPath = base + "/" + id.canonical() + conf;

    expect(zrw.getChildren(eq(confPath))).andReturn(new ArrayList<>(props.keySet())).anyTimes();

    for (Map.Entry<String,String> entry : props.entrySet()) {
      expect(zrw.getData(eq(confPath + "/" + entry.getKey())))
          .andReturn(entry.getValue().getBytes(UTF_8)).anyTimes();
    }
  }

  @Test
  public void MMapTest() {
    MMap m = new MMap();
    m.add("path1", Map.of());
    m.add("path2", Map.of("k2", "v2", "k1", "v1\\backslash"));

    Gson gson = new Gson();
    String json = gson.toJson(m);
    log.info("id: {}", json);

    MMap m2 = gson.fromJson(json, MMap.class);

    log.info("id2: {}", m2);

  }

  @Test
  public void sortIdTest() {
    Map<PropCacheId,String> m = new TreeMap<>();

    m.put(PropCacheId.forNamespace(IID, NamespaceId.of("+default")), "");
    m.put(PropCacheId.forTable(IID, TableId.of("+r")), "");
    m.put(PropCacheId.forTable(IID, TableId.of("+rep")), "");
    m.put(PropCacheId.forTable(IID, TableId.of("1")), "");
    m.put(PropCacheId.forNamespace(IID, NamespaceId.of("+accumulo")), "");
    m.put(PropCacheId.forTable(IID, TableId.of("!0")), "");
    m.put(PropCacheId.forSystem(IID), "");

    m.forEach((k, v) -> log.info("{}", k));

  }

  @Test
  public void sortTest() {
    Map<String,String> m = new TreeMap<>();
    m.put("2", "2");
    m.put("3", "3");
    m.put("+rep", "repl");
    m.put("+r", "root");
    m.put("!0", "meta");

    for (Map.Entry<String,String> e : m.entrySet()) {
      log.info("{}", e.getKey());
    }
  }

  private static class MMap {
    private final Map<String,Map<String,String>> aMap;

    public MMap() {
      aMap = new TreeMap<>();
    }

    public void add(final String path, Map<String,String> props) {
      aMap.put(path, new TreeMap<>(props));
    }

    @Override
    public String toString() {
      return "MMap{aMap=" + aMap + '}';
    }
  }

  @Test
  public void x() {
    Map<String,String> props = Map.of("a", "v1", "b", "v2", "master.c", "v3");

    Map<String,String> converted = new HashMap<>();
    props.forEach((original, value) -> {
      String finalName = DeprecatedPropertyUtil.getReplacementName(original,
          (log, replacement) -> log
              .info("Automatically renaming deprecated property '{}' with its replacement '{}'"
                  + " in ZooKeeper configuration upgrade.", original, replacement));
      converted.put(finalName, value);
    });

    log.info("props: {} -> {}", props, converted);
  }
}
