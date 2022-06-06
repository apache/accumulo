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
package org.apache.accumulo.server;

import static java.util.Arrays.stream;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.crypto.CryptoServiceFactory;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.server.conf.store.PropStore;
import org.easymock.EasyMock;

/**
 * A mocked ServerContext. Can be used with static methods, which use {@link #get()} or constructed
 * builder style, which requires a call to {@link #done()}.
 */
public class MockServerContext {
  ServerContext sc;

  public MockServerContext() {
    sc = get();
  }

  /**
   * Get a generic mocked ServerContext with junk for testing.
   */
  public static ServerContext get() {
    ServerContext context = createMock(ServerContext.class);
    ConfigurationCopy conf = new ConfigurationCopy(DefaultConfiguration.getInstance());
    conf.set(Property.INSTANCE_VOLUMES, "file:///");
    expect(context.getConfiguration()).andReturn(conf).anyTimes();
    expect(context.getCryptoService()).andReturn(CryptoServiceFactory.newDefaultInstance())
        .anyTimes();
    expect(context.getProperties()).andReturn(new Properties()).anyTimes();
    return context;
  }

  public static ServerContext getWithZK(InstanceId instanceID, String zk, int zkTimeout) {
    var sc = get();
    expect(sc.getZooKeeperRoot()).andReturn("/accumulo/" + instanceID).anyTimes();
    expect(sc.getInstanceID()).andReturn(instanceID).anyTimes();
    expect(sc.getZooKeepers()).andReturn(zk).anyTimes();
    expect(sc.getZooKeepersSessionTimeOut()).andReturn(zkTimeout).anyTimes();
    return sc;
  }

  public static ServerContext getMockContextWithPropStore(final InstanceId instanceID,
      ZooReaderWriter zrw, PropStore propStore) {

    ServerContext sc = createMock(ServerContext.class);
    expect(sc.getInstanceID()).andReturn(instanceID).anyTimes();
    expect(sc.getZooReaderWriter()).andReturn(zrw).anyTimes();
    expect(sc.getZooKeeperRoot()).andReturn("/accumulo/" + instanceID).anyTimes();
    expect(sc.getPropStore()).andReturn(propStore).anyTimes();
    return sc;
  }

  public static void verify(ServerContext context, Object... objects) {
    EasyMock.verify(context, objects);
  }

  public MockServerContext withAmple() {
    Ample ample = MockAmple.get();
    expect(sc.getAmple()).andReturn(ample).anyTimes();
    return this;
  }

  /**
   * Mock {@link TableOperations} with the given tableNames. Mocks a single call to create table.
   * Allows any number of calls to exist, list, and flush.
   */
  // public MockServerContext withTables(String name) throws Exception {
  public MockServerContext withTables(String... tableNames) throws Exception {
    TableOperations tableOps = createMock(TableOperations.class);

    // mock tables are created once and exist always
    stream(tableNames).forEach(name -> {
      try {
        tableOps.create(name);
        expectLastCall().once();
        tableOps.flush(name);
        expectLastCall().anyTimes();
        expect(tableOps.isOnline(name)).andReturn(true).anyTimes();

      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      expect(tableOps.exists(name)).andReturn(true).anyTimes();
    });
    expect(tableOps.list()).andReturn(new TreeSet<>(Set.of(tableNames))).anyTimes();
    // expect(tableOps.list()).andReturn(new TreeSet<>(Set.of(name))).anyTimes();
    expect(tableOps.tableIdMap()).andReturn(tableIdMap(tableNames)).anyTimes();
    // expect(tableOps.tableIdMap()).andReturn(tableIdMap(name)).anyTimes();
    expect(sc.tableOperations()).andReturn(tableOps).anyTimes();

    return this;
  }

  /**
   * Get a mapping of table name to internal fake Table Ids for the given tableNames.
   */
  private Map<String,String> tableIdMap(String... tableNames) {
    Map<String,String> tableIdMap = new TreeMap<>();
    int i = 1;
    for (String name : tableNames) {
      tableIdMap.put(name, "" + i++);
    }
    return tableIdMap;
  }

  /**
   * Call replay and return the Mocked ServerContext.
   */
  public ServerContext done() {
    replay(sc);
    return sc;
  }
}
