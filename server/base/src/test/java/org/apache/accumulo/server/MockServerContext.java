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

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;

import java.util.Properties;

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.server.conf.store.PropStore;
import org.easymock.EasyMock;

/**
 * Get a generic mocked ServerContext with junk for testing.
 */
public class MockServerContext {

  public static ServerContext get() {
    ServerContext context = EasyMock.createMock(ServerContext.class);
    ConfigurationCopy conf = new ConfigurationCopy(DefaultConfiguration.getInstance());
    conf.set(Property.INSTANCE_VOLUMES, "file:///");
    expect(context.getConfiguration()).andReturn(conf).anyTimes();
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
}
