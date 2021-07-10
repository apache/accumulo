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
package org.apache.accumulo.server;

import static org.easymock.EasyMock.expect;

import java.util.Properties;

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.easymock.EasyMock;

/**
 * Get a generic mocked ServerContext with junk for testing.
 */
public class MockServerContext {

  public static ServerContext get() {
    ServerContext sc = EasyMock.createMock(ServerContext.class);
    ConfigurationCopy conf = new ConfigurationCopy(DefaultConfiguration.getInstance());
    conf.set(Property.INSTANCE_VOLUMES, "file:///");
    expect(sc.getConfiguration()).andReturn(conf).anyTimes();
    expect(sc.getProperties()).andReturn(new Properties()).anyTimes();
    return sc;
  }

  public static ServerContext getWithZK(String instanceID, String zk, int zkTimeout) {
    var sc = get();
    expect(sc.getZooKeeperRoot()).andReturn("/accumulo/" + instanceID).anyTimes();
    expect(sc.getInstanceID()).andReturn(instanceID).anyTimes();
    expect(sc.getZooKeepers()).andReturn(zk).anyTimes();
    expect(sc.getZooKeepersSessionTimeOut()).andReturn(zkTimeout).anyTimes();
    return sc;
  }

}
