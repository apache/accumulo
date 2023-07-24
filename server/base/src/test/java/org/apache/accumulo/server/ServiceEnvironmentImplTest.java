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

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.junit.jupiter.api.Test;

public class ServiceEnvironmentImplTest {

  @Test
  public void getWithRecognizedPrefixTest() {

    String prefix = Property.RPC_PREFIX.getKey();
    Map<String,String> expectedPropertyMap = Map.of("rpc.javax.net.ssl.keyStoreType", "jks");

    ServerContext context = createMock(ServerContext.class);
    AccumuloConfiguration acfg = createMock(AccumuloConfiguration.class);
    expect(acfg.newDeriver(anyObject())).andReturn(Map::of).anyTimes();
    expect(acfg.getAllPropertiesWithPrefix(Property.RPC_PREFIX)).andReturn(expectedPropertyMap);
    expect(context.getConfiguration()).andReturn(acfg);
    replay(context, acfg);

    var serviceEnvironment = new ServiceEnvironmentImpl(context);

    Map<String,String> returnedProperties =
        serviceEnvironment.getConfiguration().getWithPrefix(prefix);

    assertEquals(expectedPropertyMap, returnedProperties);

    verify(context, acfg);
  }

  @Test
  public void getWithUnrecognizedPrefixTest() {
    String prefix = "a.b";
    Map<String,String> expectedPropertyMap = Map.of("a.b.favorite.license", "apache");

    ServerContext context = createMock(ServerContext.class);
    AccumuloConfiguration acfg = createMock(AccumuloConfiguration.class);
    expect(acfg.newDeriver(anyObject())).andReturn(Map::of).anyTimes();
    expect(acfg.spliterator()).andReturn(expectedPropertyMap.entrySet().spliterator());
    expect(context.getConfiguration()).andReturn(acfg);
    replay(context, acfg);

    var serviceEnvironment = new ServiceEnvironmentImpl(context);

    Map<String,String> returnedProperties =
        serviceEnvironment.getConfiguration().getWithPrefix(prefix);

    assertEquals(expectedPropertyMap, returnedProperties);

    verify(context, acfg);
  }
}
