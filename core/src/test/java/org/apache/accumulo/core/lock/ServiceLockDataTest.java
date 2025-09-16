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
package org.apache.accumulo.core.lock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import java.util.UUID;

import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.lock.ServiceLockData.ServiceDescriptor;
import org.apache.accumulo.core.lock.ServiceLockData.ServiceDescriptors;
import org.apache.accumulo.core.lock.ServiceLockData.ThriftService;
import org.junit.jupiter.api.Test;

import com.google.common.net.HostAndPort;

public class ServiceLockDataTest {

  private final UUID serverUUID = UUID.randomUUID();

  @Test
  public void testSingleServiceConstructor() throws Exception {
    ServiceLockData ss =
        new ServiceLockData(serverUUID, ServerId.tserver("127.0.0.1", 9997), ThriftService.TSERV);
    assertEquals(serverUUID, ss.getServerUUID(ThriftService.TSERV));
    assertEquals("127.0.0.1", ss.getServer(ThriftService.TSERV).getHost());
    assertEquals(ResourceGroupId.DEFAULT, ss.getGroup(ThriftService.TSERV));
    assertNull(ss.getServerUUID(ThriftService.TABLET_SCAN));
    assertNull(ss.getServer(ThriftService.TABLET_SCAN));
    assertNull(ss.getGroup(ThriftService.TABLET_SCAN));
  }

  @Test
  public void testMultipleServiceConstructor() throws Exception {
    ServiceDescriptors sds = new ServiceDescriptors();
    sds.addService(new ServiceDescriptor(serverUUID, ThriftService.TSERV,
        ServerId.tserver("127.0.0.1", 9997)));
    sds.addService(new ServiceDescriptor(serverUUID, ThriftService.TABLET_SCAN,
        ServerId.tserver("127.0.0.1", 9998)));
    ServiceLockData ss = new ServiceLockData(sds);
    assertEquals(serverUUID, ss.getServerUUID(ThriftService.TSERV));
    assertEquals("127.0.0.1:9997", ss.getServer(ThriftService.TSERV).toHostPortString());
    assertEquals(HostAndPort.fromString("127.0.0.1:9997"), HostAndPort.fromParts(
        ss.getServer(ThriftService.TSERV).getHost(), ss.getServer(ThriftService.TSERV).getPort()));
    assertEquals(ResourceGroupId.DEFAULT, ss.getGroup(ThriftService.TSERV));
    assertEquals(serverUUID, ss.getServerUUID(ThriftService.TABLET_SCAN));
    assertEquals("127.0.0.1:9998", ss.getServer(ThriftService.TABLET_SCAN).toHostPortString());
    assertEquals(HostAndPort.fromString("127.0.0.1:9998"),
        HostAndPort.fromParts(ss.getServer(ThriftService.TABLET_SCAN).getHost(),
            ss.getServer(ThriftService.TABLET_SCAN).getPort()));
    assertEquals(ResourceGroupId.DEFAULT, ss.getGroup(ThriftService.TSERV));
  }

  @Test
  public void testSingleServiceConstructorWithGroup() throws Exception {
    ServiceLockData ss = new ServiceLockData(serverUUID,
        ServerId.tserver(ResourceGroupId.of("meta"), "127.0.0.1", 9997), ThriftService.TSERV);
    assertEquals(serverUUID, ss.getServerUUID(ThriftService.TSERV));
    assertEquals("127.0.0.1:9997", ss.getServer(ThriftService.TSERV).toHostPortString());
    assertEquals(ResourceGroupId.of("meta"), ss.getGroup(ThriftService.TSERV));
    assertNull(ss.getServerUUID(ThriftService.TABLET_SCAN));
    assertNull(ss.getServer(ThriftService.TABLET_SCAN));
    assertNull(ss.getGroup(ThriftService.TABLET_SCAN));
  }

  @Test
  public void testSingleServiceConstructor2WithGroup() throws Exception {
    ServiceLockData ss = new ServiceLockData(serverUUID,
        ServerId.tserver(ResourceGroupId.of("meta"), "127.0.0.1", 9997), ThriftService.TSERV);
    assertEquals(serverUUID, ss.getServerUUID(ThriftService.TSERV));
    assertEquals("127.0.0.1:9997", ss.getServer(ThriftService.TSERV).toHostPortString());
    assertEquals(ResourceGroupId.of("meta"), ss.getGroup(ThriftService.TSERV));
    assertEquals(serverUUID, ss.getServerUUID(ThriftService.TSERV));
    assertNull(ss.getServerUUID(ThriftService.TABLET_SCAN));
    assertNull(ss.getServer(ThriftService.TABLET_SCAN));
    assertNull(ss.getGroup(ThriftService.TABLET_SCAN));
  }

  @Test
  public void testMultipleServiceConstructorWithGroup() throws Exception {
    ServiceDescriptors sds = new ServiceDescriptors();
    sds.addService(new ServiceDescriptor(serverUUID, ThriftService.TSERV,
        ServerId.tserver(ResourceGroupId.of("meta"), "127.0.0.1", 9997)));
    sds.addService(new ServiceDescriptor(serverUUID, ThriftService.TABLET_SCAN,
        ServerId.tserver(ResourceGroupId.of("ns1"), "127.0.0.1", 9998)));
    ServiceLockData ss = new ServiceLockData(sds);
    assertEquals(serverUUID, ss.getServerUUID(ThriftService.TSERV));
    assertEquals("127.0.0.1:9997", ss.getServer(ThriftService.TSERV).toHostPortString());
    assertEquals(HostAndPort.fromString("127.0.0.1:9997"), HostAndPort.fromParts(
        ss.getServer(ThriftService.TSERV).getHost(), ss.getServer(ThriftService.TSERV).getPort()));
    assertEquals(ResourceGroupId.of("meta"), ss.getGroup(ThriftService.TSERV));
    assertEquals(serverUUID, ss.getServerUUID(ThriftService.TABLET_SCAN));
    assertEquals("127.0.0.1:9998", ss.getServer(ThriftService.TABLET_SCAN).toHostPortString());
    assertEquals(HostAndPort.fromString("127.0.0.1:9998"),
        HostAndPort.fromParts(ss.getServer(ThriftService.TABLET_SCAN).getHost(),
            ss.getServer(ThriftService.TABLET_SCAN).getPort()));
    assertEquals(ResourceGroupId.of("ns1"), ss.getGroup(ThriftService.TABLET_SCAN));
    assertNull(ss.getServerUUID(ThriftService.COMPACTOR));
    assertNull(ss.getServer(ThriftService.COMPACTOR));
    assertNull(ss.getGroup(ThriftService.COMPACTOR));
  }

  @Test
  public void testParseEmpty() {
    Optional<ServiceLockData> sld = ServiceLockData.parse(new byte[0]);
    assertTrue(sld.isEmpty());
    assertFalse(sld.isPresent());
  }

}
