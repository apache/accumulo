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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import java.util.UUID;

import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.lock.ServiceLockData.ServiceDescriptor;
import org.apache.accumulo.core.lock.ServiceLockData.ServiceDescriptors;
import org.apache.accumulo.core.rpc.RpcService;
import org.junit.jupiter.api.Test;

import com.google.common.net.HostAndPort;

public class ServiceLockDataTest {

  private final UUID serverUUID = UUID.randomUUID();

  @Test
  public void testSingleServiceConstructor() throws Exception {
    ServiceLockData ss =
        new ServiceLockData(serverUUID, "127.0.0.1", RpcService.TSERV, ResourceGroupId.DEFAULT);
    assertEquals(serverUUID, ss.getServerUUID(RpcService.TSERV));
    assertEquals("127.0.0.1", ss.getAddressString(RpcService.TSERV));
    assertThrows(IllegalArgumentException.class, () -> ss.getAddress(RpcService.TSERV));
    assertEquals(ResourceGroupId.DEFAULT, ss.getGroup(RpcService.TSERV));
    assertNull(ss.getServerUUID(RpcService.TABLET_SCAN));
    assertNull(ss.getAddressString(RpcService.TABLET_SCAN));
    assertNull(ss.getAddress(RpcService.TABLET_SCAN));
    assertNull(ss.getGroup(RpcService.TABLET_SCAN));
  }

  @Test
  public void testMultipleServiceConstructor() throws Exception {
    ServiceDescriptors sds = new ServiceDescriptors();
    sds.addService(new ServiceDescriptor(serverUUID, RpcService.TSERV, "127.0.0.1:9997",
        ResourceGroupId.DEFAULT));
    sds.addService(new ServiceDescriptor(serverUUID, RpcService.TABLET_SCAN, "127.0.0.1:9998",
        ResourceGroupId.DEFAULT));
    ServiceLockData ss = new ServiceLockData(sds);
    assertEquals(serverUUID, ss.getServerUUID(RpcService.TSERV));
    assertEquals("127.0.0.1:9997", ss.getAddressString(RpcService.TSERV));
    assertEquals(HostAndPort.fromString("127.0.0.1:9997"), ss.getAddress(RpcService.TSERV));
    assertEquals(ResourceGroupId.DEFAULT, ss.getGroup(RpcService.TSERV));
    assertEquals(serverUUID, ss.getServerUUID(RpcService.TABLET_SCAN));
    assertEquals("127.0.0.1:9998", ss.getAddressString(RpcService.TABLET_SCAN));
    assertEquals(HostAndPort.fromString("127.0.0.1:9998"), ss.getAddress(RpcService.TABLET_SCAN));
    assertEquals(ResourceGroupId.DEFAULT, ss.getGroup(RpcService.TSERV));
  }

  @Test
  public void testSingleServiceConstructorWithGroup() throws Exception {
    ServiceLockData ss =
        new ServiceLockData(serverUUID, "127.0.0.1", RpcService.TSERV, ResourceGroupId.of("meta"));
    assertEquals(serverUUID, ss.getServerUUID(RpcService.TSERV));
    assertEquals("127.0.0.1", ss.getAddressString(RpcService.TSERV));
    assertThrows(IllegalArgumentException.class, () -> ss.getAddress(RpcService.TSERV));
    assertEquals(ResourceGroupId.of("meta"), ss.getGroup(RpcService.TSERV));
    assertNull(ss.getServerUUID(RpcService.TABLET_SCAN));
    assertNull(ss.getAddressString(RpcService.TABLET_SCAN));
    assertNull(ss.getAddress(RpcService.TABLET_SCAN));
    assertNull(ss.getGroup(RpcService.TABLET_SCAN));
  }

  @Test
  public void testSingleServiceConstructor2WithGroup() throws Exception {
    ServiceLockData ss =
        new ServiceLockData(serverUUID, "127.0.0.1", RpcService.TSERV, ResourceGroupId.of("meta"));
    assertEquals(serverUUID, ss.getServerUUID(RpcService.TSERV));
    assertEquals("127.0.0.1", ss.getAddressString(RpcService.TSERV));
    assertThrows(IllegalArgumentException.class, () -> ss.getAddress(RpcService.TSERV));
    assertEquals(ResourceGroupId.of("meta"), ss.getGroup(RpcService.TSERV));
    assertEquals(serverUUID, ss.getServerUUID(RpcService.TSERV));
    assertNull(ss.getAddressString(RpcService.TABLET_SCAN));
    assertNull(ss.getAddress(RpcService.TABLET_SCAN));
    assertNull(ss.getGroup(RpcService.TABLET_SCAN));
  }

  @Test
  public void testMultipleServiceConstructorWithGroup() throws Exception {
    ServiceDescriptors sds = new ServiceDescriptors();
    sds.addService(new ServiceDescriptor(serverUUID, RpcService.TSERV, "127.0.0.1:9997",
        ResourceGroupId.of("meta")));
    sds.addService(new ServiceDescriptor(serverUUID, RpcService.TABLET_SCAN, "127.0.0.1:9998",
        ResourceGroupId.of("ns1")));
    ServiceLockData ss = new ServiceLockData(sds);
    assertEquals(serverUUID, ss.getServerUUID(RpcService.TSERV));
    assertEquals("127.0.0.1:9997", ss.getAddressString(RpcService.TSERV));
    assertEquals(HostAndPort.fromString("127.0.0.1:9997"), ss.getAddress(RpcService.TSERV));
    assertEquals(ResourceGroupId.of("meta"), ss.getGroup(RpcService.TSERV));
    assertEquals(serverUUID, ss.getServerUUID(RpcService.TABLET_SCAN));
    assertEquals("127.0.0.1:9998", ss.getAddressString(RpcService.TABLET_SCAN));
    assertEquals(HostAndPort.fromString("127.0.0.1:9998"), ss.getAddress(RpcService.TABLET_SCAN));
    assertEquals(ResourceGroupId.of("ns1"), ss.getGroup(RpcService.TABLET_SCAN));
    assertNull(ss.getAddressString(RpcService.COMPACTOR));
    assertNull(ss.getAddress(RpcService.COMPACTOR));
    assertNull(ss.getGroup(RpcService.COMPACTOR));
  }

  @Test
  public void testParseEmpty() {
    Optional<ServiceLockData> sld = ServiceLockData.parse(new byte[0]);
    assertTrue(sld.isEmpty());
    assertFalse(sld.isPresent());
  }

}
