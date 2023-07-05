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

import org.apache.accumulo.core.lock.ServiceLockData.ServiceDescriptor;
import org.apache.accumulo.core.lock.ServiceLockData.ServiceDescriptors;
import org.apache.accumulo.core.lock.ServiceLockData.ThriftService;
import org.junit.jupiter.api.Test;

import com.google.common.net.HostAndPort;

public class ServiceLockDataTest {

  private final UUID serverUUID = UUID.randomUUID();

  @Test
  public void testSingleServiceConstructor() throws Exception {
    ServiceLockData ss = new ServiceLockData(serverUUID, "127.0.0.1", ThriftService.TSERV);
    assertEquals(serverUUID, ss.getServerUUID(ThriftService.TSERV));
    assertEquals("127.0.0.1", ss.getAddressString(ThriftService.TSERV));
    assertThrows(IllegalArgumentException.class, () -> ss.getAddress(ThriftService.TSERV));
    assertEquals(ServiceDescriptor.DEFAULT_GROUP_NAME, ss.getGroup(ThriftService.TSERV));
    assertNull(ss.getServerUUID(ThriftService.TABLET_SCAN));
    assertNull(ss.getAddressString(ThriftService.TABLET_SCAN));
    assertThrows(NullPointerException.class, () -> ss.getAddress(ThriftService.TABLET_SCAN));
    assertNull(ss.getGroup(ThriftService.TABLET_SCAN));
  }

  @Test
  public void testMultipleServiceConstructor() throws Exception {
    ServiceDescriptors sds = new ServiceDescriptors();
    sds.addService(new ServiceDescriptor(serverUUID, ThriftService.TSERV, "127.0.0.1:9997"));
    sds.addService(new ServiceDescriptor(serverUUID, ThriftService.TABLET_SCAN, "127.0.0.1:9998"));
    ServiceLockData ss = new ServiceLockData(sds);
    assertEquals(serverUUID, ss.getServerUUID(ThriftService.TSERV));
    assertEquals("127.0.0.1:9997", ss.getAddressString(ThriftService.TSERV));
    assertEquals(HostAndPort.fromString("127.0.0.1:9997"), ss.getAddress(ThriftService.TSERV));
    assertEquals(ServiceDescriptor.DEFAULT_GROUP_NAME, ss.getGroup(ThriftService.TSERV));
    assertEquals(serverUUID, ss.getServerUUID(ThriftService.TABLET_SCAN));
    assertEquals("127.0.0.1:9998", ss.getAddressString(ThriftService.TABLET_SCAN));
    assertEquals(HostAndPort.fromString("127.0.0.1:9998"),
        ss.getAddress(ThriftService.TABLET_SCAN));
    assertEquals(ServiceDescriptor.DEFAULT_GROUP_NAME, ss.getGroup(ThriftService.TSERV));
  }

  @Test
  public void testSingleServiceConstructorWithGroup() throws Exception {
    ServiceLockData ss = new ServiceLockData(serverUUID, "127.0.0.1", ThriftService.TSERV, "meta");
    assertEquals(serverUUID, ss.getServerUUID(ThriftService.TSERV));
    assertEquals("127.0.0.1", ss.getAddressString(ThriftService.TSERV));
    assertThrows(IllegalArgumentException.class, () -> ss.getAddress(ThriftService.TSERV));
    assertEquals("meta", ss.getGroup(ThriftService.TSERV));
    assertNull(ss.getServerUUID(ThriftService.TABLET_SCAN));
    assertNull(ss.getAddressString(ThriftService.TABLET_SCAN));
    assertThrows(NullPointerException.class, () -> ss.getAddress(ThriftService.TABLET_SCAN));
    assertNull(ss.getGroup(ThriftService.TABLET_SCAN));
  }

  @Test
  public void testSingleServiceConstructor2WithGroup() throws Exception {
    ServiceLockData ss = new ServiceLockData(serverUUID, "127.0.0.1", ThriftService.TSERV, "meta");
    assertEquals(serverUUID, ss.getServerUUID(ThriftService.TSERV));
    assertEquals("127.0.0.1", ss.getAddressString(ThriftService.TSERV));
    assertThrows(IllegalArgumentException.class, () -> ss.getAddress(ThriftService.TSERV));
    assertEquals("meta", ss.getGroup(ThriftService.TSERV));
    assertEquals(serverUUID, ss.getServerUUID(ThriftService.TSERV));
    assertNull(ss.getAddressString(ThriftService.TABLET_SCAN));
    assertThrows(NullPointerException.class, () -> ss.getAddress(ThriftService.TABLET_SCAN));
    assertNull(ss.getGroup(ThriftService.TABLET_SCAN));
  }

  @Test
  public void testMultipleServiceConstructorWithGroup() throws Exception {
    ServiceDescriptors sds = new ServiceDescriptors();
    sds.addService(
        new ServiceDescriptor(serverUUID, ThriftService.TSERV, "127.0.0.1:9997", "meta"));
    sds.addService(
        new ServiceDescriptor(serverUUID, ThriftService.TABLET_SCAN, "127.0.0.1:9998", "ns1"));
    ServiceLockData ss = new ServiceLockData(sds);
    assertEquals(serverUUID, ss.getServerUUID(ThriftService.TSERV));
    assertEquals("127.0.0.1:9997", ss.getAddressString(ThriftService.TSERV));
    assertEquals(HostAndPort.fromString("127.0.0.1:9997"), ss.getAddress(ThriftService.TSERV));
    assertEquals("meta", ss.getGroup(ThriftService.TSERV));
    assertEquals(serverUUID, ss.getServerUUID(ThriftService.TABLET_SCAN));
    assertEquals("127.0.0.1:9998", ss.getAddressString(ThriftService.TABLET_SCAN));
    assertEquals(HostAndPort.fromString("127.0.0.1:9998"),
        ss.getAddress(ThriftService.TABLET_SCAN));
    assertEquals("ns1", ss.getGroup(ThriftService.TABLET_SCAN));
  }

  @Test
  public void testParseEmpty() {
    Optional<ServiceLockData> sld = ServiceLockData.parse(new byte[0]);
    assertTrue(sld.isEmpty());
    assertFalse(sld.isPresent());
  }

}
