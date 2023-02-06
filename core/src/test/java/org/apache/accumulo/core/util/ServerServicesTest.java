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
package org.apache.accumulo.core.util;

import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;

import org.apache.accumulo.core.util.ServerLockData.ServerDescriptor;
import org.apache.accumulo.core.util.ServerLockData.ServerDescriptors;
import org.apache.accumulo.core.util.ServerLockData.Service;
import org.junit.jupiter.api.Test;

import com.google.common.net.HostAndPort;

public class ServerServicesTest {

  private final UUID serverUUID = UUID.randomUUID();

  @Test
  public void testSingleServiceConstructor() throws Exception {
    ServerLockData ss = new ServerLockData(
        ServerDescriptors.parse(serverUUID.toString() + "=TSERV_CLIENT=127.0.0.1"));
    assertEquals(serverUUID, ss.getServerUUID(Service.TSERV_CLIENT));
    assertEquals("127.0.0.1", ss.getAddressString(Service.TSERV_CLIENT));
    assertThrows(IllegalArgumentException.class, () -> ss.getAddress(Service.TSERV_CLIENT));
    assertEquals(ServerDescriptor.DEFAULT_GROUP_NAME, ss.getGroup(Service.TSERV_CLIENT));
    assertEquals(serverUUID.toString() + "=TSERV_CLIENT=127.0.0.1=default", ss.toString());
    assertNull(ss.getServerUUID(Service.SSERV_CLIENT));
    assertNull(ss.getAddressString(Service.SSERV_CLIENT));
    assertThrows(NullPointerException.class, () -> ss.getAddress(Service.SSERV_CLIENT));
    assertNull(ss.getGroup(Service.SSERV_CLIENT));
  }

  @Test
  public void testMultipleServiceConstructor() throws Exception {
    ServerLockData ss = new ServerLockData(
        ServerDescriptors.parse(serverUUID.toString() + "=TSERV_CLIENT=127.0.0.1:9997;"
            + serverUUID.toString() + "=SSERV_CLIENT=127.0.0.1:9998"));
    assertEquals(serverUUID, ss.getServerUUID(Service.TSERV_CLIENT));
    assertEquals("127.0.0.1:9997", ss.getAddressString(Service.TSERV_CLIENT));
    assertEquals(HostAndPort.fromString("127.0.0.1:9997"), ss.getAddress(Service.TSERV_CLIENT));
    assertEquals(ServerDescriptor.DEFAULT_GROUP_NAME, ss.getGroup(Service.TSERV_CLIENT));
    assertEquals(serverUUID, ss.getServerUUID(Service.SSERV_CLIENT));
    assertEquals("127.0.0.1:9998", ss.getAddressString(Service.SSERV_CLIENT));
    assertEquals(HostAndPort.fromString("127.0.0.1:9998"), ss.getAddress(Service.SSERV_CLIENT));
    assertEquals(ServerDescriptor.DEFAULT_GROUP_NAME, ss.getGroup(Service.TSERV_CLIENT));
    // toString() only includes TSERV_CLIENT and GC_CLIENT
    assertEquals(serverUUID.toString() + "=TSERV_CLIENT=127.0.0.1:9997=default", ss.toString());
  }

  @Test
  public void testSingleServiceConstructorWithGroup() throws Exception {
    ServerLockData ss = new ServerLockData(
        ServerDescriptors.parse(serverUUID.toString() + "=TSERV_CLIENT=127.0.0.1=meta"));
    assertEquals(serverUUID, ss.getServerUUID(Service.TSERV_CLIENT));
    assertEquals("127.0.0.1", ss.getAddressString(Service.TSERV_CLIENT));
    assertThrows(IllegalArgumentException.class, () -> ss.getAddress(Service.TSERV_CLIENT));
    assertEquals("meta", ss.getGroup(Service.TSERV_CLIENT));
    assertEquals(serverUUID.toString() + "=TSERV_CLIENT=127.0.0.1=meta", ss.toString());
    assertNull(ss.getServerUUID(Service.SSERV_CLIENT));
    assertNull(ss.getAddressString(Service.SSERV_CLIENT));
    assertThrows(NullPointerException.class, () -> ss.getAddress(Service.SSERV_CLIENT));
    assertNull(ss.getGroup(Service.SSERV_CLIENT));
  }

  @Test
  public void testSingleServiceConstructor2WithGroup() throws Exception {
    ServerLockData ss = new ServerLockData(serverUUID, "127.0.0.1", Service.TSERV_CLIENT, "meta");
    assertEquals(serverUUID, ss.getServerUUID(Service.TSERV_CLIENT));
    assertEquals("127.0.0.1", ss.getAddressString(Service.TSERV_CLIENT));
    assertThrows(IllegalArgumentException.class, () -> ss.getAddress(Service.TSERV_CLIENT));
    assertEquals("meta", ss.getGroup(Service.TSERV_CLIENT));
    assertEquals(serverUUID.toString() + "=TSERV_CLIENT=127.0.0.1=meta", ss.toString());
    assertEquals(serverUUID, ss.getServerUUID(Service.TSERV_CLIENT));
    assertNull(ss.getAddressString(Service.SSERV_CLIENT));
    assertThrows(NullPointerException.class, () -> ss.getAddress(Service.SSERV_CLIENT));
    assertNull(ss.getGroup(Service.SSERV_CLIENT));
  }

  @Test
  public void testMultipleServiceConstructorWithGroup() throws Exception {
    ServerLockData ss = new ServerLockData(
        ServerDescriptors.parse(serverUUID.toString() + "=TSERV_CLIENT=127.0.0.1:9997=meta;"
            + serverUUID.toString() + "=SSERV_CLIENT=127.0.0.1:9998=ns1"));
    assertEquals(serverUUID, ss.getServerUUID(Service.TSERV_CLIENT));
    assertEquals("127.0.0.1:9997", ss.getAddressString(Service.TSERV_CLIENT));
    assertEquals(HostAndPort.fromString("127.0.0.1:9997"), ss.getAddress(Service.TSERV_CLIENT));
    assertEquals("meta", ss.getGroup(Service.TSERV_CLIENT));
    assertEquals(serverUUID, ss.getServerUUID(Service.TSERV_CLIENT));
    assertEquals("127.0.0.1:9998", ss.getAddressString(Service.SSERV_CLIENT));
    assertEquals(HostAndPort.fromString("127.0.0.1:9998"), ss.getAddress(Service.SSERV_CLIENT));
    assertEquals("ns1", ss.getGroup(Service.SSERV_CLIENT));
    // toString() only includes TSERV_CLIENT and GC_CLIENT
    assertEquals(serverUUID.toString() + "=TSERV_CLIENT=127.0.0.1:9997=meta", ss.toString());
  }

}
