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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

class HostAndPortTest {

  @Test
  void testCompareTo() {
    HostAndPort hostAndPort1 = HostAndPort.fromString("example.info");
    HostAndPort hostAndPort2 = HostAndPort.fromString("example.com");
    assertTrue(hostAndPort1.compareTo(hostAndPort2) > 0);

    HostAndPort hostPortSame = HostAndPort.fromString("www.test.com");
    assertTrue(hostPortSame.compareTo(hostPortSame) == 0);

    hostAndPort1 = HostAndPort.fromString("www.example.com");
    hostAndPort2 = HostAndPort.fromString("www.example.com");
    assertTrue(hostAndPort1.compareTo(hostAndPort2) == 0);

    hostAndPort1 = HostAndPort.fromString("192.0.2.1:80");
    hostAndPort2 = HostAndPort.fromString("192.0.2.1");
    assertTrue(hostAndPort1.compareTo(hostAndPort2) > 0);

    hostAndPort1 = HostAndPort.fromString("[2001:db8::1]");
    hostAndPort2 = HostAndPort.fromString("[2001:db9::1]");
    assertTrue(hostAndPort1.compareTo(hostAndPort2) < 0);

    hostAndPort1 = HostAndPort.fromString("2001:db8:3333:4444:5555:6676:7777:8888");
    hostAndPort2 = HostAndPort.fromString("2001:db8:3333:4444:5555:6666:7777:8888");
    assertTrue(hostAndPort1.compareTo(hostAndPort2) > 0);

    hostAndPort1 = HostAndPort.fromString("192.0.2.1:80");
    hostAndPort2 = HostAndPort.fromString("192.1.2.1");
    assertTrue(hostAndPort1.compareTo(hostAndPort2) < 0);

    hostAndPort1 = HostAndPort.fromString("12.1.2.1");
    hostAndPort2 = HostAndPort.fromString("192.1.2.1");
    assertTrue(hostAndPort1.compareTo(hostAndPort2) < 0);

    hostAndPort1 = HostAndPort.fromString("wwww.example.com");
    hostAndPort2 = HostAndPort.fromString("192.1.2.1");
    assertTrue(hostAndPort1.compareTo(hostAndPort2) > 0);

    hostAndPort1 = HostAndPort.fromString("2001:db8::1");
    hostAndPort2 = HostAndPort.fromString("2001:db9::1");
    assertTrue(hostAndPort1.compareTo(hostAndPort2) < 0);

    hostAndPort1 = HostAndPort.fromString("");
    hostAndPort2 = HostAndPort.fromString("2001:db9::1");
    assertTrue(hostAndPort1.compareTo(hostAndPort2) < 0);
    assertTrue(hostAndPort2.compareTo(hostAndPort1) > 0);

    hostAndPort1 = HostAndPort.fromString("2001:db8::1");
    hostAndPort2 = null;
    assertTrue(hostAndPort1.compareTo(hostAndPort2) > 0);
  }

  @Test
  void testOrder() {
    Set<HostAndPort> hostPortSet = Stream
        .of("example.info", "192.12.2.1:80", "example.com:80", "a.bb.c.d", "12.1.2.1",
            "localhost:0000090", "example.com", "100.100.100.100", "www.example.com",
            "[2001:eb8::1]", "localhost:90", "[2001:eb8::1]:80", "2001:db8::1", "100.100.101.100",
            "2001:::1", "192.12.2.1", "192.12.2.1:81", "199.10.1.1:14", "10.100.100.100",
            "2.2.2.2:10000", "192.12.2.1:79", "1.1.1.1:24", "1.1.1.1", "192.12.2.1:79", "a.b.c.d",
            "1.100.100.100", "2.2.2.2:9999", "a.b.b.d", "www.example.com", "www.alpha.org",
            "a.b.c.d:10", "a.b.b.d:10", "a.b.b.d:11")
        .map(HostAndPort::fromString).collect(Collectors.toCollection(TreeSet::new));
    hostPortSet.add(HostAndPort.fromParts("localhost", 1));
    hostPortSet.add(HostAndPort.fromParts("localhost", 000001));

    List<HostAndPort> expected = Stream
        .of("1.1.1.1", "1.1.1.1:24", "1.100.100.100", "10.100.100.100", "100.100.100.100",
            "100.100.101.100", "12.1.2.1", "192.12.2.1", "192.12.2.1:79", "192.12.2.1:80",
            "192.12.2.1:81", "199.10.1.1:14", "2.2.2.2:9999", "2.2.2.2:10000", "[2001:::1]",
            "[2001:db8::1]", "[2001:eb8::1]", "[2001:eb8::1]:80", "a.b.b.d", "a.b.b.d:10",
            "a.b.b.d:11", "a.b.c.d", "a.b.c.d:10", "a.bb.c.d", "example.com", "example.com:80",
            "example.info", "localhost:1", "localhost:90", "www.alpha.org", "www.example.com")
        .map(HostAndPort::fromString).collect(Collectors.toList());

    Object[] expectedArray = expected.toArray();
    Object[] hostPortArray = hostPortSet.toArray();

    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expectedArray[i].toString(), hostPortArray[i].toString());
    }
  }

}
