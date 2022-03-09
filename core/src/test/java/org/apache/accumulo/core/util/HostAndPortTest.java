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
package org.apache.accumulo.core.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;

class HostAndPortTest {

  @Test
  void testCompareTo() {
    HostAndPort hostAndPort1 = HostAndPort.fromString("example.info");
    HostAndPort hostAndPort2 = HostAndPort.fromString("example.com");
    assertTrue(hostAndPort1.compareTo(hostAndPort2) > 0);

    HostAndPort hostPortSame = HostAndPort.fromString("www.test.com");
    assertTrue(hostAndPort1.compareTo(hostAndPort1) == 0);

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
    Set<HostAndPort> hostPortSet = new TreeSet<>();
    hostPortSet.add(HostAndPort.fromString("example.info"));
    hostPortSet.add(HostAndPort.fromString("192.12.2.1:80"));
    hostPortSet.add(HostAndPort.fromString("example.com:80"));
    hostPortSet.add(HostAndPort.fromString("a.bb.c.d"));
    hostPortSet.add(HostAndPort.fromString("12.1.2.1"));
    hostPortSet.add(HostAndPort.fromString("localhost:0000090"));
    hostPortSet.add(HostAndPort.fromString("example.com"));
    hostPortSet.add(HostAndPort.fromString("100.100.100.100"));
    hostPortSet.add(HostAndPort.fromString("www.example.com"));
    hostPortSet.add(HostAndPort.fromString("[2001:eb8::1]"));
    hostPortSet.add(HostAndPort.fromString("localhost:90"));
    hostPortSet.add(HostAndPort.fromString("[2001:eb8::1]:80"));
    hostPortSet.add(HostAndPort.fromString("2001:db8::1"));
    hostPortSet.add(HostAndPort.fromString("100.100.101.100"));
    hostPortSet.add(HostAndPort.fromString("2001:::1"));
    hostPortSet.add(HostAndPort.fromString("192.12.2.1"));
    hostPortSet.add(HostAndPort.fromString("192.12.2.1:81"));
    hostPortSet.add(HostAndPort.fromString("199.10.1.1:14"));
    hostPortSet.add(HostAndPort.fromString("10.100.100.100"));
    hostPortSet.add(HostAndPort.fromString("2.2.2.2:10000"));
    hostPortSet.add(HostAndPort.fromString("192.12.2.1:79"));
    hostPortSet.add(HostAndPort.fromString("1.1.1.1:24"));
    hostPortSet.add(HostAndPort.fromParts("localhost", 000001));
    hostPortSet.add(HostAndPort.fromString("1.1.1.1"));
    hostPortSet.add(HostAndPort.fromString("192.12.2.1:79"));
    hostPortSet.add(HostAndPort.fromString("a.b.c.d"));
    hostPortSet.add(HostAndPort.fromString("1.100.100.100"));
    hostPortSet.add(HostAndPort.fromString("2.2.2.2:9999"));
    hostPortSet.add(HostAndPort.fromParts("localhost", 1));
    hostPortSet.add(HostAndPort.fromString("a.b.b.d"));
    hostPortSet.add(HostAndPort.fromString("www.example.com"));
    hostPortSet.add(HostAndPort.fromString("www.alpha.org"));
    hostPortSet.add(HostAndPort.fromString("a.b.c.d:10"));
    hostPortSet.add(HostAndPort.fromString("a.b.b.d:10"));
    hostPortSet.add(HostAndPort.fromString("a.b.b.d:11"));

    List<HostAndPort> expected = List.of(HostAndPort.fromString("1.1.1.1"),
        HostAndPort.fromString("1.1.1.1:24"), HostAndPort.fromString("1.100.100.100"),
        HostAndPort.fromString("10.100.100.100"), HostAndPort.fromString("100.100.100.100"),
        HostAndPort.fromString("100.100.101.100"), HostAndPort.fromString("12.1.2.1"),
        HostAndPort.fromString("192.12.2.1"), HostAndPort.fromString("192.12.2.1:79"),
        HostAndPort.fromString("192.12.2.1:80"), HostAndPort.fromString("192.12.2.1:81"),
        HostAndPort.fromString("199.10.1.1:14"), HostAndPort.fromString("2.2.2.2:9999"),
        HostAndPort.fromString("2.2.2.2:10000"), HostAndPort.fromString("[2001:::1]"),
        HostAndPort.fromString("[2001:db8::1]"), HostAndPort.fromString("[2001:eb8::1]"),
        HostAndPort.fromString("[2001:eb8::1]:80"), HostAndPort.fromString("a.b.b.d"),
        HostAndPort.fromString("a.b.b.d:10"), HostAndPort.fromString("a.b.b.d:11"),
        HostAndPort.fromString("a.b.c.d"), HostAndPort.fromString("a.b.c.d:10"),
        HostAndPort.fromString("a.bb.c.d"), HostAndPort.fromString("example.com"),
        HostAndPort.fromString("example.com:80"), HostAndPort.fromString("example.info"),
        HostAndPort.fromString("localhost:1"), HostAndPort.fromString("localhost:90"),
        HostAndPort.fromString("www.alpha.org"), HostAndPort.fromString("www.example.com"));

    Object[] expectedArray = expected.toArray();
    Object[] hostPortArray = hostPortSet.toArray();

    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expectedArray[i].toString(), hostPortArray[i].toString());
    }
  }

}
