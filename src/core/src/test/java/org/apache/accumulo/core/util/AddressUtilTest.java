/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.util;

import java.net.InetSocketAddress;
import java.security.Security;

import junit.framework.TestCase;

import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.thrift.transport.TSocket;

/**
 * Test the AddressUtil class.
 * 
 */
public class AddressUtilTest extends TestCase {

  private static final Logger log = Logger.getLogger(AddressUtilTest.class);

  public void testAddress() {
    InetSocketAddress addr = AddressUtil.parseAddress("127.0.0.1", 12345);
    assertTrue(addr.equals(new InetSocketAddress("127.0.0.1", 12345)));
    InetSocketAddress addr2 = AddressUtil.parseAddress("127.0.0.1:1234", 12345);
    assertTrue(addr2.equals(new InetSocketAddress("127.0.0.1", 1234)));
    InetSocketAddress addr3 = AddressUtil.parseAddress("127.0.0.1:", 12345);
    assertTrue(addr3.equals(new InetSocketAddress("127.0.0.1", 12345)));
    try {
      AddressUtil.parseAddress("127.0.0.1:junk", 12345);
      fail("Number Format Exception Not Thrown");
    } catch (NumberFormatException ex) {
      assertTrue(true);
    }
    InetSocketAddress addr5 = AddressUtil.parseAddress(new Text("127.0.0.1:543"), 12345);
    assertTrue(addr5.equals(new InetSocketAddress("127.0.0.1", 543)));
    TSocket sock = AddressUtil.createTSocket("127.0.0.11111", 0);
    // lame:
    assertTrue(sock != null);
  }
  
  public void testToString() {
    assertTrue(AddressUtil.toString(new InetSocketAddress("127.0.0.1", 1234)).equals("127.0.0.1:1234"));
  }

  public void testGetNegativeTtl() {
    log.info("Checking that we can get the ttl on dns failures.");
    int expectedTtl = 20;
    boolean expectException = false;
    /* TODO replace all of this with Powermock on the Security class */
    try {
      Security.setProperty("networkaddress.cache.negative.ttl", Integer.toString(expectedTtl));
    } catch (SecurityException exception) {
      log.warn("We can't set the DNS cache period, so we're only testing fetching the system value.");
      expectedTtl = 10;
    }
    try {
      expectedTtl = Integer.parseInt(Security.getProperty("networkaddress.cache.negative.ttl"));
    } catch (SecurityException exception) {
      log.debug("Security manager won't let us fetch the property, testing default path.");
      expectedTtl = 10;
    } catch (NumberFormatException exception) {
      log.debug("property isn't a number, testing default path.");
      expectedTtl = 10;
    }
    if (-1 == expectedTtl) {
      log.debug("property is set to 'forever', testing exception path");
      expectException = true;
    }
    if (0 > expectedTtl) {
      log.debug("property is a negative value other than 'forever', testing default path.");
      expectedTtl = 10;
    }
    try {
      if (expectException) {
        log.info("AddressUtil is (hopefully) going to spit out an error about DNS lookups. you can ignore it.");
      }
      int result = AddressUtil.getAddressCacheNegativeTtl(null);
      if (expectException) {
        fail("The JVM Security settings cache DNS failures forever. In this case we expect an exception but didn't get one.");
      }
      assertEquals("Didn't get the ttl we expected", expectedTtl, result);
    } catch (IllegalArgumentException exception) {
      if (!expectException) {
        log.error("Got an exception when we weren't expecting.", exception);
        fail("We only expect to throw an IllegalArgumentException when the JVM caches DNS failures forever.");
      }
    }
  }

  public void testGetNegativeTtlThrowsOnForever() {
    log.info("When DNS is cached forever, we should throw.");
    /* TODO replace all of this with Powermock on the Security class */
    try {
      Security.setProperty("networkaddress.cache.negative.ttl", "-1");
    } catch (SecurityException exception) {
      log.error("We can't set the DNS cache period, so this test is effectively ignored.");
      return;
    }
    try {
      log.info("AddressUtil is (hopefully) going to spit out an error about DNS lookups. you can ignore it.");
      int result = AddressUtil.getAddressCacheNegativeTtl(null);
      fail("The JVM Security settings cache DNS failures forever, this should cause an exception.");
    } catch(IllegalArgumentException exception) {
      assertTrue(true);
    }
  }
}
