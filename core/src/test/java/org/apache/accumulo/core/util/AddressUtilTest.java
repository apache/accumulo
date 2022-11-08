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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.security.Security;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the AddressUtil class.
 */
public class AddressUtilTest {

  private static final Logger log = LoggerFactory.getLogger(AddressUtilTest.class);

  @Test
  public void testGetNegativeTtl() {
    log.info("Checking that we can get the ttl on dns failures.");
    int expectedTtl = 20;
    boolean expectException = false;
    /* TODO ACCUMULO-2242 replace all of this with Powermock on the Security class */
    try {
      Security.setProperty("networkaddress.cache.negative.ttl", Integer.toString(expectedTtl));
    } catch (SecurityException exception) {
      log.warn(
          "We can't set the DNS cache period, so we're only testing fetching the system value.");
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
        log.info("AddressUtil is (hopefully) going to spit out an error about DNS lookups. "
            + "you can ignore it.");
      }
      int result = AddressUtil.getAddressCacheNegativeTtl(null);
      if (expectException) {
        fail("The JVM Security settings cache DNS failures forever. "
            + "In this case we expect an exception but didn't get one.");
      }
      assertEquals(expectedTtl, result, "Didn't get the ttl we expected");
    } catch (IllegalArgumentException exception) {
      if (!expectException) {
        log.error("Got an exception when we weren't expecting.", exception);
        fail("We only expect to throw an IllegalArgumentException when the JVM "
            + "caches DNS failures forever.");
      }
    }
  }

  @Test
  public void testGetNegativeTtlThrowsOnForever() {
    log.info("When DNS is cached forever, we should throw.");
    /* TODO ACCUMULO-2242 replace all of this with Powermock on the Security class */
    try {
      Security.setProperty("networkaddress.cache.negative.ttl", "-1");
    } catch (SecurityException exception) {
      log.error("We can't set the DNS cache period, so this test is effectively ignored.");
      return;
    }

    log.info("AddressUtil is (hopefully) going to spit out an error about DNS lookups. "
        + "you can ignore it.");
    assertThrows(IllegalArgumentException.class, () -> AddressUtil.getAddressCacheNegativeTtl(null),
        "The JVM Security settings cache DNS failures forever, this should cause an exception.");

  }
}
