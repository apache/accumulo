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
package org.apache.accumulo.server.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.Credentials;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.server.AccumuloDataVersion;
import org.apache.accumulo.server.security.SystemCredentials.SystemToken;
import org.apache.commons.codec.digest.Crypt;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class SystemCredentialsTest {

  private static SiteConfiguration siteConfig = SiteConfiguration.empty().build();
  private InstanceId instanceId =
      InstanceId.of(UUID.nameUUIDFromBytes(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0}));

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "input not from a user")
  @BeforeAll
  public static void setUp() throws IOException {
    File testInstanceId =
        new File(new File(new File(new File("target"), "instanceTest"), Constants.INSTANCE_ID_DIR),
            UUID.fromString("00000000-0000-0000-0000-000000000000").toString());
    if (!testInstanceId.exists()) {
      assertTrue(
          testInstanceId.getParentFile().mkdirs() || testInstanceId.getParentFile().isDirectory());
      assertTrue(testInstanceId.createNewFile());
    }

    File testInstanceVersion =
        new File(new File(new File(new File("target"), "instanceTest"), Constants.VERSION_DIR),
            AccumuloDataVersion.get() + "");
    if (!testInstanceVersion.exists()) {
      assertTrue(testInstanceVersion.getParentFile().mkdirs()
          || testInstanceVersion.getParentFile().isDirectory());
      assertTrue(testInstanceVersion.createNewFile());
    }
  }

  @Test
  public void testWireVersion() {
    // sanity check to make sure it's a positive number
    assertTrue(SystemToken.INTERNAL_WIRE_VERSION > 0);
    // this is a sanity check that our wire version isn't crazy long, because
    // it must be less than or equal to 16 chars to be used as the SALT for SHA-512
    // when using Crypt.crypt()
    assertTrue(Integer.toString(SystemToken.INTERNAL_WIRE_VERSION).length() <= 16);
  }

  @Test
  public void testCryptDefaults() {
    // this is a sanity check that the default hash algorithm for commons-codec's
    // Crypt.crypt() method is still SHA-512 and the format hasn't changed
    // if that changes, we need to consider whether the new default is acceptable, and
    // whether or not we want to bump the wire version
    String hash = Crypt.crypt(new byte[0]);
    assertEquals(3, hash.chars().filter(ch -> ch == '$').count());
    assertTrue(hash.startsWith(SystemToken.SALT_PREFIX));
  }

  @Test
  public void testSystemCredentials() {
    Credentials a = SystemCredentials.get(instanceId, siteConfig);
    Credentials b = SystemCredentials.get(instanceId, siteConfig);
    assertEquals(a, b);
  }
}
