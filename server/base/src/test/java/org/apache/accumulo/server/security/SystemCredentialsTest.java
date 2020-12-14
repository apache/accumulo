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
package org.apache.accumulo.server.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.accumulo.core.clientImpl.Credentials;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.security.SystemCredentials.SystemToken;
import org.apache.commons.codec.digest.Crypt;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class SystemCredentialsTest {

  @Rule
  public TestName test = new TestName();

  private static SiteConfiguration siteConfig = SiteConfiguration.auto();
  private String instanceId =
      UUID.nameUUIDFromBytes(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0}).toString();

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "input not from a user")
  @BeforeClass
  public static void setUp() throws IOException {
    File testInstanceId = new File(
        new File(new File(new File("target"), "instanceTest"), ServerConstants.INSTANCE_ID_DIR),
        UUID.fromString("00000000-0000-0000-0000-000000000000").toString());
    if (!testInstanceId.exists()) {
      assertTrue(
          testInstanceId.getParentFile().mkdirs() || testInstanceId.getParentFile().isDirectory());
      assertTrue(testInstanceId.createNewFile());
    }

    File testInstanceVersion = new File(
        new File(new File(new File("target"), "instanceTest"), ServerConstants.VERSION_DIR),
        ServerConstants.DATA_VERSION + "");
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

  /**
   * This is a test to ensure the SYSTEM_TOKEN_NAME string literal in
   * {@link org.apache.accumulo.core.clientImpl.ConnectorImpl} is kept up-to-date if we move the
   * {@link SystemToken}<br>
   *
   * @deprecated This check will not be needed after Connector is removed
   */
  @Deprecated(since = "2.0.0")
  @Test
  public void testSystemToken() {
    assertEquals("org.apache.accumulo.server.security.SystemCredentials$SystemToken",
        SystemToken.class.getName());
    assertEquals(SystemCredentials.get(instanceId, siteConfig).getToken().getClass(),
        SystemToken.class);
  }

  @Test
  public void testSystemCredentials() {
    Credentials a = SystemCredentials.get(instanceId, siteConfig);
    Credentials b = SystemCredentials.get(instanceId, siteConfig);
    assertEquals(a, b);
  }
}
