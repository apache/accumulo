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
package org.apache.accumulo.server.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.ConnectorImpl;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.security.SystemCredentials.SystemToken;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
public class SystemCredentialsTest {

  @BeforeClass
  public static void setUp() throws IOException {
    File testInstanceId = new File(new File(new File(new File("target"), "instanceTest"), ServerConstants.INSTANCE_ID_DIR), UUID.fromString(
        "00000000-0000-0000-0000-000000000000").toString());
    if (!testInstanceId.exists()) {
      testInstanceId.getParentFile().mkdirs();
      testInstanceId.createNewFile();
    }

    File testInstanceVersion = new File(new File(new File(new File("target"), "instanceTest"), ServerConstants.VERSION_DIR), ServerConstants.DATA_VERSION + "");
    if (!testInstanceVersion.exists()) {
      testInstanceVersion.getParentFile().mkdirs();
      testInstanceVersion.createNewFile();
    }
  }

  /**
   * This is a test to ensure the string literal in {@link ConnectorImpl#ConnectorImpl(Instance, Credentials)} is kept up-to-date if we move the
   * {@link SystemToken}<br/>
   * This check will not be needed after ACCUMULO-1578
   */
  @Test
  public void testSystemToken() {
    assertEquals("org.apache.accumulo.server.security.SystemCredentials$SystemToken", SystemToken.class.getName());
    assertEquals(SystemCredentials.get().getToken().getClass(), SystemToken.class);
  }

  @Test
  public void testSystemCredentials() {
    Credentials a = SystemCredentials.get();
    Credentials b = SystemCredentials.get();
    assertTrue(a == b);
  }
}
