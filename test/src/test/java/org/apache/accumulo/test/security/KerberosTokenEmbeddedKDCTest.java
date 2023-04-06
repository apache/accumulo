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
package org.apache.accumulo.test.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;

import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.harness.TestingKdc;
import org.apache.accumulo.harness.WithTestNames;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class KerberosTokenEmbeddedKDCTest extends WithTestNames {

  private static final Logger log = LoggerFactory.getLogger(KerberosTokenEmbeddedKDCTest.class);

  private static volatile TestingKdc kdc;

  @BeforeAll
  public static void startKdc() throws Exception {
    kdc = new TestingKdc();
    kdc.start();
  }

  @AfterAll
  public static void stopKdc() {
    if (null != kdc) {
      kdc.stop();
    }
  }

  @BeforeEach
  public void resetUgiForKrb() {
    Configuration conf = new Configuration(false);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(conf);
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "path provided by test")
  @Test
  public void test() throws Exception {
    String user = testName();
    File userKeytab = new File(kdc.getKeytabDir(), user + ".keytab");
    if (userKeytab.exists() && !userKeytab.delete()) {
      log.warn("Unable to delete {}", userKeytab);
    }

    kdc.createPrincipal(userKeytab, user);

    user = kdc.qualifyUser(user);

    UserGroupInformation.loginUserFromKeytab(user, userKeytab.getAbsolutePath());
    KerberosToken token = new KerberosToken();

    assertEquals(user, token.getPrincipal());

    // Use the long-hand constructor, should be equivalent to short-hand
    KerberosToken tokenWithPrinc = new KerberosToken(user);
    assertEquals(token, tokenWithPrinc);
    assertEquals(token.hashCode(), tokenWithPrinc.hashCode());
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "path provided by test")
  @Test
  public void testDestroy() throws Exception {
    String user = testName();
    File userKeytab = new File(kdc.getKeytabDir(), user + ".keytab");
    if (userKeytab.exists() && !userKeytab.delete()) {
      log.warn("Unable to delete {}", userKeytab);
    }

    kdc.createPrincipal(userKeytab, user);

    user = kdc.qualifyUser(user);

    UserGroupInformation.loginUserFromKeytab(user, userKeytab.getAbsolutePath());
    KerberosToken token = new KerberosToken();

    assertEquals(user, token.getPrincipal());
    token.destroy();
    assertTrue(token.isDestroyed());
    assertNull(token.getPrincipal());
  }

}
