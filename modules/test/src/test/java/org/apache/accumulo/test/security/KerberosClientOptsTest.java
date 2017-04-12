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
package org.apache.accumulo.test.security;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.harness.TestingKdc;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test that kerberos features work properly in {@link ClientOpts}
 */
public class KerberosClientOptsTest {
  private static final Logger log = LoggerFactory.getLogger(KerberosClientOptsTest.class);

  @Rule
  public TestName testName = new TestName();

  private static TestingKdc kdc;

  @BeforeClass
  public static void startKdc() throws Exception {
    kdc = new TestingKdc();
    kdc.start();
  }

  @AfterClass
  public static void stopKdc() throws Exception {
    if (null != kdc) {
      kdc.stop();
    }
  }

  @Before
  public void resetUgiForKrb() {
    Configuration conf = new Configuration(false);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(conf);
  }

  @Test
  public void testParseArgsPerformsLogin() throws Exception {
    String user = testName.getMethodName();
    File userKeytab = new File(kdc.getKeytabDir(), user + ".keytab");
    if (userKeytab.exists() && !userKeytab.delete()) {
      log.warn("Unable to delete {}", userKeytab);
    }

    kdc.createPrincipal(userKeytab, user);

    user = kdc.qualifyUser(user);

    ClientOpts opts = new ClientOpts();
    String[] args = new String[] {"--sasl", "--keytab", userKeytab.getAbsolutePath(), "-u", user};
    opts.parseArgs(testName.getMethodName(), args);

    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    assertEquals(user, ugi.getUserName());
    assertEquals(AuthenticationMethod.KERBEROS, ugi.getAuthenticationMethod());
  }
}
