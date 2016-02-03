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
package org.apache.accumulo.test.functional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.accumulo.cluster.AccumuloCluster;
import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ClientOpts.Password;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.TestRandomDeletes;
import org.apache.accumulo.test.VerifyIngest;
import org.junit.Test;

public class DeleteIT extends AccumuloClusterHarness {

  @Override
  protected int defaultTimeoutSeconds() {
    return 2 * 60;
  }

  @Test
  public void test() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    AuthenticationToken token = getAdminToken();
    if (token instanceof KerberosToken) {
      deleteTest(c, getCluster(), getAdminPrincipal(), null, tableName, getAdminUser().getKeytab().getAbsolutePath());
    } else if (token instanceof PasswordToken) {
      PasswordToken passwdToken = (PasswordToken) token;
      deleteTest(c, getCluster(), getAdminPrincipal(), new String(passwdToken.getPassword(), UTF_8), tableName, null);
    }
  }

  public static void deleteTest(Connector c, AccumuloCluster cluster, String user, String password, String tableName, String keytab) throws Exception {
    VerifyIngest.Opts vopts = new VerifyIngest.Opts();
    TestIngest.Opts opts = new TestIngest.Opts();
    vopts.setTableName(tableName);
    opts.setTableName(tableName);
    vopts.rows = opts.rows = 1000;
    vopts.cols = opts.cols = 1;
    vopts.random = opts.random = 56;

    assertTrue("Expected one of password or keytab", null != password || null != keytab);
    if (null != password) {
      assertNull("Given password, expected null keytab", keytab);
      Password passwd = new Password(password);
      opts.setPassword(passwd);
      opts.setPrincipal(user);
      vopts.setPassword(passwd);
      vopts.setPrincipal(user);
    }
    if (null != keytab) {
      assertNull("Given keytab, expect null password", password);
      ClientConfiguration clientConfig = cluster.getClientConfig();
      opts.updateKerberosCredentials(clientConfig);
      vopts.updateKerberosCredentials(clientConfig);
    }

    BatchWriterOpts BWOPTS = new BatchWriterOpts();
    TestIngest.ingest(c, opts, BWOPTS);

    String[] args = null;

    assertTrue("Expected one of password or keytab", null != password || null != keytab);
    if (null != password) {
      assertNull("Given password, expected null keytab", keytab);
      args = new String[] {"-u", user, "-p", password, "-i", cluster.getInstanceName(), "-z", cluster.getZooKeepers(), "--table", tableName};
    }
    if (null != keytab) {
      assertNull("Given keytab, expect null password", password);
      args = new String[] {"-u", user, "-i", cluster.getInstanceName(), "-z", cluster.getZooKeepers(), "--table", tableName, "--keytab", keytab};
    }

    assertEquals(0, cluster.getClusterControl().exec(TestRandomDeletes.class, args));
    TestIngest.ingest(c, opts, BWOPTS);
    VerifyIngest.verifyIngest(c, vopts, new ScannerOpts());
  }

}
