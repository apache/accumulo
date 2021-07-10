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
package org.apache.accumulo.test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.test.VolumeChooserIT.PERTABLE_CHOOSER_PROP;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.harness.conf.StandaloneAccumuloClusterConfiguration;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.test.ShellServerIT.TestShell;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ShellConfigIT extends AccumuloClusterHarness {

  @Override
  public int defaultTimeoutSeconds() {
    return 30;
  }

  private String origPropValue;

  @Before
  public void checkProperty() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      // TABLE_VOLUME_CHOOSER is a valid property that can be updated in ZK, whereas the crypto
      // properties are not.
      // This lets us run this test more generically rather than forcibly needing to update some
      // property in accumulo.properties
      origPropValue =
          client.instanceOperations().getSystemConfiguration().get(PERTABLE_CHOOSER_PROP);
      client.instanceOperations().setProperty(PERTABLE_CHOOSER_PROP,
          FairVolumeChooser.class.getName());
    }
  }

  @After
  public void resetProperty() throws Exception {
    if (origPropValue != null) {
      try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
        client.instanceOperations().setProperty(PERTABLE_CHOOSER_PROP, origPropValue);
      }
    }
  }

  @Test
  public void experimentalPropTest() throws Exception {
    // ensure experimental props do not show up in config output unless set

    AuthenticationToken token = getAdminToken();
    File clientPropsFile = null;
    switch (getClusterType()) {
      case MINI:
        MiniAccumuloClusterImpl mac = (MiniAccumuloClusterImpl) getCluster();
        clientPropsFile = mac.getConfig().getClientPropsFile();
        break;
      case STANDALONE:
        StandaloneAccumuloClusterConfiguration standaloneConf =
            (StandaloneAccumuloClusterConfiguration) getClusterConfiguration();
        clientPropsFile = standaloneConf.getClientPropsFile();
        break;
      default:
        fail("Unknown cluster type");
    }

    assertNotNull(clientPropsFile);

    TestShell ts = null;
    if (token instanceof PasswordToken) {
      String passwd = new String(((PasswordToken) token).getPassword(), UTF_8);
      ts = new TestShell(getAdminPrincipal(), passwd, getCluster().getInstanceName(),
          getCluster().getZooKeepers(), clientPropsFile);
    } else if (token instanceof KerberosToken) {
      ts = new TestShell(getAdminPrincipal(), null, getCluster().getInstanceName(),
          getCluster().getZooKeepers(), clientPropsFile);
    } else {
      fail("Unknown token type");
    }

    assertTrue(Property.TABLE_CRYPTO_PREFIX.isExperimental());
    assertTrue(Property.TABLE_CRYPTO_SERVICE.isExperimental());
    assertTrue(Property.TSERV_WAL_CRYPTO_PREFIX.isExperimental());
    assertTrue(Property.TSERV_WAL_CRYPTO_SERVICE.isExperimental());

    String configOutput = ts.exec("config");

    assertTrue(configOutput.contains(PERTABLE_CHOOSER_PROP));
    assertFalse(configOutput.contains(Property.TABLE_CRYPTO_SERVICE.getKey()));
    assertFalse(configOutput.contains(Property.TSERV_WAL_CRYPTO_SERVICE.getKey()));
  }
}
