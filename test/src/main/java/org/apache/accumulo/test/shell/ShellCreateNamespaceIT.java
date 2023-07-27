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
package org.apache.accumulo.test.shell;

import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;
import static org.apache.accumulo.harness.AccumuloITBase.SUNNY_DAY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MINI_CLUSTER_ONLY)
@Tag(SUNNY_DAY)
public class ShellCreateNamespaceIT extends SharedMiniClusterBase {

  private MockShell ts;

  private static class SSCTITCallback implements MiniClusterConfigurationCallback {
    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
      // Only one tserver to avoid race conditions on ZK propagation (auths and configuration)
      cfg.setNumTservers(1);
      // Set the min span to 0 so we will definitely get all the traces back. See ACCUMULO-4365
      Map<String,String> siteConf = cfg.getSiteConfig();
      cfg.setSiteConfig(siteConf);
    }
  }

  @BeforeAll
  public static void setupMiniCluster() throws Exception {
    SharedMiniClusterBase.startMiniClusterWithConfig(new SSCTITCallback());
  }

  @BeforeEach
  public void setupShell() throws Exception {
    ts = new MockShell(getPrincipal(), getRootPassword(),
        getCluster().getConfig().getInstanceName(), getCluster().getConfig().getZooKeepers(),
        getCluster().getConfig().getClientPropsFile());
  }

  @AfterAll
  public static void tearDownAfterAll() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @AfterEach
  public void tearDownShell() {
    ts.shell.shutdown();
  }

  @Test
  public void createSimpleTest() throws Exception {
    final String namespace = getUniqueNames(1)[0];
    ts.exec("createnamespace " + namespace, true);
    try (AccumuloClient accumuloClient = Accumulo.newClient().from(getClientProps()).build()) {
      assertTrue(accumuloClient.namespaceOperations().exists(namespace));
      ts.exec("deletenamespace -f " + namespace);
    }
  }

  @Test
  public void copyConfigTest() throws Exception {

    final String sysPropName = "table.custom.my_sys_prop";
    final String sysPropVal1 = "sys_v1";
    final String sysPropVal2 = "sys_v2";

    final String nsPropName = "table.custom.my_ns_prop";
    final String nsPropVal1 = "ns_v1";

    final String[] names = getUniqueNames(2);
    final String ns1 = names[0];
    final String ns2 = names[1];

    ts.exec("createnamespace " + ns1, true);
    ts.exec("config -s " + sysPropName + "=" + sysPropVal1);
    ts.exec("config -s " + nsPropName + "=" + nsPropVal1 + " -ns " + ns1);

    ts.exec("createnamespace -cc " + ns1 + " " + ns2, true);

    ts.exec("config -s " + sysPropName + "=" + sysPropVal2);

    try (AccumuloClient accumuloClient = Accumulo.newClient().from(getClientProps()).build()) {
      assertTrue(accumuloClient.namespaceOperations().exists(ns1));
      assertTrue(accumuloClient.namespaceOperations().exists(ns2));

      // prop not set on namespace
      Map<String,String> p1 = accumuloClient.namespaceOperations().getNamespaceProperties(ns1);
      assertNull(p1.get(sysPropName));
      assertEquals(nsPropVal1, p1.get(nsPropName));
      // copied config will have copied the property and will override
      Map<String,String> p2 = accumuloClient.namespaceOperations().getNamespaceProperties(ns2);
      assertEquals(sysPropVal1, p2.get(sysPropName));

      // p2 will have configuration props in addition to custom prop
      assertTrue(p2.size() > p1.size());

      ts.exec("deletenamespace -f " + ns1);
      ts.exec("deletenamespace -f " + ns2);
    }
  }

  @Test
  public void copyPropertiesTest() throws Exception {

    final String sysPropName = "table.custom.my_sys_prop";
    final String sysPropVal1 = "sys_v1";
    final String sysPropVal2 = "sys_v2";

    final String nsPropName = "table.custom.my_ns_prop";
    final String nsPropVal1 = "ns_v1";

    final String[] names = getUniqueNames(2);
    final String srcNs = names[0];
    final String destNs = names[1];

    ts.exec("createnamespace " + srcNs, true);
    ts.exec("config -s " + sysPropName + "=" + sysPropVal1);
    ts.exec("config -s " + nsPropName + "=" + nsPropVal1 + " -ns " + srcNs);

    ts.exec("createnamespace --exclude-parent-properties -cc " + srcNs + " " + destNs, true);

    ts.exec("config -s " + sysPropName + "=" + sysPropVal2);

    try (AccumuloClient accumuloClient = Accumulo.newClient().from(getClientProps()).build()) {
      assertTrue(accumuloClient.namespaceOperations().exists(srcNs));
      assertTrue(accumuloClient.namespaceOperations().exists(destNs));

      // props not set on source namespace
      Map<String,String> srcProps =
          accumuloClient.namespaceOperations().getNamespaceProperties(srcNs);
      assertNull(srcProps.get(sysPropName));
      assertEquals(nsPropVal1, srcProps.get(nsPropName));

      // copied props will not have copied the configuration properties
      Map<String,String> copiedProps =
          accumuloClient.namespaceOperations().getNamespaceProperties(destNs);
      assertNull(copiedProps.get(sysPropName));
      assertEquals(nsPropVal1, copiedProps.get(nsPropName));
      // p2 will have same namespace props
      assertEquals(copiedProps, srcProps);

      Map<String,String> config = accumuloClient.namespaceOperations().getConfiguration(destNs);
      // because not copied, dest ns sees system change
      assertEquals(sysPropVal2, config.get(sysPropName));

      ts.exec("deletenamespace -f " + srcNs);
      ts.exec("deletenamespace -f " + destNs);
    }
  }

  @Test
  public void copyTablePropsInvalidOptsTest() throws Exception {
    String[] names = getUniqueNames(2);

    ts.exec("createnamespace " + names[0]);
    ts.exec("createnamespace " + names[1]);

    // test --exclude-parent requires -cc option - expect this fail
    ts.exec("createnamespace --exclude-parent-properties " + names[0] + " " + names[1], false);
  }

  @Test
  public void missingSrcCopyPropsTest() throws Exception {
    String[] names = getUniqueNames(2);
    // test -cc and -cp are mutually exclusive expect this fail
    ts.exec("createnamespace -cp " + names[0] + " " + names[1], false);
  }

  @Test
  public void missingSrcCopyConfigTest() throws Exception {
    String[] names = getUniqueNames(2);
    // test command fail if src is not available
    ts.exec("createnamespace -cc " + names[0] + " " + names[1], false);
  }

  @Test
  public void destExistsTest() throws Exception {
    String[] names = getUniqueNames(2);

    ts.exec("createnamespace " + names[0]);
    ts.exec("createnamespace " + names[1]);
    // expect to fail because target already exists
    ts.exec("createnamespace -cp " + names[0] + " " + names[1], false);
  }

  @Test
  public void duplicateNamespaceTest() throws Exception {
    String[] names = getUniqueNames(2);

    ts.exec("createnamespace " + names[0]);
    // expect to fail because target already exists
    ts.exec("createnamespace " + names[0], false);
  }

}
