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
package org.apache.accumulo.test.functional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;

import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.server.conf.cluster.ClusterConfigParser;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class ClusterConfigParserIT_SimpleSuite extends SharedMiniClusterBase {

  @TempDir
  private static Path tempDir;

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Test
  @Order(1)
  public void testMissingResourceGroupsFails() {
    assertThrows(RuntimeException.class, () -> testShellOutputMain(false));
  }

  @Test
  @Order(2)
  public void testCreateResourceGroups() throws Exception {
    Set<ResourceGroupId> grps = getCluster().getServerContext().resourceGroupOperations().list();
    assertEquals(1, grps.size());
    assert (grps.contains(ResourceGroupId.DEFAULT));
    testShellOutputMain(true);
    grps = getCluster().getServerContext().resourceGroupOperations().list();
    assertEquals(5, grps.size());
  }

  public void testShellOutputMain(final boolean create) throws Exception {

    // Test that the main method in ClusterConfigParser properly parses the configuration
    // and outputs to a given file instead of System.out when provided
    testShellOutput(configFile -> {
      try {
        Path outputFile = tempDir.resolve(testName());
        ClusterConfigParser.siteConf = getCluster().getServerContext().getSiteConfiguration();
        ClusterConfigParser.main(new String[] {Boolean.toString(create), configFile.getFile(),
            outputFile.toAbsolutePath().toString()});

        return outputFile;
      } catch (IOException e) {
        throw new RuntimeException(e.getMessage(), e);
      } finally {
        ClusterConfigParser.siteConf = null;
      }
    });
  }

  private static void testShellOutput(Function<URL,Path> outputConfigFunction) throws Exception {
    final URL configFile = ClusterConfigParserIT_SimpleSuite.class
        .getResource("/org/apache/accumulo/test/cluster.yaml");
    assertNotNull(configFile);

    final Path f = outputConfigFunction.apply(configFile);

    Map<String,String> expected = new TreeMap<>();
    expected.put("MANAGER_HOSTS", "localhost1 localhost2");
    expected.put("MONITOR_HOSTS", "localhost1 localhost2");
    expected.put("GC_HOSTS", "localhost");

    expected.put("COMPACTOR_GROUPS", "q1 q2");
    expected.put("COMPACTORS_PER_HOST_q1", "1");
    expected.put("COMPACTOR_HOSTS_q1", "localhost1 localhost2");
    expected.put("COMPACTORS_PER_HOST_q2", "4");
    expected.put("COMPACTOR_HOSTS_q2", "localhost3 localhost4");

    expected.put("SSERVER_GROUPS", "cheap default highmem");
    expected.put("SSERVERS_PER_HOST_default", "2");
    expected.put("SSERVER_HOSTS_default", "localhost1 localhost2");
    expected.put("SSERVERS_PER_HOST_highmem", "1");
    expected.put("SSERVER_HOSTS_highmem", "hmvm1 hmvm2 hmvm3");
    expected.put("SSERVERS_PER_HOST_cheap", "3");
    expected.put("SSERVER_HOSTS_cheap", "burstyvm1 burstyvm2");

    expected.put("TSERVER_GROUPS", "default");
    expected.put("TSERVERS_PER_HOST_default", "2");
    expected.put("TSERVER_HOSTS_default", "localhost1 localhost2 localhost3 localhost4");

    expected.replaceAll((k, v) -> '"' + v + '"');

    Map<String,String> actual = new TreeMap<>();
    try (BufferedReader rdr = Files.newBufferedReader(f)) {
      rdr.lines().forEach(l -> {
        String[] props = l.split("=", 2);
        actual.put(props[0], props[1]);
      });
    }

    assertEquals(expected, actual);
  }

}
