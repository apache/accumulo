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
package org.apache.accumulo.core.conf.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.PrintStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths provided by test")
public class ClusterConfigParserTest {

  @Test
  public void testParse() throws Exception {
    URL configFile = ClusterConfigParserTest.class
        .getResource("/org/apache/accumulo/core/conf/cluster/cluster.yaml");
    assertNotNull(configFile);

    Map<String,String> contents =
        ClusterConfigParser.parseConfiguration(new File(configFile.toURI()).getAbsolutePath());
    assertEquals(4, contents.size());
    assertTrue(contents.containsKey("manager"));
    assertEquals("localhost1 localhost2", contents.get("manager"));
    assertTrue(contents.containsKey("monitor"));
    assertEquals("localhost1 localhost2", contents.get("monitor"));
    assertTrue(contents.containsKey("gc"));
    assertEquals("localhost", contents.get("gc"));
    assertTrue(contents.containsKey("tserver"));
    assertEquals("localhost1 localhost2 localhost3 localhost4", contents.get("tserver"));
    assertFalse(contents.containsKey("compaction"));
    assertFalse(contents.containsKey("compaction.coordinator"));
    assertFalse(contents.containsKey("compaction.compactor"));
    assertFalse(contents.containsKey("compaction.compactor.queue"));
    assertFalse(contents.containsKey("compaction.compactor.q1"));
    assertFalse(contents.containsKey("compaction.compactor.q2"));
  }

  @Test
  public void testParseWithOptionalComponents() throws Exception {
    URL configFile = ClusterConfigParserTest.class
        .getResource("/org/apache/accumulo/core/conf/cluster/cluster-with-optional-services.yaml");
    assertNotNull(configFile);

    Map<String,String> contents =
        ClusterConfigParser.parseConfiguration(new File(configFile.toURI()).getAbsolutePath());
    assertEquals(10, contents.size());
    assertTrue(contents.containsKey("manager"));
    assertEquals("localhost1 localhost2", contents.get("manager"));
    assertTrue(contents.containsKey("monitor"));
    assertEquals("localhost1 localhost2", contents.get("monitor"));
    assertTrue(contents.containsKey("gc"));
    assertEquals("localhost", contents.get("gc"));
    assertTrue(contents.containsKey("tserver"));
    assertEquals("localhost1 localhost2 localhost3 localhost4", contents.get("tserver"));
    assertFalse(contents.containsKey("compaction"));
    assertTrue(contents.containsKey("compaction.coordinator"));
    assertEquals("localhost1 localhost2", contents.get("compaction.coordinator"));
    assertFalse(contents.containsKey("compaction.compactor"));
    assertTrue(contents.containsKey("compaction.compactor.queue"));
    assertEquals("q1 q2", contents.get("compaction.compactor.queue"));
    assertTrue(contents.containsKey("compaction.compactor.q1"));
    assertEquals("localhost1 localhost2", contents.get("compaction.compactor.q1"));
    assertTrue(contents.containsKey("compaction.compactor.q2"));
    assertEquals("localhost1 localhost2", contents.get("compaction.compactor.q2"));
    assertFalse(contents.containsKey("sserver"));
    assertTrue(contents.containsKey("sserver.group"));
    assertEquals("default", contents.get("sserver.group"));
    assertTrue(contents.containsKey("sserver.default"));
    assertEquals("localhost1 localhost2", contents.get("sserver.default"));
  }

  @Test
  public void testShellOutput() throws Exception {

    String userDir = System.getProperty("user.dir");
    String targetDir = "target";
    File dir = new File(userDir, targetDir);
    if (!dir.exists()) {
      if (!dir.mkdirs()) {
        fail("Unable to make directory ${user.dir}/target");
      }
    }
    File f = new File(dir, "ClusterConfigParserTest_testShellOutput");
    if (!f.createNewFile()) {
      fail("Unable to create file in ${user.dir}/target");
    }
    f.deleteOnExit();

    PrintStream ps = new PrintStream(f);

    URL configFile = ClusterConfigParserTest.class
        .getResource("/org/apache/accumulo/core/conf/cluster/cluster-with-optional-services.yaml");
    assertNotNull(configFile);

    Map<String,String> contents =
        ClusterConfigParser.parseConfiguration(new File(configFile.toURI()).getAbsolutePath());

    ClusterConfigParser.outputShellVariables(contents, ps);
    ps.close();

    Map<String,
        String> expected = Map.of("MANAGER_HOSTS", "\"localhost1 localhost2\"", "MONITOR_HOSTS",
            "\"localhost1 localhost2\"", "GC_HOSTS", "\"localhost\"", "TSERVER_HOSTS",
            "\"localhost1 localhost2 localhost3 localhost4\"", "COORDINATOR_HOSTS",
            "\"localhost1 localhost2\"", "COMPACTION_QUEUES", "\"q1 q2\"", "COMPACTOR_HOSTS_q1",
            "\"localhost1 localhost2\"", "COMPACTOR_HOSTS_q2", "\"localhost1 localhost2\"",
            "SSERVER_GROUPS", "\"default\"", "SSERVER_HOSTS_default", "\"localhost1 localhost2\"");

    Map<String,String> actual = new HashMap<>();
    try (BufferedReader rdr = Files.newBufferedReader(Paths.get(f.toURI()))) {
      rdr.lines().forEach(l -> {
        String[] props = l.split("=");
        actual.put(props[0], props[1]);
      });
    }

    assertEquals(expected, actual);
  }

}
