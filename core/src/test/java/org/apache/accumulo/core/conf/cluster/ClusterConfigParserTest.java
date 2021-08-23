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
package org.apache.accumulo.core.conf.cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.PrintStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths provided by test")
public class ClusterConfigParserTest {

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void testParse() throws Exception {
    URL configFile = ClusterConfigParserTest.class
        .getResource("/org/apache/accumulo/core/conf/cluster/cluster.yml");
    assertNotNull(configFile);

    Map<String,String> contents =
        ClusterConfigParser.parseConfiguration(new File(configFile.toURI()).getAbsolutePath());
    assertEquals(5, contents.size());
    assertTrue(contents.containsKey("manager"));
    assertEquals("localhost1 localhost2", contents.get("manager"));
    assertTrue(contents.containsKey("monitor"));
    assertEquals("localhost1 localhost2", contents.get("monitor"));
    assertTrue(contents.containsKey("tracer"));
    assertEquals("localhost", contents.get("tracer"));
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
  public void testParseWithExternalCompactions() throws Exception {
    URL configFile = ClusterConfigParserTest.class.getResource(
        "/org/apache/accumulo/core/conf/cluster/cluster-with-external-compactions.yml");
    assertNotNull(configFile);

    Map<String,String> contents =
        ClusterConfigParser.parseConfiguration(new File(configFile.toURI()).getAbsolutePath());
    assertEquals(9, contents.size());
    assertTrue(contents.containsKey("manager"));
    assertEquals("localhost1 localhost2", contents.get("manager"));
    assertTrue(contents.containsKey("monitor"));
    assertEquals("localhost1 localhost2", contents.get("monitor"));
    assertTrue(contents.containsKey("tracer"));
    assertEquals("localhost", contents.get("tracer"));
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
  }

  @Test
  public void testShellOutput() throws Exception {

    File f = tmp.newFile();
    f.deleteOnExit();

    PrintStream ps = new PrintStream(f);

    URL configFile = ClusterConfigParserTest.class.getResource(
        "/org/apache/accumulo/core/conf/cluster/cluster-with-external-compactions.yml");
    assertNotNull(configFile);

    Map<String,String> contents =
        ClusterConfigParser.parseConfiguration(new File(configFile.toURI()).getAbsolutePath());

    ClusterConfigParser.outputShellVariables(contents, ps);
    ps.close();

    Map<String,String> expected = new HashMap<>();
    expected.put("MANAGER_HOSTS", "\"localhost1 localhost2\"");
    expected.put("MONITOR_HOSTS", "\"localhost1 localhost2\"");
    expected.put("TRACER_HOSTS", "\"localhost\"");
    expected.put("GC_HOSTS", "\"localhost\"");
    expected.put("TSERVER_HOSTS", "\"localhost1 localhost2 localhost3 localhost4\"");
    expected.put("COORDINATOR_HOSTS", "\"localhost1 localhost2\"");
    expected.put("COMPACTION_QUEUES", "\"q1 q2\"");
    expected.put("COMPACTOR_HOSTS_q1", "\"localhost1 localhost2\"");
    expected.put("COMPACTOR_HOSTS_q2", "\"localhost1 localhost2\"");

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
