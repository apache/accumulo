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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths provided by test")
public class ClusterConfigParserTest {

  @TempDir
  private static File tempDir;

  @Test
  public void testParse() throws Exception {
    URL configFile = ClusterConfigParserTest.class
        .getResource("/org/apache/accumulo/core/conf/cluster/cluster.yaml");
    assertNotNull(configFile);

    Map<String,String> contents =
        ClusterConfigParser.parseConfiguration(new File(configFile.toURI()).getAbsolutePath());
    assertEquals(14, contents.size());
    assertTrue(contents.containsKey("manager"));
    assertEquals("localhost1 localhost2", contents.get("manager"));
    assertTrue(contents.containsKey("monitor"));
    assertEquals("localhost1 localhost2", contents.get("monitor"));
    assertTrue(contents.containsKey("gc"));
    assertEquals("localhost", contents.get("gc"));

    assertFalse(contents.containsKey("compactor"));
    assertFalse(contents.containsKey("compactor.q1"));
    assertFalse(contents.containsKey("compactor.q2"));
    assertFalse(contents.containsKey("compactor.q1.servers_per_host"));
    assertTrue(contents.containsKey("compactor.q1.hosts"));
    assertEquals("localhost1 localhost2", contents.get("compactor.q1.hosts"));
    assertTrue(contents.containsKey("compactor.q2.servers_per_host"));
    assertEquals("4", contents.get("compactor.q2.servers_per_host"));
    assertTrue(contents.containsKey("compactor.q2.hosts"));
    assertEquals("localhost3 localhost4", contents.get("compactor.q2.hosts"));

    assertFalse(contents.containsKey("sserver"));
    assertFalse(contents.containsKey("sserver.default"));
    assertTrue(contents.containsKey("sserver.default.servers_per_host"));
    assertEquals("2", contents.get("sserver.default.servers_per_host"));
    assertTrue(contents.containsKey("sserver.default.hosts"));
    assertEquals("localhost1 localhost2", contents.get("sserver.default.hosts"));
    assertFalse(contents.containsKey("sserver.highmem"));
    assertTrue(contents.containsKey("sserver.highmem.servers_per_host"));
    assertEquals("1", contents.get("sserver.highmem.servers_per_host"));
    assertTrue(contents.containsKey("sserver.highmem.hosts"));
    assertEquals("hmvm1 hmvm2 hmvm3", contents.get("sserver.highmem.hosts"));
    assertFalse(contents.containsKey("sserver.cheap"));
    assertTrue(contents.containsKey("sserver.cheap.servers_per_host"));
    assertEquals("3", contents.get("sserver.cheap.servers_per_host"));
    assertTrue(contents.containsKey("sserver.cheap.hosts"));
    assertEquals("burstyvm1 burstyvm2", contents.get("sserver.cheap.hosts"));

    assertFalse(contents.containsKey("tserver"));
    assertFalse(contents.containsKey("tserver.default"));
    assertTrue(contents.containsKey("tserver.default.servers_per_host"));
    assertEquals("2", contents.get("tserver.default.servers_per_host"));
    assertTrue(contents.containsKey("tserver.default.hosts"));
    assertEquals("localhost1 localhost2 localhost3 localhost4",
        contents.get("tserver.default.hosts"));
    assertFalse(contents.containsKey("compactor"));
    assertFalse(contents.containsKey("compactor.queue"));
    assertFalse(contents.containsKey("tservers_per_host"));
    assertFalse(contents.containsKey("sservers_per_host"));
    assertFalse(contents.containsKey("compactors_per_host"));
  }

  @Test
  public void testShellOutput() throws Exception {

    testShellOutput(configFile -> {
      try {
        final Map<String,String> contents =
            ClusterConfigParser.parseConfiguration(new File(configFile.toURI()).getAbsolutePath());

        final File outputFile = new File(tempDir, "ClusterConfigParserTest_testShellOutput");
        if (!outputFile.createNewFile()) {
          fail("Unable to create file in " + tempDir);
        }
        outputFile.deleteOnExit();

        final PrintStream ps = new PrintStream(outputFile);
        ClusterConfigParser.outputShellVariables(contents, ps);
        ps.close();

        return outputFile;
      } catch (Exception e) {
        throw new RuntimeException(e.getMessage(), e);
      }
    });
  }

  @Test
  public void testShellOutputMain() throws Exception {

    // Test that the main method in ClusterConfigParser properly parses the configuration
    // and outputs to a given file instead of System.out when provided
    testShellOutput(configFile -> {
      try {
        File outputFile = new File(tempDir, "ClusterConfigParserTest_testShellOutputMain");
        ClusterConfigParser.main(new String[] {configFile.getFile(), outputFile.getAbsolutePath()});

        return outputFile;
      } catch (IOException e) {
        throw new RuntimeException(e.getMessage(), e);
      }
    });
  }

  private void testShellOutput(Function<URL,File> outputConfigFunction) throws Exception {
    final URL configFile = ClusterConfigParserTest.class
        .getResource("/org/apache/accumulo/core/conf/cluster/cluster.yaml");
    assertNotNull(configFile);

    final File f = outputConfigFunction.apply(configFile);

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
    try (BufferedReader rdr = Files.newBufferedReader(Paths.get(f.toURI()))) {
      rdr.lines().forEach(l -> {
        String[] props = l.split("=", 2);
        actual.put(props[0], props[1]);
      });
    }

    assertEquals(expected, actual);
  }

  @Test
  public void testFileWithUnknownSections() throws Exception {
    URL configFile = ClusterConfigParserTest.class
        .getResource("/org/apache/accumulo/core/conf/cluster/bad-server-name.yaml");
    assertNotNull(configFile);

    Map<String,String> contents =
        ClusterConfigParser.parseConfiguration(new File(configFile.toURI()).getAbsolutePath());

    try (var baos = new ByteArrayOutputStream(); var ps = new PrintStream(baos)) {
      var exception = assertThrows(IllegalArgumentException.class,
          () -> ClusterConfigParser.outputShellVariables(contents, ps));
      assertTrue(exception.getMessage().contains("vserver"));
    }
  }

  @Test
  public void testFileWithInvalidGroupName() throws Exception {
    URL configFile = ClusterConfigParserTest.class
        .getResource("/org/apache/accumulo/core/conf/cluster/bad-group-name.yaml");
    assertNotNull(configFile);

    Map<String,String> contents =
        ClusterConfigParser.parseConfiguration(new File(configFile.toURI()).getAbsolutePath());

    try (var baos = new ByteArrayOutputStream(); var ps = new PrintStream(baos)) {
      var exception = assertThrows(RuntimeException.class,
          () -> ClusterConfigParser.outputShellVariables(contents, ps));
      assertTrue(exception.getMessage().contains("bad-group-name"));
    }
  }

  @Test
  public void testFileWithInvalidSuffix() throws Exception {
    URL configFile = ClusterConfigParserTest.class
        .getResource("/org/apache/accumulo/core/conf/cluster/bad-group-suffix.yaml");
    assertNotNull(configFile);

    Map<String,String> contents =
        ClusterConfigParser.parseConfiguration(new File(configFile.toURI()).getAbsolutePath());

    try (var baos = new ByteArrayOutputStream(); var ps = new PrintStream(baos)) {
      var exception = assertThrows(IllegalArgumentException.class,
          () -> ClusterConfigParser.outputShellVariables(contents, ps));
      assertTrue(exception.getMessage().contains("servers_per_hosts"));
    }
  }

  @Test
  public void testFileWithTooManyLevels() throws Exception {
    URL configFile = ClusterConfigParserTest.class
        .getResource("/org/apache/accumulo/core/conf/cluster/too-many-levels.yaml");
    assertNotNull(configFile);

    Map<String,String> contents =
        ClusterConfigParser.parseConfiguration(new File(configFile.toURI()).getAbsolutePath());

    try (var baos = new ByteArrayOutputStream(); var ps = new PrintStream(baos)) {
      var exception = assertThrows(IllegalArgumentException.class,
          () -> ClusterConfigParser.outputShellVariables(contents, ps));
      assertTrue(exception.getMessage().contains("too many levels"));
    }
  }

  @Test
  public void testGroupNamePattern() {
    ClusterConfigParser.validateGroupNames(List.of("a"));
    ClusterConfigParser.validateGroupNames(List.of("a", "b"));
    ClusterConfigParser.validateGroupNames(List.of("default", "reg_ular"));
    ClusterConfigParser.validateGroupNames(List.of("a1b2c3d4__"));
    assertThrows(RuntimeException.class,
        () -> ClusterConfigParser.validateGroupNames(List.of("0abcde")));
    assertThrows(RuntimeException.class,
        () -> ClusterConfigParser.validateGroupNames(List.of("a-b")));
    assertThrows(RuntimeException.class,
        () -> ClusterConfigParser.validateGroupNames(List.of("a*b")));
    assertThrows(RuntimeException.class,
        () -> ClusterConfigParser.validateGroupNames(List.of("a?b")));
  }
}
