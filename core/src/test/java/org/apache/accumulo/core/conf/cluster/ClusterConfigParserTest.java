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

import java.io.File;
import java.net.URL;
import java.util.Map;

import org.junit.Test;

public class ClusterConfigParserTest {

  @Test
  public void testParse() throws Exception {
    URL configFile = ClusterConfigParserTest.class.getResource("/cluster.yml");
    assertNotNull(configFile);

    Map<String,String> contents =
        ClusterConfigParser.parseConfiguration(new File(configFile.toURI()).getAbsolutePath());
    assertEquals(5, contents.size());
    assertTrue(contents.containsKey("managers"));
    assertEquals("localhost1,localhost2", contents.get("managers"));
    assertTrue(contents.containsKey("monitors"));
    assertEquals("localhost1,localhost2", contents.get("monitors"));
    assertTrue(contents.containsKey("tracer"));
    assertEquals("localhost", contents.get("tracer"));
    assertTrue(contents.containsKey("gc"));
    assertEquals("localhost", contents.get("gc"));
    assertTrue(contents.containsKey("tservers"));
    assertEquals("localhost1,localhost2,localhost3,localhost4", contents.get("tservers"));
    assertFalse(contents.containsKey("compaction"));
    assertFalse(contents.containsKey("compaction.coordinators"));
    assertFalse(contents.containsKey("compaction.compactors"));
    assertFalse(contents.containsKey("compaction.compactors.queues"));
    assertFalse(contents.containsKey("compaction.compactors.q1"));
    assertFalse(contents.containsKey("compaction.compactors.q2"));
  }

  @Test
  public void testParseWithExternalCompactions() throws Exception {
    URL configFile =
        ClusterConfigParserTest.class.getResource("/cluster-with-external-compactions.yml");
    assertNotNull(configFile);

    Map<String,String> contents =
        ClusterConfigParser.parseConfiguration(new File(configFile.toURI()).getAbsolutePath());
    assertEquals(9, contents.size());
    assertTrue(contents.containsKey("managers"));
    assertEquals("localhost1,localhost2", contents.get("managers"));
    assertTrue(contents.containsKey("monitors"));
    assertEquals("localhost1,localhost2", contents.get("monitors"));
    assertTrue(contents.containsKey("tracer"));
    assertEquals("localhost", contents.get("tracer"));
    assertTrue(contents.containsKey("gc"));
    assertEquals("localhost", contents.get("gc"));
    assertTrue(contents.containsKey("tservers"));
    assertEquals("localhost1,localhost2,localhost3,localhost4", contents.get("tservers"));
    assertFalse(contents.containsKey("compaction"));
    assertTrue(contents.containsKey("compaction.coordinators"));
    assertEquals("localhost1,localhost2", contents.get("compaction.coordinators"));
    assertFalse(contents.containsKey("compaction.compactors"));
    assertTrue(contents.containsKey("compaction.compactors.queues"));
    assertEquals("q1,q2", contents.get("compaction.compactors.queues"));
    assertTrue(contents.containsKey("compaction.compactors.q1"));
    assertEquals("localhost1,localhost2", contents.get("compaction.compactors.q1"));
    assertTrue(contents.containsKey("compaction.compactors.q2"));
    assertEquals("localhost1,localhost2", contents.get("compaction.compactors.q2"));
  }

}
