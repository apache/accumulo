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
package org.apache.accumulo.miniclusterImpl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.ServerType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths not set by user input")
public class MiniAccumuloConfigImplTest {

  @SuppressWarnings("deprecation")
  private static final Property INSTANCE_DFS_URI = Property.INSTANCE_DFS_URI;

  static TemporaryFolder tempFolder =
      new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));

  @BeforeClass
  public static void setUp() throws IOException {
    tempFolder.create();
  }

  @Test
  public void testZookeeperPort() {

    // set specific zookeeper port
    MiniAccumuloConfigImpl config = new MiniAccumuloConfigImpl(tempFolder.getRoot(), "password")
        .setZooKeeperPort(5000).initialize();
    assertEquals(5000, config.getZooKeeperPort());

    // generate zookeeper port
    config = new MiniAccumuloConfigImpl(tempFolder.getRoot(), "password").initialize();
    assertTrue(config.getZooKeeperPort() > 0);
  }

  @Test
  public void testZooKeeperStartupTime() {

    // set specific zookeeper startup time
    MiniAccumuloConfigImpl config = new MiniAccumuloConfigImpl(tempFolder.getRoot(), "password")
        .setZooKeeperStartupTime(5000).initialize();
    assertEquals(5000, config.getZooKeeperStartupTime());
  }

  @Test
  public void testSiteConfig() {

    // constructor site config overrides default props
    Map<String,String> siteConfig = new HashMap<>();
    siteConfig.put(INSTANCE_DFS_URI.getKey(), "hdfs://");
    MiniAccumuloConfigImpl config = new MiniAccumuloConfigImpl(tempFolder.getRoot(), "password")
        .setSiteConfig(siteConfig).initialize();
    assertEquals("hdfs://", config.getSiteConfig().get(INSTANCE_DFS_URI.getKey()));
  }

  @Test
  public void testMemoryConfig() {

    MiniAccumuloConfigImpl config =
        new MiniAccumuloConfigImpl(tempFolder.getRoot(), "password").initialize();
    config.setDefaultMemory(96, MemoryUnit.MEGABYTE);
    assertEquals(96 * 1024 * 1024L, config.getMemory(ServerType.MASTER));
    assertEquals(96 * 1024 * 1024L, config.getMemory(ServerType.TABLET_SERVER));
    assertEquals(96 * 1024 * 1024L, config.getDefaultMemory());
    config.setMemory(ServerType.MASTER, 256, MemoryUnit.MEGABYTE);
    assertEquals(256 * 1024 * 1024L, config.getMemory(ServerType.MASTER));
    assertEquals(96 * 1024 * 1024L, config.getDefaultMemory());
    assertEquals(96 * 1024 * 1024L, config.getMemory(ServerType.TABLET_SERVER));
  }

  @AfterClass
  public static void tearDown() {
    tempFolder.delete();
  }
}
