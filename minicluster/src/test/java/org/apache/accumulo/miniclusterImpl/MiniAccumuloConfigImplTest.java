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
package org.apache.accumulo.miniclusterImpl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.ServerType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths not set by user input")
public class MiniAccumuloConfigImplTest {

  @TempDir
  private static File tempFolder;

  @Test
  public void testZookeeperPort() {

    // set specific zookeeper port
    MiniAccumuloConfigImpl config =
        new MiniAccumuloConfigImpl(tempFolder, "password").setZooKeeperPort(5000).initialize();
    assertEquals(5000, config.getZooKeeperPort());

    // generate zookeeper port
    config = new MiniAccumuloConfigImpl(tempFolder, "password").initialize();
    assertTrue(config.getZooKeeperPort() > 0);
  }

  @Test
  public void testZooKeeperStartupTime() {

    // set specific zookeeper startup time
    MiniAccumuloConfigImpl config = new MiniAccumuloConfigImpl(tempFolder, "password")
        .setZooKeeperStartupTime(5000).initialize();
    assertEquals(5000, config.getZooKeeperStartupTime());
  }

  @Test
  public void testSiteConfig() {

    // constructor site config overrides default props
    Map<String,String> siteConfig = new HashMap<>();
    siteConfig.put(Property.INSTANCE_VOLUMES.getKey(), "hdfs://");
    MiniAccumuloConfigImpl config =
        new MiniAccumuloConfigImpl(tempFolder, "password").setSiteConfig(siteConfig).initialize();
    assertEquals("hdfs://", config.getSiteConfig().get(Property.INSTANCE_VOLUMES.getKey()));
  }

  @Test
  public void testMemoryConfig() {

    MiniAccumuloConfigImpl config = new MiniAccumuloConfigImpl(tempFolder, "password").initialize();
    config.setDefaultMemory(96, MemoryUnit.MEGABYTE);
    assertEquals(96 * 1024 * 1024L, config.getMemory(ServerType.MANAGER));
    assertEquals(96 * 1024 * 1024L, config.getMemory(ServerType.TABLET_SERVER));
    assertEquals(96 * 1024 * 1024L, config.getDefaultMemory());
    config.setMemory(ServerType.MANAGER, 256, MemoryUnit.MEGABYTE);
    assertEquals(256 * 1024 * 1024L, config.getMemory(ServerType.MANAGER));
    assertEquals(96 * 1024 * 1024L, config.getDefaultMemory());
    assertEquals(96 * 1024 * 1024L, config.getMemory(ServerType.TABLET_SERVER));
  }

}
