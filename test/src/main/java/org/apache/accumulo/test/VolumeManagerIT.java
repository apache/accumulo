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
package org.apache.accumulo.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.functional.ReadWriteIT;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.jupiter.api.Test;

public class VolumeManagerIT extends ConfigurableMacBase {

  private String vol1;
  private String vol2;

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.useMiniDFS(true);
    cfg.setPreStartConfigProcessor((config) -> {
      vol1 = config.getSiteConfig().get(Property.INSTANCE_VOLUMES.getKey());
      assertTrue(vol1.contains("localhost"));
      vol2 = vol1.replace("localhost", "127.0.0.1");
      config.setProperty(Property.INSTANCE_VOLUMES.getKey(), String.join(",", vol2, vol1));

      // Set Volume specific HDFS overrides
      config.setProperty("general.volume.chooser",
          "org.apache.accumulo.core.spi.fs.PreferredVolumeChooser");
      config.setProperty("general.custom.volume.preferred.default", vol1);
      config.setProperty("general.custom.volume.preferred.logger", vol2);

      // Need to escape the colons in the custom property volume because it's part of the key. It's
      // being written out to a file and read in using the Properties object.
      String vol1FileOutput = vol1.replaceAll(":", "\\\\:");
      String vol2FileOutput = vol2.replaceAll(":", "\\\\:");
      config.setProperty(
          Property.INSTANCE_VOLUME_CONFIG_PREFIX.getKey() + vol1FileOutput + ".dfs.blocksize",
          "10485760");
      config.setProperty(
          Property.INSTANCE_VOLUME_CONFIG_PREFIX.getKey() + vol2FileOutput + ".dfs.blocksize",
          "51200000");
      config.setProperty(Property.INSTANCE_VOLUME_CONFIG_PREFIX.getKey() + vol1FileOutput
          + ".dfs.client.use.datanode.hostname", "true");
      config.setProperty(Property.INSTANCE_VOLUME_CONFIG_PREFIX.getKey() + vol2FileOutput
          + ".dfs.client.use.datanode.hostname", "false");
      config.setProperty(Property.INSTANCE_VOLUME_CONFIG_PREFIX.getKey() + vol1FileOutput
          + ".dfs.client.hedged.read.threadpool.size", "0");
      config.setProperty(Property.INSTANCE_VOLUME_CONFIG_PREFIX.getKey() + vol2FileOutput
          + ".dfs.client.hedged.read.threadpool.size", "1");
    });
  }

  @Test
  public void testHdfsConfigOverrides() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {

      Map<String,String> siteConfig = c.instanceOperations().getSiteConfiguration();
      assertEquals("10485760", siteConfig
          .get(Property.INSTANCE_VOLUME_CONFIG_PREFIX.getKey() + vol1 + ".dfs.blocksize"));
      assertEquals("51200000", siteConfig
          .get(Property.INSTANCE_VOLUME_CONFIG_PREFIX.getKey() + vol2 + ".dfs.blocksize"));
      assertEquals("true", siteConfig.get(Property.INSTANCE_VOLUME_CONFIG_PREFIX.getKey() + vol1
          + ".dfs.client.use.datanode.hostname"));
      assertEquals("false", siteConfig.get(Property.INSTANCE_VOLUME_CONFIG_PREFIX.getKey() + vol2
          + ".dfs.client.use.datanode.hostname"));
      assertEquals("0", siteConfig.get(Property.INSTANCE_VOLUME_CONFIG_PREFIX.getKey() + vol1
          + ".dfs.client.hedged.read.threadpool.size"));
      assertEquals("1", siteConfig.get(Property.INSTANCE_VOLUME_CONFIG_PREFIX.getKey() + vol2
          + ".dfs.client.hedged.read.threadpool.size"));

      String[] names = getUniqueNames(2);
      String t1 = names[0];
      String t2 = names[1];

      NewTableConfiguration ntc1 = new NewTableConfiguration();
      ntc1.setProperties(Map.of("table.custom.volume.preferred", vol1));
      c.tableOperations().create(t1, ntc1);
      ReadWriteIT.ingest(c, 10, 10, 100, 0, t1);
      c.tableOperations().flush(t1, null, null, true);

      NewTableConfiguration ntc2 = new NewTableConfiguration();
      ntc2.setProperties(Map.of("table.custom.volume.preferred", vol2));
      c.tableOperations().create(t2, ntc2);
      ReadWriteIT.ingest(c, 10, 10, 100, 0, t2);
      c.tableOperations().flush(t2, null, null, true);

      String tid1 = c.tableOperations().tableIdMap().get(t1);
      String tid2 = c.tableOperations().tableIdMap().get(t2);

      assertNotNull(tid1);
      assertNotNull(tid2);

      // Confirm that table 1 has a block size of 10485760
      FileSystem fs = this.cluster.getMiniDfs().getFileSystem();
      RemoteIterator<LocatedFileStatus> iter1 =
          fs.listFiles(new Path("/accumulo/tables/" + tid1), true);
      while (iter1.hasNext()) {
        LocatedFileStatus stat = iter1.next();
        if (stat.isFile()) {
          assertEquals(10485760, stat.getBlockSize());
        }
      }

      // Confirm that table 1 has a block size of 51200000
      RemoteIterator<LocatedFileStatus> iter2 =
          fs.listFiles(new Path("/accumulo/tables/" + tid2), true);
      while (iter2.hasNext()) {
        LocatedFileStatus stat = iter2.next();
        if (stat.isFile()) {
          assertEquals(51200000, stat.getBlockSize());
        }
      }

    }

  }

}
