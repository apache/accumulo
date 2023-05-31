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
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Test;

// verify that trash is used if our property is set to not ignore it (the default)
// and Hadoop is configured to enable it
public class GarbageCollectorTrashEnabledIT extends GarbageCollectorTrashBase {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(5);
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {

    Map<String,String> hadoopOverrides = new HashMap<>();
    hadoopOverrides.put(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, "5");
    cfg.setHadoopConfOverrides(hadoopOverrides);
    cfg.useMiniDFS(true);

    cfg.setProperty(Property.GC_CYCLE_START, "1");
    cfg.setProperty(Property.GC_CYCLE_DELAY, "1");
    @SuppressWarnings("removal")
    Property p = Property.GC_TRASH_IGNORE;
    cfg.setProperty(p, "false"); // default, use trash if configured
    cfg.setProperty(Property.GC_PORT, "0");
    cfg.setProperty(Property.TSERV_MAXMEM, "5K");
    cfg.setProperty(Property.TABLE_MAJC_RATIO, "5.0");
    cfg.setProperty(Property.TSERV_MAJC_DELAY, "180s");
  }

  @Test
  public void testTrashHadoopEnabledAccumuloEnabled() throws Exception {
    String table = this.getUniqueNames(1)[0];
    final FileSystem fs = super.getCluster().getFileSystem();
    super.makeTrashDir(fs);
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      ArrayList<StoredTabletFile> files = super.loadData(super.getServerContext(), c, table);
      assertFalse(files.isEmpty());
      c.tableOperations().compact(table, new CompactionConfig());
      TableId tid = TableId.of(c.tableOperations().tableIdMap().get(table));
      super.waitForFilesToBeGCd(files);
      assertEquals(files.size(), super.countFilesInTrash(fs, tid));
    }
  }

}
