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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.TrashPolicyDefault;
import org.junit.jupiter.api.Test;

// verify that trash is used if Hadoop is configured to use it and that using a custom policy works
public class GarbageCollectorTrashEnabledWithCustomPolicyIT extends GarbageCollectorTrashBase {

  public static class NoFlushFilesInTrashPolicy extends TrashPolicyDefault {

    @Override
    public boolean moveToTrash(Path path) throws IOException {
      // Don't put flush files in the Trash
      if (!path.getName().startsWith("F")) {
        return super.moveToTrash(path);
      }
      return false;
    }

  }

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(5);
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    // By default Hadoop trash is disabled - fs.trash.interval defaults to 0; override that here
    Map<String,String> hadoopOverrides = new HashMap<>();
    hadoopOverrides.put(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, "5");
    hadoopOverrides.put("fs.trash.classname", NoFlushFilesInTrashPolicy.class.getName());
    cfg.setHadoopConfOverrides(hadoopOverrides);
    cfg.useMiniDFS(true);

    cfg.setProperty(Property.GC_CYCLE_START, "1");
    cfg.setProperty(Property.GC_CYCLE_DELAY, "1");
    cfg.setProperty(Property.GC_PORT, "0");
    cfg.setProperty(Property.TSERV_MAXMEM, "5K");
    cfg.setProperty(Property.TABLE_MAJC_RATIO, "5.0");
  }

  @Test
  public void testTrashHadoopEnabledAccumuloEnabled() throws Exception {
    String table = this.getUniqueNames(1)[0];
    final FileSystem fs = super.getCluster().getFileSystem();
    super.makeTrashDir(fs);
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      ReadWriteIT.ingest(c, 10, 10, 10, 0, table);
      c.tableOperations().flush(table, null, null, true);
      ArrayList<StoredTabletFile> files1 = getFilesForTable(super.getServerContext(), c, table);
      assertFalse(files1.isEmpty());
      assertTrue(files1.stream().allMatch(stf -> stf.getPath().getName().startsWith("F")));
      c.tableOperations().compact(table, new CompactionConfig());
      super.waitForFilesToBeGCd(files1);
      ArrayList<StoredTabletFile> files2 = getFilesForTable(super.getServerContext(), c, table);
      assertFalse(files2.isEmpty());
      assertTrue(files2.stream().noneMatch(stf -> stf.getPath().getName().startsWith("F")));
      assertTrue(files2.stream().allMatch(stf -> stf.getPath().getName().startsWith("A")));
      c.tableOperations().compact(table, new CompactionConfig());
      super.waitForFilesToBeGCd(files2);
      ArrayList<StoredTabletFile> files3 = getFilesForTable(super.getServerContext(), c, table);
      assertTrue(files3.stream().allMatch(stf -> stf.getPath().getName().startsWith("A")));
      assertEquals(1, files3.size());
      TableId tid = TableId.of(c.tableOperations().tableIdMap().get(table));
      assertEquals(1, super.countFilesInTrash(fs, tid));
    }
  }

}
