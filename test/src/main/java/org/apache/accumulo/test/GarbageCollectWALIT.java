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

import java.time.Duration;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.jupiter.api.Test;

public class GarbageCollectWALIT extends ConfigurableMacBase {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(3);
  }

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.INSTANCE_ZK_HOST, "5s");
    cfg.setProperty(Property.GC_CYCLE_START, "1s");
    cfg.setProperty(Property.GC_CYCLE_DELAY, "1s");
    cfg.setNumTservers(1);
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  @Test
  public void test() throws Exception {
    // not yet, please
    String tableName = getUniqueNames(1)[0];
    cluster.getClusterControl().stop(ServerType.GARBAGE_COLLECTOR);
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      c.tableOperations().create(tableName);
      // count the number of WALs in the filesystem
      assertEquals(2, countWALsInFS(cluster));
      cluster.getClusterControl().stop(ServerType.TABLET_SERVER);
      cluster.getClusterControl().start(ServerType.GARBAGE_COLLECTOR);
      cluster.getClusterControl().start(ServerType.TABLET_SERVER);
      try (Scanner scanner = c.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
        scanner.forEach((k, v) -> {});
      }
      UtilWaitThread.sleep(3 * 5_000);
      assertEquals(2, countWALsInFS(cluster));
    }
  }

  private int countWALsInFS(MiniAccumuloClusterImpl cluster) throws Exception {
    FileSystem fs = cluster.getFileSystem();
    RemoteIterator<LocatedFileStatus> iterator =
        fs.listFiles(new Path(cluster.getConfig().getAccumuloDir() + "/wal"), true);
    int result = 0;
    while (iterator.hasNext()) {
      LocatedFileStatus next = iterator.next();
      if (!next.isDirectory()) {
        result++;
      }
    }
    return result;
  }
}
