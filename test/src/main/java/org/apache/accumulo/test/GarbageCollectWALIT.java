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
package org.apache.accumulo.test;

import static org.junit.Assert.assertEquals;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;

import com.google.common.collect.Iterators;

public class GarbageCollectWALIT extends ConfigurableMacBase {

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.INSTANCE_ZK_HOST, "5s");
    cfg.setProperty(Property.GC_CYCLE_START, "1s");
    cfg.setProperty(Property.GC_CYCLE_DELAY, "1s");
    cfg.setNumTservers(1);
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  @Test(timeout = 2 * 60 * 1000)
  public void test() throws Exception {
    // not yet, please
    String tableName = getUniqueNames(1)[0];
    cluster.getClusterControl().stop(ServerType.GARBAGE_COLLECTOR);
    Connector c = getConnector();
    c.tableOperations().create(tableName);
    // count the number of WALs in the filesystem
    assertEquals(2, countWALsInFS(cluster));
    cluster.getClusterControl().stop(ServerType.TABLET_SERVER);
    cluster.getClusterControl().start(ServerType.GARBAGE_COLLECTOR);
    cluster.getClusterControl().start(ServerType.TABLET_SERVER);
    Iterators.size(c.createScanner(MetadataTable.NAME, Authorizations.EMPTY).iterator());
    // let GC run
    UtilWaitThread.sleep(3 * 5 * 1000);
    assertEquals(2, countWALsInFS(cluster));
  }

  private int countWALsInFS(MiniAccumuloClusterImpl cluster) throws Exception {
    FileSystem fs = cluster.getFileSystem();
    RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path(cluster.getConfig().getAccumuloDir() + "/wal"), true);
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
