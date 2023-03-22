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

import java.util.*;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.manager.TabletOperations;
import org.apache.accumulo.manager.split.Splitter;
import org.apache.accumulo.test.TestIngest;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

public class OfflineSplitIT extends AccumuloClusterHarness {

  @Test
  public void testSplit() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      Path importDir = new Path(getCluster().getTemporaryPath(), "bimp");
      getFileSystem().mkdirs(importDir);

      String tableName = getUniqueNames(1)[0];

      var newTableConf = new NewTableConfiguration().createOffline()
          .setProperties(Map.of(Property.TABLE_SPLIT_THRESHOLD.getKey(), "2K"));
      c.tableOperations().create(tableName, newTableConf);

      var tid = TableId.of(c.tableOperations().tableIdMap().get(tableName));
      var e1 = new KeyExtent(tid, null, null);

      var context = cluster.getServerContext();

      TestIngest.IngestParams params =
          new TestIngest.IngestParams(c.properties(), tableName, 10000);
      params.outputFile = new Path(importDir, "f1.rf").toString();
      // create an rfile with one entry, there was a bug with this:
      TestIngest.ingest(c, getFileSystem(), params);

      c.tableOperations().importDirectory(importDir.toString()).to(tableName).load();

      TabletOperations tabletOperations = extent -> {
        return () -> {};
      };
      Splitter splitter = new Splitter(context, Ample.DataLevel.USER, tabletOperations);
      splitter.start();

      while (true) {
        context.getAmple().readTablets().forLevel(Ample.DataLevel.USER).build().stream()
            .forEach(tm -> System.out.println(tm.getExtent() + " " + tm.getFilesMap()));
        Thread.sleep(10000);
      }
    }
  }
}
