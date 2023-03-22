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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataOperations;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.TabletOperationId;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

public class MetadataOperationsIT extends AccumuloClusterHarness {

  @Test
  public void testSplit() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      c.securityOperations().grantTablePermission("root", MetadataTable.NAME,
          TablePermission.WRITE);

      String tableName = getUniqueNames(1)[0];

      c.tableOperations().create(tableName, new NewTableConfiguration().createOffline());

      var tid = TableId.of(c.tableOperations().tableIdMap().get(tableName));
      var e1 = new KeyExtent(tid, null, null);

      var context = cluster.getServerContext();

      SortedSet<Text> splits = new TreeSet<>(List.of(new Text("c"), new Text("f"), new Text("j")));

      AtomicInteger nextDir = new AtomicInteger(1);

      Supplier<String> dirNameGenerator = () -> {
        return nextDir.getAndIncrement() + "";
      };

      MetadataOperations.doSplit(context.getAmple(), e1, splits,
          new TabletOperationId(UUID.randomUUID().toString()), dirNameGenerator);

      Assert.assertEquals(splits, new TreeSet<>(c.tableOperations().listSplits(tableName)));
    }
  }
}
