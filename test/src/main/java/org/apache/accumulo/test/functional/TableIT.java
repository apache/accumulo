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

import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.FileNotFoundException;
import java.time.Duration;

import org.apache.accumulo.cluster.AccumuloCluster;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.accumulo.test.VerifyIngest.VerifyParams;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MINI_CLUSTER_ONLY)
public class TableIT extends AccumuloClusterHarness {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @Test
  public void test() throws Exception {
    assumeTrue(getClusterType() == ClusterType.MINI);

    AccumuloCluster cluster = getCluster();
    MiniAccumuloClusterImpl mac = (MiniAccumuloClusterImpl) cluster;
    String rootPath = mac.getConfig().getDir().getAbsolutePath();

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      TableOperations to = c.tableOperations();
      String tableName = getUniqueNames(1)[0];
      to.create(tableName);

      VerifyParams params = new VerifyParams(getClientProps(), tableName);
      TestIngest.ingest(c, params);
      to.flush(tableName, null, null, true);
      VerifyIngest.verifyIngest(c, params);
      TableId id = TableId.of(to.tableIdMap().get(tableName));
      try (Scanner s = c.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
        s.setRange(new KeyExtent(id, null, null).toMetaRange());
        s.fetchColumnFamily(DataFileColumnFamily.NAME);
        assertTrue(s.stream().findAny().isPresent());

        FileSystem fs = getCluster().getFileSystem();
        assertTrue(fs.listStatus(new Path(rootPath + "/accumulo/tables/" + id)).length > 0);
        to.delete(tableName);
        assertTrue(s.stream().findAny().isEmpty());

        try {
          assertEquals(0, fs.listStatus(new Path(rootPath + "/accumulo/tables/" + id)).length);
        } catch (FileNotFoundException ex) {
          // that's fine, too
        }
        assertNull(to.tableIdMap().get(tableName));
        to.create(tableName);
        TestIngest.ingest(c, params);
        VerifyIngest.verifyIngest(c, params);
        to.delete(tableName);
      }
    }
  }

}
