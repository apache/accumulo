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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.rfile.CreateEmpty;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * XXX As a part of verifying lossy recovery via inserting an empty rfile, this test deletes test
 * table tablets. This will require write access to the backing files of the test Accumulo mini
 * cluster.
 *
 * This test should read the file location from the test harness and that file should be on the
 * local filesystem. If you want to take a paranoid approach just make sure the test user doesn't
 * have write access to the HDFS files of any colocated live Accumulo instance or any important
 * local filesystem files..
 */
public class RecoveryWithEmptyRFileIT extends ConfigurableMacBase {
  private static final Logger log = LoggerFactory.getLogger(RecoveryWithEmptyRFileIT.class);

  private static final int ROWS = 200000;
  private static final int COLS = 1;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.useMiniDFS(true);
  }

  @Test
  public void replaceMissingRFile() throws Exception {
    log.info("Ingest some data, verify it was stored properly, replace an"
        + " underlying rfile with an empty one and verify we can scan.");
    Properties props = getClientProperties();
    try (AccumuloClient client = Accumulo.newClient().from(props).build()) {
      String tableName = getUniqueNames(1)[0];
      ReadWriteIT.ingest(client, ROWS, COLS, 50, 0, tableName);
      ReadWriteIT.verify(client, ROWS, COLS, 50, 0, tableName);

      client.tableOperations().flush(tableName, null, null, true);
      client.tableOperations().offline(tableName, true);

      log.debug("Replacing rfile(s) with empty");
      try (Scanner meta = client.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
        String tableId = client.tableOperations().tableIdMap().get(tableName);
        meta.setRange(new Range(new Text(tableId + ";"), new Text(tableId + "<")));
        meta.fetchColumnFamily(DataFileColumnFamily.NAME);
        boolean foundFile = false;
        for (Entry<Key,Value> entry : meta) {
          foundFile = true;
          Path rfile = new Path(entry.getKey().getColumnQualifier().toString());
          log.debug("Removing rfile '{}'", rfile);
          cluster.getFileSystem().delete(rfile, false);
          Process processInfo = cluster.exec(CreateEmpty.class, rfile.toString()).getProcess();
          assertEquals(0, processInfo.waitFor());
        }
        assertTrue(foundFile);
      }

      log.trace("invalidate cached file handles by issuing a compaction");
      client.tableOperations().online(tableName, true);
      client.tableOperations().compact(tableName, null, null, false, true);

      log.debug("make sure we can still scan");
      try (Scanner scan = client.createScanner(tableName, Authorizations.EMPTY)) {
        scan.setRange(new Range());
        long cells = 0L;
        for (Entry<Key,Value> entry : scan) {
          if (entry != null) {
            cells++;
          }
        }
        assertEquals(0L, cells);
      }
    }
  }
}
