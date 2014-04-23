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
package org.apache.accumulo.test.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.rfile.CreateEmpty;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.server.util.Admin;
import org.apache.accumulo.test.functional.ReadWriteIT;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.TestMultiTableIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.junit.Test;

/**
  XXX As a part of verifying lossy recovery via inserting an empty rfile,
  this test deletes test table tablets. This will require write access to
  the backing files of the test Accumulo mini cluster.

  This test should read the file location from the test harness and that
  file should be on the local filesystem. If you want to take a paranoid
  approach just make sure the test user doesn't have write access to the
  HDFS files of any colocated live Accumulo instance or any important
  local filesystem files..
*/
public class RecoveryWithEmptyRFileIT extends ConfigurableMacIT {
  private static final Logger log = Logger.getLogger(RecoveryWithEmptyRFileIT.class);

  static final int ROWS = 200000;
  static final int COLS = 1;
  static final String COLF = "colf";

  @Override
  protected int defaultTimeoutSeconds() {
    return 2 * 60;
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.useMiniDFS(true);
  }

  @Test
  public void replaceMissingRFile() throws Exception {
    log.info("Ingest some data, verify it was stored properly, replace an underlying rfile with an empty one and verify we can scan.");
    Connector connector = getConnector();
    ReadWriteIT.ingest(connector, ROWS, COLS, 50, 0);
    ReadWriteIT.verify(connector, ROWS, COLS, 50, 0);

    connector.tableOperations().flush("test_ingest", null, null, true);
    connector.tableOperations().offline("test_ingest", true);

    log.debug("Replacing rfile(s) with empty");
    Scanner meta = connector.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    String tableId = connector.tableOperations().tableIdMap().get("test_ingest");
    meta.setRange(new Range(new Text(tableId + ";"), new Text(tableId + "<")));
    meta.fetchColumnFamily(DataFileColumnFamily.NAME);
    boolean foundFile = false;
    for (Entry<Key,Value> entry : meta) {
      foundFile = true;
      Path rfile = new Path(entry.getKey().getColumnQualifier().toString());
      log.debug("Removing rfile '" + rfile + "'");
      cluster.getFileSystem().delete(rfile, false);
      Process info = cluster.exec(CreateEmpty.class, rfile.toString());
      assertEquals(0, info.waitFor());
    }
    meta.close();
    assertTrue(foundFile);

    log.trace("invalidate cached file handles by issuing a compaction");
    connector.tableOperations().online("test_ingest", true);
    connector.tableOperations().compact("test_ingest", null, null, false, true);

    log.debug("make sure we can still scan");
    Scanner scan = connector.createScanner("test_ingest", Authorizations.EMPTY);
    scan.setRange(new Range());
    long cells = 0l;
    for (Entry<Key,Value> entry : scan) {
      cells++;
    }
    scan.close();
    assertEquals(0l, cells);
    FileSystem.closeAll();
  }

}
