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

import java.io.File;
import java.util.Collections;
import java.util.UUID;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.test.functional.ConfigurableMacIT;
import org.apache.accumulo.tserver.log.DfsLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

/**
 *
 */
public class MissingWalHeaderCompletesRecoveryIT extends ConfigurableMacIT {
  private static final Logger log = LoggerFactory.getLogger(MissingWalHeaderCompletesRecoveryIT.class);

  private static boolean rootHasWritePermission;

  @Override
  protected int defaultTimeoutSeconds() {
    return 2 * 60;
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration conf) {
    cfg.setNumTservers(1);
    cfg.setProperty(Property.MASTER_RECOVERY_DELAY, "1s");
    // Make sure the GC doesn't delete the file before the metadata reference is added
    cfg.setProperty(Property.GC_CYCLE_START, "999999s");
    conf.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  @Before
  public void setupMetadataPermission() throws Exception {
    Connector conn = getConnector();
    rootHasWritePermission = conn.securityOperations().hasTablePermission("root", MetadataTable.NAME, TablePermission.WRITE);
    if (!rootHasWritePermission) {
      conn.securityOperations().grantTablePermission("root", MetadataTable.NAME, TablePermission.WRITE);
      // Make sure it propagates through ZK
      Thread.sleep(5000);
    }
  }

  @After
  public void resetMetadataPermission() throws Exception {
    Connector conn = getConnector();
    // Final state doesn't match the original
    if (rootHasWritePermission != conn.securityOperations().hasTablePermission("root", MetadataTable.NAME, TablePermission.WRITE)) {
      if (rootHasWritePermission) {
        // root had write permission when starting, ensure root still does
        conn.securityOperations().grantTablePermission("root", MetadataTable.NAME, TablePermission.WRITE);
      } else {
        // root did not have write permission when starting, ensure that it does not
        conn.securityOperations().revokeTablePermission("root", MetadataTable.NAME, TablePermission.WRITE);
      }
    }
  }

  @Test
  public void testEmptyWalRecoveryCompletes() throws Exception {
    Connector conn = getConnector();
    MiniAccumuloClusterImpl cluster = getCluster();
    FileSystem fs = FileSystem.get(new Configuration());

    // Fake out something that looks like host:port, it's irrelevant
    String fakeServer = "127.0.0.1:12345";

    File walogs = new File(cluster.getConfig().getAccumuloDir(), ServerConstants.WAL_DIR);
    File walogServerDir = new File(walogs, fakeServer.replace(':', '+'));
    File emptyWalog = new File(walogServerDir, UUID.randomUUID().toString());

    log.info("Created empty WAL at " + emptyWalog.toURI());

    fs.create(new Path(emptyWalog.toURI())).close();

    Assert.assertTrue("root user did not have write permission to metadata table",
        conn.securityOperations().hasTablePermission("root", MetadataTable.NAME, TablePermission.WRITE));

    String tableName = getUniqueNames(1)[0];
    conn.tableOperations().create(tableName);

    String tableId = conn.tableOperations().tableIdMap().get(tableName);
    Assert.assertNotNull("Table ID was null", tableId);

    LogEntry logEntry = new LogEntry();
    logEntry.server = "127.0.0.1:12345";
    logEntry.filename = emptyWalog.toURI().toString();
    logEntry.tabletId = 10;
    logEntry.logSet = Collections.singleton(logEntry.filename);

    log.info("Taking {} offline", tableName);
    conn.tableOperations().offline(tableName, true);

    log.info("{} is offline", tableName);

    Text row = MetadataSchema.TabletsSection.getRow(new Text(tableId), null);
    Mutation m = new Mutation(row);
    m.put(logEntry.getColumnFamily(), logEntry.getColumnQualifier(), logEntry.getValue());

    BatchWriter bw = conn.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());
    bw.addMutation(m);
    bw.close();

    log.info("Bringing {} online", tableName);
    conn.tableOperations().online(tableName, true);

    log.info("{} is online", tableName);

    // Reading the table implies that recovery completed successfully (the empty file was ignored)
    // otherwise the tablet will never come online and we won't be able to read it.
    Scanner s = conn.createScanner(tableName, Authorizations.EMPTY);
    Assert.assertEquals(0, Iterables.size(s));
  }

  @Test
  public void testPartialHeaderWalRecoveryCompletes() throws Exception {
    Connector conn = getConnector();
    MiniAccumuloClusterImpl cluster = getCluster();
    FileSystem fs = FileSystem.get(new Configuration());

    // Fake out something that looks like host:port, it's irrelevant
    String fakeServer = "127.0.0.1:12345";

    File walogs = new File(cluster.getConfig().getAccumuloDir(), ServerConstants.WAL_DIR);
    File walogServerDir = new File(walogs, fakeServer.replace(':', '+'));
    File partialHeaderWalog = new File(walogServerDir, UUID.randomUUID().toString());

    log.info("Created WAL with malformed header at " + partialHeaderWalog.toURI());

    // Write half of the header
    FSDataOutputStream wal = fs.create(new Path(partialHeaderWalog.toURI()));
    wal.write(DfsLogger.LOG_FILE_HEADER_V3.getBytes(), 0, DfsLogger.LOG_FILE_HEADER_V3.length() / 2);
    wal.close();

    Assert.assertTrue("root user did not have write permission to metadata table",
        conn.securityOperations().hasTablePermission("root", MetadataTable.NAME, TablePermission.WRITE));

    String tableName = getUniqueNames(1)[0];
    conn.tableOperations().create(tableName);

    String tableId = conn.tableOperations().tableIdMap().get(tableName);
    Assert.assertNotNull("Table ID was null", tableId);

    LogEntry logEntry = new LogEntry();
    logEntry.server = "127.0.0.1:12345";
    logEntry.filename = partialHeaderWalog.toURI().toString();
    logEntry.tabletId = 10;
    logEntry.logSet = Collections.singleton(logEntry.filename);

    log.info("Taking {} offline", tableName);
    conn.tableOperations().offline(tableName, true);

    log.info("{} is offline", tableName);

    Text row = MetadataSchema.TabletsSection.getRow(new Text(tableId), null);
    Mutation m = new Mutation(row);
    m.put(logEntry.getColumnFamily(), logEntry.getColumnQualifier(), logEntry.getValue());

    BatchWriter bw = conn.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());
    bw.addMutation(m);
    bw.close();

    log.info("Bringing {} online", tableName);
    conn.tableOperations().online(tableName, true);

    log.info("{} is online", tableName);

    // Reading the table implies that recovery completed successfully (the empty file was ignored)
    // otherwise the tablet will never come online and we won't be able to read it.
    Scanner s = conn.createScanner(tableName, Authorizations.EMPTY);
    Assert.assertEquals(0, Iterables.size(s));
  }

}
