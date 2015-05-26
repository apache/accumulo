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
package org.apache.accumulo.test.replication;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.IteratorSetting.Column;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.conf.ColumnSet;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.ReplicationSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.ReplicationSchema.WorkSection;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;
import org.apache.accumulo.fate.zookeeper.ZooLock;
import org.apache.accumulo.gc.SimpleGarbageCollector;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.minicluster.impl.ProcessReference;
import org.apache.accumulo.server.replication.ReplicaSystemFactory;
import org.apache.accumulo.server.replication.StatusCombiner;
import org.apache.accumulo.server.replication.StatusFormatter;
import org.apache.accumulo.server.replication.StatusUtil;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.accumulo.server.util.ReplicationTableUtil;
import org.apache.accumulo.test.functional.ConfigurableMacIT;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.protobuf.TextFormat;

/**
 * Replication tests which verify expected functionality using a single MAC instance. A MockReplicaSystem is used to "fake" the peer instance that we're
 * replicating to. This lets us test replication in a functional way without having to worry about two real systems.
 */
public class ReplicationIT extends ConfigurableMacIT {
  private static final Logger log = LoggerFactory.getLogger(ReplicationIT.class);

  @Override
  public int defaultTimeoutSeconds() {
    return 60 * 10;
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    // Run the master replication loop run frequently
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "10s");
    cfg.setProperty(Property.MASTER_REPLICATION_SCAN_INTERVAL, "1s");
    cfg.setProperty(Property.REPLICATION_WORK_ASSIGNMENT_SLEEP, "1s");
    cfg.setProperty(Property.TSERV_WALOG_MAX_SIZE, "1M");
    cfg.setProperty(Property.GC_CYCLE_START, "1s");
    cfg.setProperty(Property.GC_CYCLE_DELAY, "0");
    cfg.setProperty(Property.REPLICATION_NAME, "master");
    cfg.setProperty(Property.REPLICATION_WORK_PROCESSOR_DELAY, "1s");
    cfg.setProperty(Property.REPLICATION_WORK_PROCESSOR_PERIOD, "1s");
    cfg.setProperty(Property.TSERV_TOTAL_MUTATION_QUEUE_MAX, "1M");
    cfg.setNumTservers(1);
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  private Multimap<String,String> getLogs(Connector conn) throws TableNotFoundException {
    Multimap<String,String> logs = HashMultimap.create();
    Scanner scanner = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    scanner.fetchColumnFamily(LogColumnFamily.NAME);
    scanner.setRange(new Range());
    for (Entry<Key,Value> entry : scanner) {
      if (Thread.interrupted()) {
        Thread.currentThread().interrupt();
        return logs;
      }

      LogEntry logEntry = LogEntry.fromKeyValue(entry.getKey(), entry.getValue());

      for (String log : logEntry.logSet) {
        // Need to normalize the log file from LogEntry
        logs.put(new Path(log).toString(), logEntry.extent.getTableId().toString());
      }
    }
    return logs;
  }

  private Multimap<String,String> getAllLogs(Connector conn) throws TableNotFoundException {
    Multimap<String,String> logs = getLogs(conn);
    try {
      Scanner scanner = conn.createScanner(ReplicationTable.NAME, Authorizations.EMPTY);
      StatusSection.limit(scanner);
      Text buff = new Text();
      for (Entry<Key,Value> entry : scanner) {
        if (Thread.interrupted()) {
          Thread.currentThread().interrupt();
          return logs;
        }

        StatusSection.getFile(entry.getKey(), buff);
        String file = buff.toString();
        StatusSection.getTableId(entry.getKey(), buff);
        String tableId = buff.toString();

        logs.put(file, tableId);
      }
    } catch (TableOfflineException e) {
      log.debug("Replication table isn't online yet");
    }
    return logs;
  }

  private void waitForGCLock(Connector conn) throws InterruptedException {
    // Check if the GC process has the lock before wasting our retry attempts
    ZooKeeperInstance zki = (ZooKeeperInstance) conn.getInstance();
    ZooCacheFactory zcf = new ZooCacheFactory();
    ZooCache zcache = zcf.getZooCache(zki.getZooKeepers(), zki.getZooKeepersSessionTimeOut());
    String zkPath = ZooUtil.getRoot(conn.getInstance()) + Constants.ZGC_LOCK;
    log.info("Looking for GC lock at {}", zkPath);
    byte[] data = ZooLock.getLockData(zcache, zkPath, null);
    while (null == data) {
      log.info("Waiting for GC ZooKeeper lock to be acquired");
      Thread.sleep(1000);
      data = ZooLock.getLockData(zcache, zkPath, null);
    }
  }

  @Test
  public void replicationTableCreated() throws AccumuloException, AccumuloSecurityException {
    Assert.assertTrue(getConnector().tableOperations().exists(ReplicationTable.NAME));
    Assert.assertEquals(ReplicationTable.ID, getConnector().tableOperations().tableIdMap().get(ReplicationTable.NAME));
  }

  @Test
  public void verifyReplicationTableConfig() throws AccumuloException, TableNotFoundException, AccumuloSecurityException {
    TableOperations tops = getConnector().tableOperations();
    Map<String,EnumSet<IteratorScope>> iterators = tops.listIterators(ReplicationTable.NAME);

    // verify combiners are only iterators (no versioning)
    Assert.assertEquals(1, iterators.size());

    // look for combiner
    Assert.assertTrue(iterators.containsKey(ReplicationTable.COMBINER_NAME));
    Assert.assertTrue(iterators.get(ReplicationTable.COMBINER_NAME).containsAll(EnumSet.allOf(IteratorScope.class)));
    for (IteratorScope scope : EnumSet.allOf(IteratorScope.class)) {
      IteratorSetting is = tops.getIteratorSetting(ReplicationTable.NAME, ReplicationTable.COMBINER_NAME, scope);
      Assert.assertEquals(30, is.getPriority());
      Assert.assertEquals(StatusCombiner.class.getName(), is.getIteratorClass());
      Assert.assertEquals(1, is.getOptions().size());
      Assert.assertTrue(is.getOptions().containsKey("columns"));
      String cols = is.getOptions().get("columns");
      Column statusSectionCol = new Column(StatusSection.NAME);
      Column workSectionCol = new Column(WorkSection.NAME);
      Assert.assertEquals(
          ColumnSet.encodeColumns(statusSectionCol.getColumnFamily(), statusSectionCol.getColumnQualifier()) + ","
              + ColumnSet.encodeColumns(workSectionCol.getColumnFamily(), workSectionCol.getColumnQualifier()), cols);
    }

    boolean foundLocalityGroups = false;
    boolean foundLocalityGroupDef1 = false;
    boolean foundLocalityGroupDef2 = false;
    boolean foundFormatter = false;
    Joiner j = Joiner.on(",");
    Function<Text,String> textToString = new Function<Text,String>() {
      @Override
      public String apply(Text text) {
        return text.toString();
      }
    };
    for (Entry<String,String> p : tops.getProperties(ReplicationTable.NAME)) {
      String key = p.getKey();
      String val = p.getValue();
      // STATUS_LG_NAME, STATUS_LG_COLFAMS, WORK_LG_NAME, WORK_LG_COLFAMS
      if (key.equals(Property.TABLE_FORMATTER_CLASS.getKey()) && val.equals(StatusFormatter.class.getName())) {
        // look for formatter
        foundFormatter = true;
      } else if (key.equals(Property.TABLE_LOCALITY_GROUPS.getKey()) && val.equals(j.join(ReplicationTable.LOCALITY_GROUPS.keySet()))) {
        // look for locality groups enabled
        foundLocalityGroups = true;
      } else if (key.startsWith(Property.TABLE_LOCALITY_GROUP_PREFIX.getKey())) {
        // look for locality group column family definitions
        if (key.equals(Property.TABLE_LOCALITY_GROUP_PREFIX.getKey() + ReplicationTable.STATUS_LG_NAME)
            && val.equals(j.join(Iterables.transform(ReplicationTable.STATUS_LG_COLFAMS, textToString)))) {
          foundLocalityGroupDef1 = true;
        } else if (key.equals(Property.TABLE_LOCALITY_GROUP_PREFIX.getKey() + ReplicationTable.WORK_LG_NAME)
            && val.equals(j.join(Iterables.transform(ReplicationTable.WORK_LG_COLFAMS, textToString)))) {
          foundLocalityGroupDef2 = true;
        }
      }
    }
    Assert.assertTrue(foundLocalityGroups);
    Assert.assertTrue(foundLocalityGroupDef1);
    Assert.assertTrue(foundLocalityGroupDef2);
    Assert.assertTrue(foundFormatter);
  }

  @Test
  public void correctRecordsCompleteFile() throws Exception {
    Connector conn = getConnector();
    String table = "table1";
    conn.tableOperations().create(table);
    // If we have more than one tserver, this is subject to a race condition.
    conn.tableOperations().setProperty(table, Property.TABLE_REPLICATION.getKey(), "true");

    BatchWriter bw = conn.createBatchWriter(table, new BatchWriterConfig());
    for (int i = 0; i < 10; i++) {
      Mutation m = new Mutation(Integer.toString(i));
      m.put(new byte[0], new byte[0], new byte[0]);
      bw.addMutation(m);
    }

    bw.close();

    // After writing data, we'll get a replication table online
    boolean online = ReplicationTable.isOnline(conn);
    int attempts = 10;
    do {
      if (!online) {
        UtilWaitThread.sleep(2000);
        online = ReplicationTable.isOnline(conn);
        attempts--;
      }
    } while (!online && attempts > 0);
    Assert.assertTrue("Replication table was not online", online);

    for (int i = 0; i < 5; i++) {
      if (conn.securityOperations().hasTablePermission("root", ReplicationTable.NAME, TablePermission.READ)) {
        break;
      }
      log.info("Could not read replication table, waiting and will retry");
      Thread.sleep(2000);
    }

    Assert.assertTrue("'root' user could not read the replication table",
        conn.securityOperations().hasTablePermission("root", ReplicationTable.NAME, TablePermission.READ));

    Set<String> replRows = Sets.newHashSet();
    Scanner scanner;
    attempts = 5;
    while (replRows.isEmpty() && attempts > 0) {
      scanner = ReplicationTable.getScanner(conn);
      StatusSection.limit(scanner);
      for (Entry<Key,Value> entry : scanner) {
        Key k = entry.getKey();

        String fileUri = k.getRow().toString();
        try {
          new URI(fileUri);
        } catch (URISyntaxException e) {
          Assert.fail("Expected a valid URI: " + fileUri);
        }

        replRows.add(fileUri);
      }
    }

    Set<String> wals = Sets.newHashSet();
    Scanner s;
    attempts = 5;
    while (wals.isEmpty() && attempts > 0) {
      s = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
      s.fetchColumnFamily(MetadataSchema.TabletsSection.LogColumnFamily.NAME);
      for (Entry<Key,Value> entry : s) {
        LogEntry logEntry = LogEntry.fromKeyValue(entry.getKey(), entry.getValue());
        wals.add(new Path(logEntry.filename).toString());
      }
      attempts--;
    }

    // We only have one file that should need replication (no trace table)
    // We should find an entry in tablet and in the repl row
    Assert.assertEquals("Rows found: " + replRows, 1, replRows.size());

    // This should be the same set of WALs that we also are using
    Assert.assertEquals(replRows, wals);
  }

  @Test
  public void noRecordsWithoutReplication() throws Exception {
    Connector conn = getConnector();
    List<String> tables = new ArrayList<>();

    // replication shouldn't be online when we begin
    Assert.assertFalse(ReplicationTable.isOnline(conn));

    for (int i = 0; i < 5; i++) {
      String name = "table" + i;
      tables.add(name);
      conn.tableOperations().create(name);
    }

    // nor after we create some tables (that aren't being replicated)
    Assert.assertFalse(ReplicationTable.isOnline(conn));

    for (String table : tables) {
      BatchWriter bw = conn.createBatchWriter(table, new BatchWriterConfig());

      for (int j = 0; j < 5; j++) {
        Mutation m = new Mutation(Integer.toString(j));
        for (int k = 0; k < 5; k++) {
          String value = Integer.toString(k);
          m.put(value, "", value);
        }
        bw.addMutation(m);
      }

      bw.close();
    }

    // After writing data, still no replication table
    Assert.assertFalse(ReplicationTable.isOnline(conn));

    for (String table : tables) {
      conn.tableOperations().compact(table, null, null, true, true);
    }

    // After compacting data, still no replication table
    Assert.assertFalse(ReplicationTable.isOnline(conn));

    for (String table : tables) {
      conn.tableOperations().delete(table);
    }

    // After deleting tables, still no replication table
    Assert.assertFalse(ReplicationTable.isOnline(conn));
  }

  @Test
  public void twoEntriesForTwoTables() throws Exception {
    Connector conn = getConnector();
    String table1 = "table1", table2 = "table2";

    // replication shouldn't exist when we begin
    Assert.assertFalse("Replication table already online at the beginning of the test", ReplicationTable.isOnline(conn));

    // Create two tables
    conn.tableOperations().create(table1);
    conn.tableOperations().create(table2);

    // Enable replication on table1
    conn.tableOperations().setProperty(table1, Property.TABLE_REPLICATION.getKey(), "true");

    // Despite having replication on, we shouldn't have any need to write a record to it (and bring it online)
    Assert.assertFalse(ReplicationTable.isOnline(conn));

    // Write some data to table1
    BatchWriter bw = conn.createBatchWriter(table1, new BatchWriterConfig());

    for (int rows = 0; rows < 50; rows++) {
      Mutation m = new Mutation(Integer.toString(rows));
      for (int cols = 0; cols < 50; cols++) {
        String value = Integer.toString(cols);
        m.put(value, "", value);
      }
      bw.addMutation(m);
    }

    bw.close();

    // After the commit for these mutations finishes, we'll get a replication entry in accumulo.metadata for table1
    // Don't want to compact table1 as it ultimately cause the entry in accumulo.metadata to be removed before we can verify it's there

    // After writing data, we'll get a replication table online
    boolean online = ReplicationTable.isOnline(conn);
    int attempts = 10;
    do {
      if (!online) {
        UtilWaitThread.sleep(5000);
        online = ReplicationTable.isOnline(conn);
        attempts--;
      }
    } while (!online && attempts > 0);
    Assert.assertTrue("Replication table did not exist", online);

    Assert.assertTrue(ReplicationTable.isOnline(conn));
    conn.securityOperations().grantTablePermission("root", ReplicationTable.NAME, TablePermission.READ);

    // Verify that we found a single replication record that's for table1
    Scanner s = ReplicationTable.getScanner(conn);
    StatusSection.limit(s);
    Iterator<Entry<Key,Value>> iter = s.iterator();
    attempts = 5;
    while (attempts > 0) {
      if (!iter.hasNext()) {
        s.close();
        Thread.sleep(1000);
        s = ReplicationTable.getScanner(conn);
        iter = s.iterator();
        attempts--;
      } else {
        break;
      }
    }
    Assert.assertTrue(iter.hasNext());
    Entry<Key,Value> entry = iter.next();
    // We should at least find one status record for this table, we might find a second if another log was started from ingesting the data
    Assert.assertEquals("Expected to find replication entry for " + table1, conn.tableOperations().tableIdMap().get(table1), entry.getKey()
        .getColumnQualifier().toString());
    s.close();

    // Enable replication on table2
    conn.tableOperations().setProperty(table2, Property.TABLE_REPLICATION.getKey(), "true");

    // Write some data to table2
    bw = conn.createBatchWriter(table2, new BatchWriterConfig());

    for (int rows = 0; rows < 50; rows++) {
      Mutation m = new Mutation(Integer.toString(rows));
      for (int cols = 0; cols < 50; cols++) {
        String value = Integer.toString(cols);
        m.put(value, "", value);
      }
      bw.addMutation(m);
    }

    bw.close();

    // After the commit on these mutations, we'll get a replication entry in accumulo.metadata for table2
    // Don't want to compact table2 as it ultimately cause the entry in accumulo.metadata to be removed before we can verify it's there

    // After writing data, we'll get a replication table online
    Assert.assertTrue(ReplicationTable.isOnline(conn));
    conn.securityOperations().grantTablePermission("root", ReplicationTable.NAME, TablePermission.READ);

    Set<String> tableIds = Sets.newHashSet(conn.tableOperations().tableIdMap().get(table1), conn.tableOperations().tableIdMap().get(table2));
    Set<String> tableIdsForMetadata = Sets.newHashSet(tableIds);

    // Wait to make sure the table permission propagate
    Thread.sleep(5000);

    s = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    s.setRange(MetadataSchema.ReplicationSection.getRange());

    List<Entry<Key,Value>> records = new ArrayList<>();
    for (Entry<Key,Value> metadata : s) {
      records.add(metadata);
    }

    Assert.assertEquals("Expected to find 2 records, but actually found " + records, 2, records.size());

    for (Entry<Key,Value> metadata : records) {
      Assert.assertTrue("Expected record to be in metadata but wasn't " + metadata.getKey().toStringNoTruncate() + ", tableIds remaining "
          + tableIdsForMetadata, tableIdsForMetadata.remove(metadata.getKey().getColumnQualifier().toString()));
    }

    Assert.assertTrue("Expected that we had removed all metadata entries " + tableIdsForMetadata, tableIdsForMetadata.isEmpty());

    // Should be creating these records in replication table from metadata table every second
    Thread.sleep(5000);

    // Verify that we found two replication records: one for table1 and one for table2
    s = ReplicationTable.getScanner(conn);
    StatusSection.limit(s);
    iter = s.iterator();
    Assert.assertTrue("Found no records in replication table", iter.hasNext());
    entry = iter.next();
    Assert.assertTrue("Expected to find element in replication table", tableIds.remove(entry.getKey().getColumnQualifier().toString()));
    Assert.assertTrue("Expected to find two elements in replication table, only found one ", iter.hasNext());
    entry = iter.next();
    Assert.assertTrue("Expected to find element in replication table", tableIds.remove(entry.getKey().getColumnQualifier().toString()));
    Assert.assertFalse("Expected to only find two elements in replication table", iter.hasNext());
  }

  @Test
  public void replicationEntriesPrecludeWalDeletion() throws Exception {
    final Connector conn = getConnector();
    String table1 = "table1", table2 = "table2", table3 = "table3";
    final Multimap<String,String> logs = HashMultimap.create();
    final AtomicBoolean keepRunning = new AtomicBoolean(true);

    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        // Should really be able to interrupt here, but the Scanner throws a fit to the logger
        // when that happens
        while (keepRunning.get()) {
          try {
            logs.putAll(getAllLogs(conn));
          } catch (TableNotFoundException e) {
            log.error("Metadata table doesn't exist");
          }
        }
      }

    });

    t.start();

    conn.tableOperations().create(table1);
    conn.tableOperations().setProperty(table1, Property.TABLE_REPLICATION.getKey(), "true");
    conn.tableOperations().setProperty(table1, Property.TABLE_REPLICATION_TARGET.getKey() + "cluster1", "1");
    Thread.sleep(2000);

    // Write some data to table1
    BatchWriter bw = conn.createBatchWriter(table1, new BatchWriterConfig());
    for (int rows = 0; rows < 200; rows++) {
      Mutation m = new Mutation(Integer.toString(rows));
      for (int cols = 0; cols < 500; cols++) {
        String value = Integer.toString(cols);
        m.put(value, "", value);
      }
      bw.addMutation(m);
    }

    bw.close();

    conn.tableOperations().create(table2);
    conn.tableOperations().setProperty(table2, Property.TABLE_REPLICATION.getKey(), "true");
    conn.tableOperations().setProperty(table2, Property.TABLE_REPLICATION_TARGET.getKey() + "cluster1", "1");
    Thread.sleep(2000);

    // Write some data to table2
    bw = conn.createBatchWriter(table2, new BatchWriterConfig());
    for (int rows = 0; rows < 200; rows++) {
      Mutation m = new Mutation(Integer.toString(rows));
      for (int cols = 0; cols < 500; cols++) {
        String value = Integer.toString(cols);
        m.put(value, "", value);
      }
      bw.addMutation(m);
    }

    bw.close();

    conn.tableOperations().create(table3);
    conn.tableOperations().setProperty(table3, Property.TABLE_REPLICATION.getKey(), "true");
    conn.tableOperations().setProperty(table3, Property.TABLE_REPLICATION_TARGET.getKey() + "cluster1", "1");
    Thread.sleep(2000);

    // Write some data to table3
    bw = conn.createBatchWriter(table3, new BatchWriterConfig());
    for (int rows = 0; rows < 200; rows++) {
      Mutation m = new Mutation(Integer.toString(rows));
      for (int cols = 0; cols < 500; cols++) {
        String value = Integer.toString(cols);
        m.put(value, "", value);
      }
      bw.addMutation(m);
    }

    bw.close();

    // Force a write to metadata for the data written
    for (String table : Arrays.asList(table1, table2, table3)) {
      conn.tableOperations().flush(table, null, null, true);
    }

    keepRunning.set(false);
    t.join(5000);

    // The master is only running every second to create records in the replication table from the metadata table
    // Sleep a sufficient amount of time to ensure that we get the straggling WALs that might have been created at the end
    Thread.sleep(5000);

    Scanner s = ReplicationTable.getScanner(conn);
    StatusSection.limit(s);
    Set<String> replFiles = new HashSet<>();
    for (Entry<Key,Value> entry : s) {
      replFiles.add(entry.getKey().getRow().toString());
    }

    // We might have a WAL that was use solely for the replication table
    // We want to remove that from our list as it should not appear in the replication table
    String replicationTableId = conn.tableOperations().tableIdMap().get(ReplicationTable.NAME);
    Iterator<Entry<String,String>> observedLogs = logs.entries().iterator();
    while (observedLogs.hasNext()) {
      Entry<String,String> observedLog = observedLogs.next();
      if (replicationTableId.equals(observedLog.getValue())) {
        log.info("Removing {} because its tableId is for the replication table", observedLog);
        observedLogs.remove();
      }
    }

    // We should have *some* reference to each log that was seen in the metadata table
    // They might not yet all be closed though (might be newfile)
    Assert.assertEquals("Metadata log distribution: " + logs, logs.keySet(), replFiles);

    final Configuration conf = new Configuration();
    for (String replFile : replFiles) {
      Path p = new Path(replFile);
      FileSystem fs = p.getFileSystem(conf);
      Assert.assertTrue("File does not exist anymore, it was likely incorrectly garbage collected: " + p, fs.exists(p));
    }
  }

  @Test
  public void combinerWorksOnMetadata() throws Exception {
    Connector conn = getConnector();

    conn.securityOperations().grantTablePermission("root", MetadataTable.NAME, TablePermission.WRITE);

    ReplicationTableUtil.configureMetadataTable(conn, MetadataTable.NAME);

    Status stat1 = StatusUtil.fileCreated(100);
    Status stat2 = StatusUtil.fileClosed();

    BatchWriter bw = conn.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());
    Mutation m = new Mutation(ReplicationSection.getRowPrefix() + "file:/accumulo/wals/tserver+port/uuid");
    m.put(ReplicationSection.COLF, new Text("1"), ProtobufUtil.toValue(stat1));
    bw.addMutation(m);
    bw.close();

    Scanner s = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    s.setRange(ReplicationSection.getRange());

    Status actual = Status.parseFrom(Iterables.getOnlyElement(s).getValue().get());
    Assert.assertEquals(stat1, actual);

    bw = conn.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());
    m = new Mutation(ReplicationSection.getRowPrefix() + "file:/accumulo/wals/tserver+port/uuid");
    m.put(ReplicationSection.COLF, new Text("1"), ProtobufUtil.toValue(stat2));
    bw.addMutation(m);
    bw.close();

    s = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    s.setRange(ReplicationSection.getRange());

    actual = Status.parseFrom(Iterables.getOnlyElement(s).getValue().get());
    Status expected = Status.newBuilder().setBegin(0).setEnd(0).setClosed(true).setInfiniteEnd(true).setCreatedTime(100).build();

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void noDeadlock() throws Exception {
    final Connector conn = getConnector();

    ReplicationTable.setOnline(conn);
    conn.securityOperations().grantTablePermission("root", ReplicationTable.NAME, TablePermission.WRITE);
    conn.tableOperations().deleteRows(ReplicationTable.NAME, null, null);

    String table1 = "table1", table2 = "table2", table3 = "table3";
    conn.tableOperations().create(table1);
    conn.tableOperations().setProperty(table1, Property.TABLE_REPLICATION.getKey(), "true");
    conn.tableOperations().setProperty(table1, Property.TABLE_REPLICATION_TARGET.getKey() + "cluster1", "1");
    conn.tableOperations().create(table2);
    conn.tableOperations().setProperty(table2, Property.TABLE_REPLICATION.getKey(), "true");
    conn.tableOperations().setProperty(table2, Property.TABLE_REPLICATION_TARGET.getKey() + "cluster1", "1");
    conn.tableOperations().create(table3);
    conn.tableOperations().setProperty(table3, Property.TABLE_REPLICATION.getKey(), "true");
    conn.tableOperations().setProperty(table3, Property.TABLE_REPLICATION_TARGET.getKey() + "cluster1", "1");

    // Write some data to table1
    BatchWriter bw = conn.createBatchWriter(table1, new BatchWriterConfig());
    for (int rows = 0; rows < 200; rows++) {
      Mutation m = new Mutation(Integer.toString(rows));
      for (int cols = 0; cols < 500; cols++) {
        String value = Integer.toString(cols);
        m.put(value, "", value);
      }
      bw.addMutation(m);
    }

    bw.close();

    // Write some data to table2
    bw = conn.createBatchWriter(table2, new BatchWriterConfig());
    for (int rows = 0; rows < 200; rows++) {
      Mutation m = new Mutation(Integer.toString(rows));
      for (int cols = 0; cols < 500; cols++) {
        String value = Integer.toString(cols);
        m.put(value, "", value);
      }
      bw.addMutation(m);
    }

    bw.close();

    // Write some data to table3
    bw = conn.createBatchWriter(table3, new BatchWriterConfig());
    for (int rows = 0; rows < 200; rows++) {
      Mutation m = new Mutation(Integer.toString(rows));
      for (int cols = 0; cols < 500; cols++) {
        String value = Integer.toString(cols);
        m.put(value, "", value);
      }
      bw.addMutation(m);
    }

    bw.close();

    // Flush everything to try to make the replication records
    for (String table : Arrays.asList(table1, table2, table3)) {
      conn.tableOperations().flush(table, null, null, true);
    }

    for (String table : Arrays.asList(MetadataTable.NAME, table1, table2, table3)) {
      Iterators.size(conn.createScanner(table, Authorizations.EMPTY).iterator());
    }
  }

  @Test
  public void filesClosedAfterUnused() throws Exception {
    Connector conn = getConnector();

    String table = "table";
    conn.tableOperations().create(table);
    String tableId = conn.tableOperations().tableIdMap().get(table);

    Assert.assertNotNull(tableId);

    conn.tableOperations().setProperty(table, Property.TABLE_REPLICATION.getKey(), "true");
    conn.tableOperations().setProperty(table, Property.TABLE_REPLICATION_TARGET.getKey() + "cluster1", "1");
    // just sleep
    conn.instanceOperations().setProperty(Property.REPLICATION_PEERS.getKey() + "cluster1",
        ReplicaSystemFactory.getPeerConfigurationValue(MockReplicaSystem.class, "50000"));

    // Write a mutation to make a log file
    BatchWriter bw = conn.createBatchWriter(table, new BatchWriterConfig());
    Mutation m = new Mutation("one");
    m.put("", "", "");
    bw.addMutation(m);
    bw.close();

    // Write another to make sure the logger rolls itself?
    bw = conn.createBatchWriter(table, new BatchWriterConfig());
    m = new Mutation("three");
    m.put("", "", "");
    bw.addMutation(m);
    bw.close();

    Scanner s = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    s.fetchColumnFamily(TabletsSection.LogColumnFamily.NAME);
    s.setRange(TabletsSection.getRange(tableId));
    Set<String> wals = new HashSet<>();
    for (Entry<Key,Value> entry : s) {
      LogEntry logEntry = LogEntry.fromKeyValue(entry.getKey(), entry.getValue());
      for (String file : logEntry.logSet) {
        Path p = new Path(file);
        wals.add(p.toString());
      }
    }

    log.warn("Found wals {}", wals);

    bw = conn.createBatchWriter(table, new BatchWriterConfig());
    m = new Mutation("three");
    byte[] bytes = new byte[1024 * 1024];
    m.put("1".getBytes(), new byte[0], bytes);
    m.put("2".getBytes(), new byte[0], bytes);
    m.put("3".getBytes(), new byte[0], bytes);
    m.put("4".getBytes(), new byte[0], bytes);
    m.put("5".getBytes(), new byte[0], bytes);
    bw.addMutation(m);
    bw.close();

    conn.tableOperations().flush(table, null, null, true);

    while (!ReplicationTable.isOnline(conn)) {
      UtilWaitThread.sleep(2000);
    }

    for (int i = 0; i < 10; i++) {
      s = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
      s.fetchColumnFamily(LogColumnFamily.NAME);
      s.setRange(TabletsSection.getRange(tableId));
      for (Entry<Key,Value> entry : s) {
        log.info(entry.getKey().toStringNoTruncate() + "=" + entry.getValue());
      }

      try {
        s = ReplicationTable.getScanner(conn);
        StatusSection.limit(s);
        Text buff = new Text();
        boolean allReferencedLogsClosed = true;
        int recordsFound = 0;
        for (Entry<Key,Value> e : s) {
          recordsFound++;
          allReferencedLogsClosed = true;
          StatusSection.getFile(e.getKey(), buff);
          String file = buff.toString();
          if (wals.contains(file)) {
            Status stat = Status.parseFrom(e.getValue().get());
            if (!stat.getClosed()) {
              log.info("{} wasn't closed", file);
              allReferencedLogsClosed = false;
            }
          }
        }

        if (recordsFound > 0 && allReferencedLogsClosed) {
          return;
        }
        Thread.sleep(2000);
      } catch (RuntimeException e) {
        Throwable cause = e.getCause();
        if (cause instanceof AccumuloSecurityException) {
          AccumuloSecurityException ase = (AccumuloSecurityException) cause;
          switch (ase.getSecurityErrorCode()) {
            case PERMISSION_DENIED:
              // We tried to read the replication table before the GRANT went through
              Thread.sleep(2000);
              break;
            default:
              throw e;
          }
        }
      }
    }

    Assert.fail("We had a file that was referenced but didn't get closed");
  }

  @Test
  public void singleTableWithSingleTarget() throws Exception {
    // We want to kill the GC so it doesn't come along and close Status records and mess up the comparisons
    // against expected Status messages.
    for (ProcessReference proc : cluster.getProcesses().get(ServerType.GARBAGE_COLLECTOR)) {
      cluster.killProcess(ServerType.GARBAGE_COLLECTOR, proc);
    }

    Connector conn = getConnector();
    String table1 = "table1";

    // replication shouldn't be online when we begin
    Assert.assertFalse(ReplicationTable.isOnline(conn));

    // Create a table
    conn.tableOperations().create(table1);

    int attempts = 10;

    // Might think the table doesn't yet exist, retry
    while (attempts > 0) {
      try {
        // Enable replication on table1
        conn.tableOperations().setProperty(table1, Property.TABLE_REPLICATION.getKey(), "true");
        // Replicate table1 to cluster1 in the table with id of '4'
        conn.tableOperations().setProperty(table1, Property.TABLE_REPLICATION_TARGET.getKey() + "cluster1", "4");
        // Sleep for 100 seconds before saying something is replicated
        conn.instanceOperations().setProperty(Property.REPLICATION_PEERS.getKey() + "cluster1",
            ReplicaSystemFactory.getPeerConfigurationValue(MockReplicaSystem.class, "100000"));
        break;
      } catch (Exception e) {
        attempts--;
        if (attempts <= 0) {
          throw e;
        }
        UtilWaitThread.sleep(2000);
      }
    }

    // Write some data to table1
    BatchWriter bw = conn.createBatchWriter(table1, new BatchWriterConfig());
    for (int rows = 0; rows < 2000; rows++) {
      Mutation m = new Mutation(Integer.toString(rows));
      for (int cols = 0; cols < 50; cols++) {
        String value = Integer.toString(cols);
        m.put(value, "", value);
      }
      bw.addMutation(m);
    }

    bw.close();

    // Make sure the replication table is online at this point
    boolean online = ReplicationTable.isOnline(conn);
    attempts = 10;
    do {
      if (!online) {
        UtilWaitThread.sleep(2000);
        online = ReplicationTable.isOnline(conn);
        attempts--;
      }
    } while (!online && attempts > 0);
    Assert.assertTrue("Replication table was never created", online);

    // ACCUMULO-2743 The Observer in the tserver has to be made aware of the change to get the combiner (made by the master)
    for (int i = 0; i < 10 && !conn.tableOperations().listIterators(ReplicationTable.NAME).keySet().contains(ReplicationTable.COMBINER_NAME); i++) {
      UtilWaitThread.sleep(2000);
    }

    Assert.assertTrue("Combiner was never set on replication table",
        conn.tableOperations().listIterators(ReplicationTable.NAME).keySet().contains(ReplicationTable.COMBINER_NAME));

    // Trigger the minor compaction, waiting for it to finish.
    // This should write the entry to metadata that the file has data
    conn.tableOperations().flush(table1, null, null, true);

    // Make sure that we have one status element, should be a new file
    Scanner s = ReplicationTable.getScanner(conn);
    StatusSection.limit(s);
    Entry<Key,Value> entry = null;
    Status expectedStatus = StatusUtil.openWithUnknownLength();
    attempts = 10;
    // This record will move from new to new with infinite length because of the minc (flush)
    while (null == entry && attempts > 0) {
      try {
        entry = Iterables.getOnlyElement(s);
        Status actual = Status.parseFrom(entry.getValue().get());
        if (actual.getInfiniteEnd() != expectedStatus.getInfiniteEnd()) {
          entry = null;
          // the master process didn't yet fire and write the new mutation, wait for it to do
          // so and try to read it again
          Thread.sleep(1000);
        }
      } catch (NoSuchElementException e) {
        entry = null;
        Thread.sleep(500);
      } catch (IllegalArgumentException e) {
        // saw this contain 2 elements once
        s = ReplicationTable.getScanner(conn);
        StatusSection.limit(s);
        for (Entry<Key,Value> content : s) {
          log.info(content.getKey().toStringNoTruncate() + " => " + content.getValue());
        }
        throw e;
      } finally {
        attempts--;
      }
    }

    Assert.assertNotNull("Could not find expected entry in replication table", entry);
    Status actual = Status.parseFrom(entry.getValue().get());
    Assert.assertTrue("Expected to find a replication entry that is open with infinite length: " + ProtobufUtil.toString(actual),
        !actual.getClosed() && actual.getInfiniteEnd());

    // Try a couple of times to watch for the work record to be created
    boolean notFound = true;
    for (int i = 0; i < 10 && notFound; i++) {
      s = ReplicationTable.getScanner(conn);
      WorkSection.limit(s);
      int elementsFound = Iterables.size(s);
      if (0 < elementsFound) {
        Assert.assertEquals(1, elementsFound);
        notFound = false;
      }
      Thread.sleep(500);
    }

    // If we didn't find the work record, print the contents of the table
    if (notFound) {
      s = ReplicationTable.getScanner(conn);
      for (Entry<Key,Value> content : s) {
        log.info(content.getKey().toStringNoTruncate() + " => " + content.getValue());
      }
      Assert.assertFalse("Did not find the work entry for the status entry", notFound);
    }

    // Write some more data so that we over-run the single WAL
    bw = conn.createBatchWriter(table1, new BatchWriterConfig());
    for (int rows = 0; rows < 3000; rows++) {
      Mutation m = new Mutation(Integer.toString(rows));
      for (int cols = 0; cols < 50; cols++) {
        String value = Integer.toString(cols);
        m.put(value, "", value);
      }
      bw.addMutation(m);
    }

    bw.close();

    log.info("Issued compaction for table");
    conn.tableOperations().compact(table1, null, null, true, true);
    log.info("Compaction completed");

    // Master is creating entries in the replication table from the metadata table every second.
    // Compaction should trigger the record to be written to metadata. Wait a bit to ensure
    // that the master has time to work.
    Thread.sleep(5000);

    s = ReplicationTable.getScanner(conn);
    StatusSection.limit(s);
    int numRecords = 0;
    for (Entry<Key,Value> e : s) {
      numRecords++;
      log.info("Found status record {}\t{}", e.getKey().toStringNoTruncate(), ProtobufUtil.toString(Status.parseFrom(e.getValue().get())));
    }

    Assert.assertEquals(2, numRecords);

    // We should eventually get 2 work records recorded, need to account for a potential delay though
    // might see: status1 -> work1 -> status2 -> (our scans) -> work2
    notFound = true;
    for (int i = 0; i < 10 && notFound; i++) {
      s = ReplicationTable.getScanner(conn);
      WorkSection.limit(s);
      int elementsFound = Iterables.size(s);
      if (2 == elementsFound) {
        notFound = false;
      }
      Thread.sleep(500);
    }

    // If we didn't find the work record, print the contents of the table
    if (notFound) {
      s = ReplicationTable.getScanner(conn);
      for (Entry<Key,Value> content : s) {
        log.info(content.getKey().toStringNoTruncate() + " => " + content.getValue());
      }
      Assert.assertFalse("Did not find the work entries for the status entries", notFound);
    }
  }

  @Test
  public void correctClusterNameInWorkEntry() throws Exception {
    Connector conn = getConnector();
    String table1 = "table1";

    // replication shouldn't be online when we begin
    Assert.assertFalse(ReplicationTable.isOnline(conn));

    // Create two tables
    conn.tableOperations().create(table1);

    int attempts = 5;
    while (attempts > 0) {
      try {
        // Enable replication on table1
        conn.tableOperations().setProperty(table1, Property.TABLE_REPLICATION.getKey(), "true");
        // Replicate table1 to cluster1 in the table with id of '4'
        conn.tableOperations().setProperty(table1, Property.TABLE_REPLICATION_TARGET.getKey() + "cluster1", "4");
        attempts = 0;
      } catch (Exception e) {
        attempts--;
        if (attempts <= 0) {
          throw e;
        }
        UtilWaitThread.sleep(500);
      }
    }

    // Write some data to table1
    BatchWriter bw = conn.createBatchWriter(table1, new BatchWriterConfig());
    for (int rows = 0; rows < 2000; rows++) {
      Mutation m = new Mutation(Integer.toString(rows));
      for (int cols = 0; cols < 50; cols++) {
        String value = Integer.toString(cols);
        m.put(value, "", value);
      }
      bw.addMutation(m);
    }

    bw.close();
    conn.tableOperations().flush(table1, null, null, true);

    String tableId = conn.tableOperations().tableIdMap().get(table1);
    Assert.assertNotNull("Table ID was null", tableId);

    // Make sure the replication table exists at this point
    boolean online = ReplicationTable.isOnline(conn);
    attempts = 5;
    do {
      if (!online) {
        UtilWaitThread.sleep(500);
        online = ReplicationTable.isOnline(conn);
        attempts--;
      }
    } while (!online && attempts > 0);
    Assert.assertTrue("Replication table did not exist", online);

    for (int i = 0; i < 5 && !conn.securityOperations().hasTablePermission("root", ReplicationTable.NAME, TablePermission.READ); i++) {
      Thread.sleep(1000);
    }

    Assert.assertTrue(conn.securityOperations().hasTablePermission("root", ReplicationTable.NAME, TablePermission.READ));

    boolean notFound = true;
    Scanner s;
    for (int i = 0; i < 10 && notFound; i++) {
      s = ReplicationTable.getScanner(conn);
      WorkSection.limit(s);
      try {
        Entry<Key,Value> e = Iterables.getOnlyElement(s);
        Text expectedColqual = new ReplicationTarget("cluster1", "4", tableId).toText();
        Assert.assertEquals(expectedColqual, e.getKey().getColumnQualifier());
        notFound = false;
      } catch (NoSuchElementException e) {} catch (IllegalArgumentException e) {
        s = ReplicationTable.getScanner(conn);
        for (Entry<Key,Value> content : s) {
          log.info(content.getKey().toStringNoTruncate() + " => " + content.getValue());
        }
        Assert.fail("Found more than one work section entry");
      }

      Thread.sleep(500);
    }

    if (notFound) {
      s = ReplicationTable.getScanner(conn);
      for (Entry<Key,Value> content : s) {
        log.info(content.getKey().toStringNoTruncate() + " => " + content.getValue());
      }
      Assert.assertFalse("Did not find the work entry for the status entry", notFound);
    }
  }

  @Test
  public void replicationRecordsAreClosedAfterGarbageCollection() throws Exception {
    Collection<ProcessReference> gcProcs = cluster.getProcesses().get(ServerType.GARBAGE_COLLECTOR);
    for (ProcessReference ref : gcProcs) {
      cluster.killProcess(ServerType.GARBAGE_COLLECTOR, ref);
    }

    final Connector conn = getConnector();

    ReplicationTable.setOnline(conn);
    conn.securityOperations().grantTablePermission("root", ReplicationTable.NAME, TablePermission.WRITE);
    conn.tableOperations().deleteRows(ReplicationTable.NAME, null, null);

    final AtomicBoolean keepRunning = new AtomicBoolean(true);
    final Set<String> metadataWals = new HashSet<>();

    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        // Should really be able to interrupt here, but the Scanner throws a fit to the logger
        // when that happens
        while (keepRunning.get()) {
          try {
            metadataWals.addAll(getLogs(conn).keySet());
          } catch (Exception e) {
            log.error("Metadata table doesn't exist");
          }
        }
      }

    });

    t.start();

    String table1 = "table1", table2 = "table2", table3 = "table3";

    BatchWriter bw;
    try {
      conn.tableOperations().create(table1);
      conn.tableOperations().setProperty(table1, Property.TABLE_REPLICATION.getKey(), "true");
      conn.tableOperations().setProperty(table1, Property.TABLE_REPLICATION_TARGET.getKey() + "cluster1", "1");
      conn.instanceOperations().setProperty(Property.REPLICATION_PEERS.getKey() + "cluster1",
          ReplicaSystemFactory.getPeerConfigurationValue(MockReplicaSystem.class, null));

      // Write some data to table1
      bw = conn.createBatchWriter(table1, new BatchWriterConfig());
      for (int rows = 0; rows < 200; rows++) {
        Mutation m = new Mutation(Integer.toString(rows));
        for (int cols = 0; cols < 500; cols++) {
          String value = Integer.toString(cols);
          m.put(value, "", value);
        }
        bw.addMutation(m);
      }

      bw.close();

      conn.tableOperations().create(table2);
      conn.tableOperations().setProperty(table2, Property.TABLE_REPLICATION.getKey(), "true");
      conn.tableOperations().setProperty(table2, Property.TABLE_REPLICATION_TARGET.getKey() + "cluster1", "1");

      // Write some data to table2
      bw = conn.createBatchWriter(table2, new BatchWriterConfig());
      for (int rows = 0; rows < 200; rows++) {
        Mutation m = new Mutation(Integer.toString(rows));
        for (int cols = 0; cols < 500; cols++) {
          String value = Integer.toString(cols);
          m.put(value, "", value);
        }
        bw.addMutation(m);
      }

      bw.close();

      conn.tableOperations().create(table3);
      conn.tableOperations().setProperty(table3, Property.TABLE_REPLICATION.getKey(), "true");
      conn.tableOperations().setProperty(table3, Property.TABLE_REPLICATION_TARGET.getKey() + "cluster1", "1");

      // Write some data to table3
      bw = conn.createBatchWriter(table3, new BatchWriterConfig());
      for (int rows = 0; rows < 200; rows++) {
        Mutation m = new Mutation(Integer.toString(rows));
        for (int cols = 0; cols < 500; cols++) {
          String value = Integer.toString(cols);
          m.put(value, "", value);
        }
        bw.addMutation(m);
      }

      bw.close();

      // Flush everything to try to make the replication records
      for (String table : Arrays.asList(table1, table2, table3)) {
        conn.tableOperations().compact(table, null, null, true, true);
      }
    } finally {
      keepRunning.set(false);
      t.join(5000);
      Assert.assertFalse(t.isAlive());
    }

    // Kill the tserver(s) and restart them
    // to ensure that the WALs we previously observed all move to closed.
    for (ProcessReference proc : cluster.getProcesses().get(ServerType.TABLET_SERVER)) {
      cluster.killProcess(ServerType.TABLET_SERVER, proc);
    }

    cluster.exec(TabletServer.class);

    // Make sure we can read all the tables (recovery complete)
    for (String table : Arrays.asList(table1, table2, table3)) {
      Iterators.size(conn.createScanner(table, Authorizations.EMPTY).iterator());
    }

    // Starting the gc will run CloseWriteAheadLogReferences which will first close Statuses
    // in the metadata table, and then in the replication table
    Process gc = cluster.exec(SimpleGarbageCollector.class);

    waitForGCLock(conn);

    Thread.sleep(1000);

    log.info("GC is up and should have had time to run at least once by now");

    try {
      boolean allClosed = true;

      // We should either find all closed records or no records
      // After they're closed, they are candidates for deletion
      for (int i = 0; i < 10; i++) {
        Scanner s = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
        s.setRange(Range.prefix(ReplicationSection.getRowPrefix()));
        Iterator<Entry<Key,Value>> iter = s.iterator();

        long recordsFound = 0l;
        while (allClosed && iter.hasNext()) {
          Entry<Key,Value> entry = iter.next();
          String wal = entry.getKey().getRow().toString();
          if (metadataWals.contains(wal)) {
            Status status = Status.parseFrom(entry.getValue().get());
            log.info("{}={}", entry.getKey().toStringNoTruncate(), ProtobufUtil.toString(status));
            allClosed &= status.getClosed();
            recordsFound++;
          }
        }

        log.info("Found {} records from the metadata table", recordsFound);
        if (allClosed) {
          break;
        }

        UtilWaitThread.sleep(2000);
      }

      if (!allClosed) {
        Scanner s = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
        s.setRange(Range.prefix(ReplicationSection.getRowPrefix()));
        for (Entry<Key,Value> entry : s) {
          log.info(entry.getKey().toStringNoTruncate() + " " + ProtobufUtil.toString(Status.parseFrom(entry.getValue().get())));
        }
        Assert.fail("Expected all replication records in the metadata table to be closed");
      }

      for (int i = 0; i < 10; i++) {
        allClosed = true;

        Scanner s = ReplicationTable.getScanner(conn);
        Iterator<Entry<Key,Value>> iter = s.iterator();

        long recordsFound = 0l;
        while (allClosed && iter.hasNext()) {
          Entry<Key,Value> entry = iter.next();
          String wal = entry.getKey().getRow().toString();
          if (metadataWals.contains(wal)) {
            Status status = Status.parseFrom(entry.getValue().get());
            log.info("{}={}", entry.getKey().toStringNoTruncate(), ProtobufUtil.toString(status));
            allClosed &= status.getClosed();
            recordsFound++;
          }
        }

        log.info("Found {} records from the replication table", recordsFound);
        if (allClosed) {
          break;
        }

        UtilWaitThread.sleep(3000);
      }

      if (!allClosed) {
        Scanner s = ReplicationTable.getScanner(conn);
        StatusSection.limit(s);
        for (Entry<Key,Value> entry : s) {
          log.info(entry.getKey().toStringNoTruncate() + " " + TextFormat.shortDebugString(Status.parseFrom(entry.getValue().get())));
        }
        Assert.fail("Expected all replication records in the replication table to be closed");
      }

    } finally {
      gc.destroy();
      gc.waitFor();
    }

  }

  @Test
  public void replicatedStatusEntriesAreDeleted() throws Exception {
    // Just stop it now, we'll restart it after we restart the tserver
    for (ProcessReference proc : getCluster().getProcesses().get(ServerType.GARBAGE_COLLECTOR)) {
      getCluster().killProcess(ServerType.GARBAGE_COLLECTOR, proc);
    }

    final Connector conn = getConnector();
    log.info("Got connector to MAC");
    String table1 = "table1";

    // replication shouldn't be online when we begin
    Assert.assertFalse(ReplicationTable.isOnline(conn));

    // Create two tables
    conn.tableOperations().create(table1);

    int attempts = 5;
    while (attempts > 0) {
      try {
        // Enable replication on table1
        conn.tableOperations().setProperty(table1, Property.TABLE_REPLICATION.getKey(), "true");
        // Replicate table1 to cluster1 in the table with id of '4'
        conn.tableOperations().setProperty(table1, Property.TABLE_REPLICATION_TARGET.getKey() + "cluster1", "4");
        // Use the MockReplicaSystem impl and sleep for 5seconds
        conn.instanceOperations().setProperty(Property.REPLICATION_PEERS.getKey() + "cluster1",
            ReplicaSystemFactory.getPeerConfigurationValue(MockReplicaSystem.class, "1000"));
        attempts = 0;
      } catch (Exception e) {
        attempts--;
        if (attempts <= 0) {
          throw e;
        }
        UtilWaitThread.sleep(500);
      }
    }

    String tableId = conn.tableOperations().tableIdMap().get(table1);
    Assert.assertNotNull("Could not determine table id for " + table1, tableId);

    // Write some data to table1
    BatchWriter bw = conn.createBatchWriter(table1, new BatchWriterConfig());
    for (int rows = 0; rows < 2000; rows++) {
      Mutation m = new Mutation(Integer.toString(rows));
      for (int cols = 0; cols < 50; cols++) {
        String value = Integer.toString(cols);
        m.put(value, "", value);
      }
      bw.addMutation(m);
    }

    bw.close();
    conn.tableOperations().flush(table1, null, null, true);

    // Make sure the replication table exists at this point
    boolean online = ReplicationTable.isOnline(conn);
    attempts = 10;
    do {
      if (!online) {
        UtilWaitThread.sleep(1000);
        online = ReplicationTable.isOnline(conn);
        attempts--;
      }
    } while (!online && attempts > 0);
    Assert.assertTrue("Replication table did not exist", online);

    // Grant ourselves the write permission for later
    conn.securityOperations().grantTablePermission("root", ReplicationTable.NAME, TablePermission.WRITE);

    // Find the WorkSection record that will be created for that data we ingested
    boolean notFound = true;
    Scanner s;
    for (int i = 0; i < 10 && notFound; i++) {
      try {
        s = ReplicationTable.getScanner(conn);
        WorkSection.limit(s);
        Entry<Key,Value> e = Iterables.getOnlyElement(s);
        Text expectedColqual = new ReplicationTarget("cluster1", "4", tableId).toText();
        Assert.assertEquals(expectedColqual, e.getKey().getColumnQualifier());
        notFound = false;
      } catch (NoSuchElementException e) {

      } catch (IllegalArgumentException e) {
        // Somehow we got more than one element. Log what they were
        s = ReplicationTable.getScanner(conn);
        for (Entry<Key,Value> content : s) {
          log.info(content.getKey().toStringNoTruncate() + " => " + content.getValue());
        }
        Assert.fail("Found more than one work section entry");
      } catch (RuntimeException e) {
        // Catch a propagation issue, fail if it's not what we expect
        Throwable cause = e.getCause();
        if (cause instanceof AccumuloSecurityException) {
          AccumuloSecurityException sec = (AccumuloSecurityException) cause;
          switch (sec.getSecurityErrorCode()) {
            case PERMISSION_DENIED:
              // retry -- the grant didn't happen yet
              log.warn("Sleeping because permission was denied");
              break;
            default:
              throw e;
          }
        } else {
          throw e;
        }
      }

      Thread.sleep(2000);
    }

    if (notFound) {
      s = ReplicationTable.getScanner(conn);
      for (Entry<Key,Value> content : s) {
        log.info(content.getKey().toStringNoTruncate() + " => " + ProtobufUtil.toString(Status.parseFrom(content.getValue().get())));
      }
      Assert.assertFalse("Did not find the work entry for the status entry", notFound);
    }

    /**
     * By this point, we should have data ingested into a table, with at least one WAL as a candidate for replication. Compacting the table should close all
     * open WALs, which should ensure all records we're going to replicate have entries in the replication table, and nothing will exist in the metadata table
     * anymore
     */

    log.info("Killing tserver");
    // Kill the tserver(s) and restart them
    // to ensure that the WALs we previously observed all move to closed.
    for (ProcessReference proc : cluster.getProcesses().get(ServerType.TABLET_SERVER)) {
      cluster.killProcess(ServerType.TABLET_SERVER, proc);
    }

    log.info("Starting tserver");
    cluster.exec(TabletServer.class);

    log.info("Waiting to read tables");

    // Make sure we can read all the tables (recovery complete)
    for (String table : new String[] {MetadataTable.NAME, table1}) {
      Iterators.size(conn.createScanner(table, Authorizations.EMPTY).iterator());
    }

    log.info("Checking for replication entries in replication");
    // Then we need to get those records over to the replication table
    boolean foundResults = false;
    for (int i = 0; i < 5; i++) {
      s = ReplicationTable.getScanner(conn);
      int count = 0;
      for (Entry<Key,Value> entry : s) {
        count++;
        log.info("{}={}", entry.getKey().toStringNoTruncate(), entry.getValue());
      }
      if (count > 0) {
        foundResults = true;
        break;
      }
      Thread.sleep(1000);
    }

    Assert.assertTrue("Did not find any replication entries in the replication table", foundResults);

    getCluster().exec(SimpleGarbageCollector.class);

    // Wait for a bit since the GC has to run (should be running after a one second delay)
    waitForGCLock(conn);

    Thread.sleep(1000);

    // We expect no records in the metadata table after compaction. We have to poll
    // because we have to wait for the StatusMaker's next iteration which will clean
    // up the dangling *closed* records after we create the record in the replication table.
    // We need the GC to close the file (CloseWriteAheadLogReferences) before we can remove the record
    log.info("Checking metadata table for replication entries");
    foundResults = true;
    for (int i = 0; i < 10; i++) {
      s = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
      s.setRange(ReplicationSection.getRange());
      long size = 0;
      for (Entry<Key,Value> e : s) {
        size++;
        log.info("{}={}", e.getKey().toStringNoTruncate(), ProtobufUtil.toString(Status.parseFrom(e.getValue().get())));
      }
      if (size == 0) {
        foundResults = false;
        break;
      }
      Thread.sleep(2000);
      log.info("");
    }

    Assert.assertFalse("Replication status messages were not cleaned up from metadata table", foundResults);

    /**
     * After we close out and subsequently delete the metadata record, this will propagate to the replication table, which will cause those records to be
     * deleted after replication occurs
     */

    int recordsFound = 0;
    for (int i = 0; i < 30; i++) {
      s = ReplicationTable.getScanner(conn);
      recordsFound = 0;
      for (Entry<Key,Value> entry : s) {
        recordsFound++;
        log.info(entry.getKey().toStringNoTruncate() + " " + ProtobufUtil.toString(Status.parseFrom(entry.getValue().get())));
      }

      if (0 == recordsFound) {
        break;
      } else {
        Thread.sleep(1000);
        log.info("");
      }
    }

    Assert.assertEquals("Found unexpected replication records in the replication table", 0, recordsFound);
  }
}
