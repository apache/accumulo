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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.IteratorSetting.Column;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
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
import org.apache.accumulo.core.replication.ReplicationTableOfflineException;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;
import org.apache.accumulo.fate.zookeeper.ZooLock;
import org.apache.accumulo.gc.SimpleGarbageCollector;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.log.WalStateManager;
import org.apache.accumulo.server.log.WalStateManager.WalState;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.replication.ReplicaSystemFactory;
import org.apache.accumulo.server.replication.StatusCombiner;
import org.apache.accumulo.server.replication.StatusFormatter;
import org.apache.accumulo.server.replication.StatusUtil;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.accumulo.server.util.ReplicationTableUtil;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class ReplicationIT extends ConfigurableMacBase {
  private static final Logger log = LoggerFactory.getLogger(ReplicationIT.class);
  private static final long MILLIS_BETWEEN_REPLICATION_TABLE_ONLINE_CHECKS = 5000l;

  @Override
  public int defaultTimeoutSeconds() {
    return 60 * 10;
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    // Run the master replication loop run frequently
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
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

  private Multimap<String,Table.ID> getLogs(Connector conn) throws Exception {
    // Map of server to tableId
    Multimap<TServerInstance,String> serverToTableID = HashMultimap.create();
    Scanner scanner = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    scanner.setRange(MetadataSchema.TabletsSection.getRange());
    scanner.fetchColumnFamily(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME);
    for (Entry<Key,Value> entry : scanner) {
      TServerInstance key = new TServerInstance(entry.getValue(), entry.getKey().getColumnQualifier());
      byte[] tableId = KeyExtent.tableOfMetadataRow(entry.getKey().getRow());
      serverToTableID.put(key, new String(tableId, UTF_8));
    }
    // Map of logs to tableId
    Multimap<String,Table.ID> logs = HashMultimap.create();
    Instance i = conn.getInstance();
    ZooReaderWriter zk = new ZooReaderWriter(i.getZooKeepers(), i.getZooKeepersSessionTimeOut(), "");
    WalStateManager wals = new WalStateManager(conn.getInstance(), zk);
    for (Entry<TServerInstance,List<UUID>> entry : wals.getAllMarkers().entrySet()) {
      for (UUID id : entry.getValue()) {
        Pair<WalState,Path> state = wals.state(entry.getKey(), id);
        for (String tableId : serverToTableID.get(entry.getKey())) {
          logs.put(state.getSecond().toString(), Table.ID.of(tableId));
        }
      }
    }
    return logs;
  }

  private Multimap<String,Table.ID> getAllLogs(Connector conn) throws Exception {
    Multimap<String,Table.ID> logs = getLogs(conn);
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
        Table.ID tableId = StatusSection.getTableId(entry.getKey());

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
    Assert.assertEquals(ReplicationTable.ID.canonicalID(), getConnector().tableOperations().tableIdMap().get(ReplicationTable.NAME));
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
            && val.equals(j.join(Iterables.transform(ReplicationTable.STATUS_LG_COLFAMS, text -> text.toString())))) {
          foundLocalityGroupDef1 = true;
        } else if (key.equals(Property.TABLE_LOCALITY_GROUP_PREFIX.getKey() + ReplicationTable.WORK_LG_NAME)
            && val.equals(j.join(Iterables.transform(ReplicationTable.WORK_LG_COLFAMS, text -> text.toString())))) {
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
    while (!ReplicationTable.isOnline(conn)) {
      sleepUninterruptibly(MILLIS_BETWEEN_REPLICATION_TABLE_ONLINE_CHECKS, TimeUnit.MILLISECONDS);
    }
    Assert.assertTrue("Replication table did not exist", ReplicationTable.isOnline(conn));

    for (int i = 0; i < 5; i++) {
      if (conn.securityOperations().hasTablePermission("root", ReplicationTable.NAME, TablePermission.READ)) {
        break;
      }
      log.info("Could not read replication table, waiting and will retry");
      Thread.sleep(2000);
    }

    Assert.assertTrue("'root' user could not read the replication table",
        conn.securityOperations().hasTablePermission("root", ReplicationTable.NAME, TablePermission.READ));

    Set<String> replRows = new HashSet<>();
    Scanner scanner;
    int attempts = 5;
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

    Set<String> wals = new HashSet<>();
    attempts = 5;
    Instance i = conn.getInstance();
    ZooReaderWriter zk = new ZooReaderWriter(i.getZooKeepers(), i.getZooKeepersSessionTimeOut(), "");
    while (wals.isEmpty() && attempts > 0) {
      WalStateManager markers = new WalStateManager(i, zk);
      for (Entry<Path,WalState> entry : markers.getAllState().entrySet()) {
        wals.add(entry.getKey().toString());
      }
      attempts--;
    }

    // We only have one file that should need replication (no trace table)
    // We should find an entry in tablet and in the repl row
    Assert.assertEquals("Rows found: " + replRows, 1, replRows.size());

    // There should only be one extra WALog that replication doesn't know about
    replRows.removeAll(wals);
    Assert.assertEquals(2, wals.size());
    Assert.assertEquals(0, replRows.size());
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
      writeSomeData(conn, table, 5, 5);
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
    conn.securityOperations().grantTablePermission("root", ReplicationTable.NAME, TablePermission.READ);
    // wait for permission to propagate
    Thread.sleep(5000);

    // Enable replication on table1
    conn.tableOperations().setProperty(table1, Property.TABLE_REPLICATION.getKey(), "true");

    // Despite having replication on, we shouldn't have any need to write a record to it (and bring it online)
    Assert.assertFalse(ReplicationTable.isOnline(conn));

    // Write some data to table1
    writeSomeData(conn, table1, 50, 50);

    // After the commit for these mutations finishes, we'll get a replication entry in accumulo.metadata for table1
    // Don't want to compact table1 as it ultimately cause the entry in accumulo.metadata to be removed before we can verify it's there

    // After writing data, we'll get a replication table online
    while (!ReplicationTable.isOnline(conn)) {
      sleepUninterruptibly(MILLIS_BETWEEN_REPLICATION_TABLE_ONLINE_CHECKS, TimeUnit.MILLISECONDS);
    }
    Assert.assertTrue(ReplicationTable.isOnline(conn));

    // Verify that we found a single replication record that's for table1
    Scanner s = ReplicationTable.getScanner(conn);
    StatusSection.limit(s);
    for (int i = 0; i < 5; i++) {
      if (Iterators.size(s.iterator()) == 1) {
        break;
      }
      Thread.sleep(1000);
    }
    Entry<Key,Value> entry = Iterators.getOnlyElement(s.iterator());
    // We should at least find one status record for this table, we might find a second if another log was started from ingesting the data
    Assert.assertEquals("Expected to find replication entry for " + table1, conn.tableOperations().tableIdMap().get(table1), entry.getKey()
        .getColumnQualifier().toString());
    s.close();

    // Enable replication on table2
    conn.tableOperations().setProperty(table2, Property.TABLE_REPLICATION.getKey(), "true");

    // Write some data to table2
    writeSomeData(conn, table2, 50, 50);

    // After the commit on these mutations, we'll get a replication entry in accumulo.metadata for table2
    // Don't want to compact table2 as it ultimately cause the entry in accumulo.metadata to be removed before we can verify it's there

    Set<String> tableIds = Sets.newHashSet(conn.tableOperations().tableIdMap().get(table1), conn.tableOperations().tableIdMap().get(table2));
    Set<String> tableIdsForMetadata = Sets.newHashSet(tableIds);

    List<Entry<Key,Value>> records = new ArrayList<>();
    s = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    s.setRange(MetadataSchema.ReplicationSection.getRange());
    for (Entry<Key,Value> metadata : s) {
      records.add(metadata);
      log.debug("Meta: {} => {}", metadata.getKey().toStringNoTruncate(), metadata.getValue().toString());
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
    Iterator<Entry<Key,Value>> iter = s.iterator();
    Assert.assertTrue("Found no records in replication table", iter.hasNext());
    entry = iter.next();
    Assert.assertTrue("Expected to find element in replication table", tableIds.remove(entry.getKey().getColumnQualifier().toString()));
    Assert.assertTrue("Expected to find two elements in replication table, only found one ", iter.hasNext());
    entry = iter.next();
    Assert.assertTrue("Expected to find element in replication table", tableIds.remove(entry.getKey().getColumnQualifier().toString()));
    Assert.assertFalse("Expected to only find two elements in replication table", iter.hasNext());
  }

  private void writeSomeData(Connector conn, String table, int rows, int cols) throws Exception {
    BatchWriter bw = conn.createBatchWriter(table, new BatchWriterConfig());
    for (int row = 0; row < rows; row++) {
      Mutation m = new Mutation(Integer.toString(row));
      for (int col = 0; col < cols; col++) {
        String value = Integer.toString(col);
        m.put(value, "", value);
      }
      bw.addMutation(m);
    }
    bw.close();
  }

  @Test
  public void replicationEntriesPrecludeWalDeletion() throws Exception {
    final Connector conn = getConnector();
    String table1 = "table1", table2 = "table2", table3 = "table3";
    final Multimap<String,Table.ID> logs = HashMultimap.create();
    final AtomicBoolean keepRunning = new AtomicBoolean(true);

    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        // Should really be able to interrupt here, but the Scanner throws a fit to the logger
        // when that happens
        while (keepRunning.get()) {
          try {
            logs.putAll(getAllLogs(conn));
          } catch (Exception e) {
            log.error("Error getting logs", e);
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
    writeSomeData(conn, table1, 200, 500);

    conn.tableOperations().create(table2);
    conn.tableOperations().setProperty(table2, Property.TABLE_REPLICATION.getKey(), "true");
    conn.tableOperations().setProperty(table2, Property.TABLE_REPLICATION_TARGET.getKey() + "cluster1", "1");
    Thread.sleep(2000);

    writeSomeData(conn, table2, 200, 500);

    conn.tableOperations().create(table3);
    conn.tableOperations().setProperty(table3, Property.TABLE_REPLICATION.getKey(), "true");
    conn.tableOperations().setProperty(table3, Property.TABLE_REPLICATION_TARGET.getKey() + "cluster1", "1");
    Thread.sleep(2000);

    writeSomeData(conn, table3, 200, 500);

    // Force a write to metadata for the data written
    for (String table : Arrays.asList(table1, table2, table3)) {
      conn.tableOperations().flush(table, null, null, true);
    }

    keepRunning.set(false);
    t.join(5000);

    // The master is only running every second to create records in the replication table from the metadata table
    // Sleep a sufficient amount of time to ensure that we get the straggling WALs that might have been created at the end
    Thread.sleep(5000);

    Set<String> replFiles = getReferencesToFilesToBeReplicated(conn);

    // We might have a WAL that was use solely for the replication table
    // We want to remove that from our list as it should not appear in the replication table
    String replicationTableId = conn.tableOperations().tableIdMap().get(ReplicationTable.NAME);
    Iterator<Entry<String,Table.ID>> observedLogs = logs.entries().iterator();
    while (observedLogs.hasNext()) {
      Entry<String,Table.ID> observedLog = observedLogs.next();
      if (replicationTableId.equals(observedLog.getValue().canonicalID())) {
        log.info("Removing {} because its tableId is for the replication table", observedLog);
        observedLogs.remove();
      }
    }

    // We should have *some* reference to each log that was seen in the metadata table
    // They might not yet all be closed though (might be newfile)
    Assert.assertTrue("Metadata log distribution: " + logs + "replFiles " + replFiles, logs.keySet().containsAll(replFiles));
    Assert.assertTrue("Difference between replication entries and current logs is bigger than one", logs.keySet().size() - replFiles.size() <= 1);

    final Configuration conf = new Configuration();
    for (String replFile : replFiles) {
      Path p = new Path(replFile);
      FileSystem fs = p.getFileSystem(conf);
      if (!fs.exists(p)) {
        // double-check: the garbage collector can be fast
        Set<String> currentSet = getReferencesToFilesToBeReplicated(conn);
        log.info("Current references {}", currentSet);
        log.info("Looking for reference to {}", replFile);
        log.info("Contains? {}", currentSet.contains(replFile));
        Assert.assertTrue("File does not exist anymore, it was likely incorrectly garbage collected: " + p, !currentSet.contains(replFile));
      }
    }
  }

  private Set<String> getReferencesToFilesToBeReplicated(final Connector conn) throws ReplicationTableOfflineException {
    Scanner s = ReplicationTable.getScanner(conn);
    StatusSection.limit(s);
    Set<String> replFiles = new HashSet<>();
    for (Entry<Key,Value> entry : s) {
      replFiles.add(entry.getKey().getRow().toString());
    }
    return replFiles;
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

    writeSomeData(conn, table1, 200, 500);

    writeSomeData(conn, table2, 200, 500);

    writeSomeData(conn, table3, 200, 500);

    // Flush everything to try to make the replication records
    for (String table : Arrays.asList(table1, table2, table3)) {
      conn.tableOperations().flush(table, null, null, true);
    }

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
    Table.ID tableId = Table.ID.of(conn.tableOperations().tableIdMap().get(table));

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
      wals.add(new Path(logEntry.filename).toString());
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
      sleepUninterruptibly(MILLIS_BETWEEN_REPLICATION_TABLE_ONLINE_CHECKS, TimeUnit.MILLISECONDS);
    }

    for (int i = 0; i < 10; i++) {
      s = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
      s.fetchColumnFamily(LogColumnFamily.NAME);
      s.setRange(TabletsSection.getRange(tableId));
      for (Entry<Key,Value> entry : s) {
        log.info("{}={}", entry.getKey().toStringNoTruncate(), entry.getValue());
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
    getCluster().getClusterControl().stop(ServerType.GARBAGE_COLLECTOR);

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
        sleepUninterruptibly(2, TimeUnit.SECONDS);
      }
    }

    // Write some data to table1
    writeSomeData(conn, table1, 2000, 50);

    // Make sure the replication table is online at this point
    while (!ReplicationTable.isOnline(conn)) {
      sleepUninterruptibly(MILLIS_BETWEEN_REPLICATION_TABLE_ONLINE_CHECKS, TimeUnit.MILLISECONDS);
    }
    Assert.assertTrue("Replication table was never created", ReplicationTable.isOnline(conn));

    // ACCUMULO-2743 The Observer in the tserver has to be made aware of the change to get the combiner (made by the master)
    for (int i = 0; i < 10 && !conn.tableOperations().listIterators(ReplicationTable.NAME).keySet().contains(ReplicationTable.COMBINER_NAME); i++) {
      sleepUninterruptibly(2, TimeUnit.SECONDS);
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
          log.info("{} => {}", content.getKey().toStringNoTruncate(), content.getValue());
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
        log.info("{} => {}", content.getKey().toStringNoTruncate(), content.getValue());
      }
      Assert.assertFalse("Did not find the work entry for the status entry", notFound);
    }

    // Write some more data so that we over-run the single WAL
    writeSomeData(conn, table1, 3000, 50);

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
        log.info("{} => {}", content.getKey().toStringNoTruncate(), content.getValue());
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
        sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
      }
    }

    // Write some data to table1
    writeSomeData(conn, table1, 2000, 50);
    conn.tableOperations().flush(table1, null, null, true);

    Table.ID tableId = Table.ID.of(conn.tableOperations().tableIdMap().get(table1));
    Assert.assertNotNull("Table ID was null", tableId);

    // Make sure the replication table exists at this point
    while (!ReplicationTable.isOnline(conn)) {
      sleepUninterruptibly(MILLIS_BETWEEN_REPLICATION_TABLE_ONLINE_CHECKS, TimeUnit.MILLISECONDS);
    }
    Assert.assertTrue("Replication table did not exist", ReplicationTable.isOnline(conn));

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
          log.info("{} => {}", content.getKey().toStringNoTruncate(), content.getValue());
        }
        Assert.fail("Found more than one work section entry");
      }

      Thread.sleep(500);
    }

    if (notFound) {
      s = ReplicationTable.getScanner(conn);
      for (Entry<Key,Value> content : s) {
        log.info("{} => {}", content.getKey().toStringNoTruncate(), content.getValue());
      }
      Assert.assertFalse("Did not find the work entry for the status entry", notFound);
    }
  }

  @Test
  public void replicationRecordsAreClosedAfterGarbageCollection() throws Exception {
    getCluster().getClusterControl().stop(ServerType.GARBAGE_COLLECTOR);

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

    try {
      conn.tableOperations().create(table1);
      conn.tableOperations().setProperty(table1, Property.TABLE_REPLICATION.getKey(), "true");
      conn.tableOperations().setProperty(table1, Property.TABLE_REPLICATION_TARGET.getKey() + "cluster1", "1");
      conn.instanceOperations().setProperty(Property.REPLICATION_PEERS.getKey() + "cluster1",
          ReplicaSystemFactory.getPeerConfigurationValue(MockReplicaSystem.class, null));

      // Write some data to table1
      writeSomeData(conn, table1, 200, 500);

      conn.tableOperations().create(table2);
      conn.tableOperations().setProperty(table2, Property.TABLE_REPLICATION.getKey(), "true");
      conn.tableOperations().setProperty(table2, Property.TABLE_REPLICATION_TARGET.getKey() + "cluster1", "1");

      writeSomeData(conn, table2, 200, 500);

      conn.tableOperations().create(table3);
      conn.tableOperations().setProperty(table3, Property.TABLE_REPLICATION.getKey(), "true");
      conn.tableOperations().setProperty(table3, Property.TABLE_REPLICATION_TARGET.getKey() + "cluster1", "1");

      writeSomeData(conn, table3, 200, 500);

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
    cluster.getClusterControl().stop(ServerType.TABLET_SERVER);
    cluster.getClusterControl().start(ServerType.TABLET_SERVER);

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

        sleepUninterruptibly(2, TimeUnit.SECONDS);
      }

      if (!allClosed) {
        Scanner s = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
        s.setRange(Range.prefix(ReplicationSection.getRowPrefix()));
        for (Entry<Key,Value> entry : s) {
          log.info("{} {}", entry.getKey().toStringNoTruncate(), ProtobufUtil.toString(Status.parseFrom(entry.getValue().get())));
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

        sleepUninterruptibly(3, TimeUnit.SECONDS);
      }

      if (!allClosed) {
        Scanner s = ReplicationTable.getScanner(conn);
        StatusSection.limit(s);
        for (Entry<Key,Value> entry : s) {
          log.info("{} {}", entry.getKey().toStringNoTruncate(), TextFormat.shortDebugString(Status.parseFrom(entry.getValue().get())));
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
    getCluster().getClusterControl().stop(ServerType.GARBAGE_COLLECTOR);

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
        sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
      }
    }

    Table.ID tableId = Table.ID.of(conn.tableOperations().tableIdMap().get(table1));
    Assert.assertNotNull("Could not determine table id for " + table1, tableId);

    // Write some data to table1
    writeSomeData(conn, table1, 2000, 50);
    conn.tableOperations().flush(table1, null, null, true);

    // Make sure the replication table exists at this point
    while (!ReplicationTable.isOnline(conn)) {
      sleepUninterruptibly(MILLIS_BETWEEN_REPLICATION_TABLE_ONLINE_CHECKS, TimeUnit.MILLISECONDS);
    }
    Assert.assertTrue("Replication table did not exist", ReplicationTable.isOnline(conn));

    // Grant ourselves the write permission for later
    conn.securityOperations().grantTablePermission("root", ReplicationTable.NAME, TablePermission.WRITE);

    log.info("Checking for replication entries in replication");
    // Then we need to get those records over to the replication table
    Scanner s;
    Set<String> entries = new HashSet<>();
    for (int i = 0; i < 5; i++) {
      s = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
      s.setRange(ReplicationSection.getRange());
      entries.clear();
      for (Entry<Key,Value> entry : s) {
        entries.add(entry.getKey().getRow().toString());
        log.info("{}={}", entry.getKey().toStringNoTruncate(), entry.getValue());
      }
      if (!entries.isEmpty()) {
        log.info("Replication entries {}", entries);
        break;
      }
      Thread.sleep(1000);
    }

    Assert.assertFalse("Did not find any replication entries in the replication table", entries.isEmpty());

    // Find the WorkSection record that will be created for that data we ingested
    boolean notFound = true;
    for (int i = 0; i < 10 && notFound; i++) {
      try {
        s = ReplicationTable.getScanner(conn);
        WorkSection.limit(s);
        Entry<Key,Value> e = Iterables.getOnlyElement(s);
        log.info("Found entry: {}", e.getKey().toStringNoTruncate());
        Text expectedColqual = new ReplicationTarget("cluster1", "4", tableId).toText();
        Assert.assertEquals(expectedColqual, e.getKey().getColumnQualifier());
        notFound = false;
      } catch (NoSuchElementException e) {

      } catch (IllegalArgumentException e) {
        // Somehow we got more than one element. Log what they were
        s = ReplicationTable.getScanner(conn);
        for (Entry<Key,Value> content : s) {
          log.info("{} => {}", content.getKey().toStringNoTruncate(), content.getValue());
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
        log.info("{} => {}", content.getKey().toStringNoTruncate(), ProtobufUtil.toString(Status.parseFrom(content.getValue().get())));
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
    cluster.getClusterControl().stop(ServerType.TABLET_SERVER);

    log.info("Starting tserver");
    cluster.getClusterControl().start(ServerType.TABLET_SERVER);

    log.info("Waiting to read tables");
    sleepUninterruptibly(2 * 3, TimeUnit.SECONDS);

    // Make sure we can read all the tables (recovery complete)
    for (String table : new String[] {MetadataTable.NAME, table1}) {
      Iterators.size(conn.createScanner(table, Authorizations.EMPTY).iterator());
    }

    log.info("Recovered metadata:");
    s = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    for (Entry<Key,Value> entry : s) {
      log.info("{}={}", entry.getKey().toStringNoTruncate(), entry.getValue());
    }

    cluster.getClusterControl().start(ServerType.GARBAGE_COLLECTOR);

    // Wait for a bit since the GC has to run (should be running after a one second delay)
    waitForGCLock(conn);

    Thread.sleep(1000);

    log.info("After GC");
    s = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    for (Entry<Key,Value> entry : s) {
      log.info("{}={}", entry.getKey().toStringNoTruncate(), entry.getValue());
    }

    // We expect no records in the metadata table after compaction. We have to poll
    // because we have to wait for the StatusMaker's next iteration which will clean
    // up the dangling *closed* records after we create the record in the replication table.
    // We need the GC to close the file (CloseWriteAheadLogReferences) before we can remove the record
    log.info("Checking metadata table for replication entries");
    Set<String> remaining = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      s = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
      s.setRange(ReplicationSection.getRange());
      remaining.clear();
      for (Entry<Key,Value> e : s) {
        remaining.add(e.getKey().getRow().toString());
      }
      remaining.retainAll(entries);
      if (remaining.isEmpty()) {
        break;
      }
      log.info("remaining {}", remaining);
      Thread.sleep(2000);
      log.info("");
    }

    Assert.assertTrue("Replication status messages were not cleaned up from metadata table", remaining.isEmpty());

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
        log.info("{} {}", entry.getKey().toStringNoTruncate(), ProtobufUtil.toString(Status.parseFrom(entry.getValue().get())));
      }

      if (recordsFound <= 2) {
        break;
      } else {
        Thread.sleep(1000);
        log.info("");
      }
    }

    Assert.assertTrue("Found unexpected replication records in the replication table", recordsFound <= 2);
  }
}
