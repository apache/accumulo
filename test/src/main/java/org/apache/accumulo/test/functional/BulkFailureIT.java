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

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.InvalidTabletHostingRequestException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.ClientTabletCache;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.BulkFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.tabletingest.thrift.DataFileInfo;
import org.apache.accumulo.core.tabletingest.thrift.TabletIngestClientService;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.tablets.UniqueNameAllocator;
import org.apache.accumulo.server.zookeeper.TransactionWatcher.ZooArbitrator;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;

@Disabled // ELASTICITY_TODO
public class BulkFailureIT extends AccumuloClusterHarness {

  private static final Logger LOG = LoggerFactory.getLogger(BulkFailureIT.class);

  interface Loader {
    void load(long txid, ClientContext context, KeyExtent extent, Path path, long size,
        boolean expectFailure) throws Exception;
  }

  @Test
  public void testImportCompactionImport() throws Exception {
    String[] tables = getUniqueNames(2);

    // run test calling new bulk import RPCs
    runTest(tables[1], 22222222L, BulkFailureIT::newLoad);
  }

  private static Path createNewBulkDir(ServerContext context, VolumeManager fs, String sourceDir,
      TableId tableId) throws IOException {
    Path tableDir = fs.matchingFileSystem(new Path(sourceDir), context.getTablesDirs());
    if (tableDir == null) {
      throw new IOException(
          sourceDir + " is not in the same file system as any volume configured for Accumulo");
    }

    Path directory = new Path(tableDir, tableId.canonical());
    fs.mkdirs(directory);

    // only one should be able to create the lock file
    // the purpose of the lock file is to avoid a race
    // condition between the call to fs.exists() and
    // fs.mkdirs()... if only hadoop had a mkdir() function
    // that failed when the dir existed

    UniqueNameAllocator namer = context.getUniqueNameAllocator();

    while (true) {
      Path newBulkDir = new Path(directory, Constants.BULK_PREFIX + namer.getNextName());
      if (fs.exists(newBulkDir)) { // sanity check
        throw new IOException("Dir exist when it should not " + newBulkDir);
      }
      if (fs.mkdirs(newBulkDir)) {
        return newBulkDir;
      }

      sleepUninterruptibly(3, TimeUnit.SECONDS);
    }
  }

  public static String prepareBulkImport(ServerContext manager, final VolumeManager fs, String dir,
      TableId tableId, long tid) throws Exception {
    final Path bulkDir = createNewBulkDir(manager, fs, dir, tableId);

    manager.getAmple().addBulkLoadInProgressFlag(
        "/" + bulkDir.getParent().getName() + "/" + bulkDir.getName(), tid);

    Path dirPath = new Path(dir);
    FileStatus[] dataFiles = fs.listStatus(dirPath);

    final UniqueNameAllocator namer = manager.getUniqueNameAllocator();

    AccumuloConfiguration serverConfig = manager.getConfiguration();
    int numThreads = serverConfig.getCount(Property.MANAGER_RENAME_THREADS);
    ExecutorService workers =
        ThreadPools.getServerThreadPools().createFixedThreadPool(numThreads, "bulk rename", false);
    List<Future<Exception>> results = new ArrayList<>();

    for (FileStatus file : dataFiles) {
      final FileStatus fileStatus = file;
      results.add(workers.submit(() -> {
        try {
          String[] sa = fileStatus.getPath().getName().split("\\.");
          String extension = "";
          if (sa.length > 1) {
            extension = sa[sa.length - 1];

            if (!FileOperations.getValidExtensions().contains(extension)) {
              LOG.warn("{} does not have a valid extension, ignoring", fileStatus.getPath());
              return null;
            }
          } else {
            LOG.warn("{} does not have any extension, ignoring", fileStatus.getPath());
            return null;
          }

          String newName = "I" + namer.getNextName() + "." + extension;
          Path newPath = new Path(bulkDir, newName);
          try {
            fs.rename(fileStatus.getPath(), newPath);
            LOG.debug("Moved {} to {}", fileStatus.getPath(), newPath);
          } catch (IOException E1) {
            LOG.error("Could not move: {} {}", fileStatus.getPath(), E1.getMessage());
          }

        } catch (Exception ex) {
          return ex;
        }
        return null;
      }));
    }
    workers.shutdown();
    while (!workers.awaitTermination(1000L, TimeUnit.MILLISECONDS)) {}

    for (Future<Exception> ex : results) {
      if (ex.get() != null) {
        throw ex.get();
      }
    }
    return bulkDir.toString();
  }

  /**
   * This test verifies two things. First it ensures that after a bulk imported file is compacted
   * that import request are ignored. Second it ensures that after the bulk import transaction is
   * canceled that import request fail. The public API for bulk import can not be used for this
   * test. Internal (non public API) RPCs and Zookeeper state is manipulated directly. This is the
   * only way to interleave compactions with multiple, duplicate import RPC request.
   */
  protected void runTest(String table, long fateTxid, Loader loader) throws IOException,
      AccumuloException, AccumuloSecurityException, TableExistsException, KeeperException,
      InterruptedException, Exception, FileNotFoundException, TableNotFoundException {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      SortedMap<Key,Value> testData = createTestData();

      FileSystem fs = getCluster().getFileSystem();
      String testFile = createTestFile(fateTxid, testData, fs);

      c.tableOperations().create(table);
      String tableId = c.tableOperations().tableIdMap().get(table);

      // Table has no splits, so this extent corresponds to the tables single tablet
      KeyExtent extent = new KeyExtent(TableId.of(tableId), null, null);

      ServerContext asCtx = getServerContext();
      ZooArbitrator.start(asCtx, Constants.BULK_ARBITRATOR_TYPE, fateTxid);

      VolumeManager vm = asCtx.getVolumeManager();

      // move the file into a directory for the table and rename the file to something unique
      String bulkDir = prepareBulkImport(asCtx, vm, testFile, TableId.of(tableId), fateTxid);
      assertNotNull(bulkDir);

      // determine the files new name and path
      FileStatus status = fs.listStatus(new Path(bulkDir))[0];
      Path bulkLoadPath = fs.makeQualified(status.getPath());

      // Directly ask the tablet to load the file.
      loader.load(fateTxid, asCtx, extent, bulkLoadPath, status.getLen(), false);

      assertEquals(Set.of(bulkLoadPath), getFiles(c, extent));
      assertEquals(Set.of(bulkLoadPath), getLoaded(c, extent));
      assertEquals(testData, readTable(table, c));

      // Compact the bulk imported file. Subsequent request to load the file should be ignored.
      c.tableOperations().compact(table, new CompactionConfig().setWait(true));

      Set<Path> tabletFiles = getFiles(c, extent);
      assertFalse(tabletFiles.contains(bulkLoadPath));
      assertEquals(1, tabletFiles.size());
      assertEquals(Set.of(bulkLoadPath), getLoaded(c, extent));
      assertEquals(testData, readTable(table, c));

      // this request should be ignored by the tablet
      loader.load(fateTxid, asCtx, extent, bulkLoadPath, status.getLen(), false);

      assertEquals(tabletFiles, getFiles(c, extent));
      assertEquals(Set.of(bulkLoadPath), getLoaded(c, extent));
      assertEquals(testData, readTable(table, c));

      // this is done to ensure the tablet reads the load flags from the metadata table when it
      // loads
      c.tableOperations().offline(table, true);
      c.tableOperations().online(table, true);

      // this request should be ignored by the tablet
      loader.load(fateTxid, asCtx, extent, bulkLoadPath, status.getLen(), false);

      assertEquals(tabletFiles, getFiles(c, extent));
      assertEquals(Set.of(bulkLoadPath), getLoaded(c, extent));
      assertEquals(testData, readTable(table, c));

      // After this, all load request should fail.
      ZooArbitrator.stop(asCtx, Constants.BULK_ARBITRATOR_TYPE, fateTxid);

      c.securityOperations().grantTablePermission(c.whoami(), MetadataTable.NAME,
          TablePermission.WRITE);

      BatchDeleter bd = c.createBatchDeleter(MetadataTable.NAME, Authorizations.EMPTY, 1);
      bd.setRanges(Collections.singleton(extent.toMetaRange()));
      bd.fetchColumnFamily(BulkFileColumnFamily.NAME);
      bd.delete();

      loader.load(fateTxid, asCtx, extent, bulkLoadPath, status.getLen(), true);

      assertEquals(tabletFiles, getFiles(c, extent));
      assertEquals(Set.of(), getLoaded(c, extent));
      assertEquals(testData, readTable(table, c));
    }
  }

  private SortedMap<Key,Value> createTestData() {
    SortedMap<Key,Value> testData = new TreeMap<>();
    testData.put(new Key("r001", "f002", "q009", 56), new Value("v001"));
    testData.put(new Key("r001", "f002", "q019", 56), new Value("v002"));
    testData.put(new Key("r002", "f002", "q009", 57), new Value("v003"));
    testData.put(new Key("r002", "f002", "q019", 57), new Value("v004"));
    return testData;
  }

  private String createTestFile(long txid, SortedMap<Key,Value> testData, FileSystem fs)
      throws IOException {
    Path base = new Path(getCluster().getTemporaryPath(), "testBulk_ICI_" + txid);

    fs.delete(base, true);
    fs.mkdirs(base);
    Path files = new Path(base, "files");

    try (RFileWriter writer =
        RFile.newWriter().to(new Path(files, "ici_01.rf").toString()).withFileSystem(fs).build()) {
      writer.append(testData.entrySet());
    }

    String filesStr = fs.makeQualified(files).toString();
    return filesStr;
  }

  private SortedMap<Key,Value> readTable(String table, AccumuloClient connector)
      throws TableNotFoundException {
    Scanner scanner = connector.createScanner(table, Authorizations.EMPTY);

    SortedMap<Key,Value> actual = new TreeMap<>();

    for (Entry<Key,Value> entry : scanner) {
      actual.put(entry.getKey(), entry.getValue());
    }

    return actual;
  }

  public static Set<Path> getLoaded(AccumuloClient connector, KeyExtent extent)
      throws TableNotFoundException {
    return getPaths(connector, extent, BulkFileColumnFamily.NAME);
  }

  public static Set<Path> getFiles(AccumuloClient connector, KeyExtent extent)
      throws TableNotFoundException {
    return getPaths(connector, extent, DataFileColumnFamily.NAME);
  }

  private static Set<Path> getPaths(AccumuloClient connector, KeyExtent extent, Text fam)
      throws TableNotFoundException {
    HashSet<Path> files = new HashSet<>();

    Scanner scanner = connector.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    scanner.setRange(extent.toMetaRange());
    scanner.fetchColumnFamily(fam);

    for (Entry<Key,Value> entry : scanner) {
      files.add(StoredTabletFile.of(entry.getKey().getColumnQualifier()).getPath());
    }

    return files;
  }

  private static void newLoad(long txid, ClientContext context, KeyExtent extent, Path path,
      long size, boolean expectFailure) throws Exception {

    TabletIngestClientService.Iface client = getClient(context, extent);
    try {

      Map<String,DataFileInfo> val = Map.of(path.getName(), new DataFileInfo(size));
      Map<KeyExtent,Map<String,DataFileInfo>> files = Map.of(extent, val);

      // ELASTICITY_TODO this used to call bulk import directly on tserver, need to look into the
      // bigger picture of what this test was doing and how it can work w/ the new bulk import
      throw new UnsupportedOperationException();
      /*
       * client.loadFiles(TraceUtil.traceInfo(), context.rpcCreds(), txid,
       * path.getParent().toString(), files.entrySet().stream().collect( Collectors.toMap(entry ->
       * entry.getKey().toThrift(), Entry::getValue)), false);
       *
       * if (!expectFailure) { while (!getLoaded(context, extent).contains(path)) {
       * Thread.sleep(100); } }
       */
    } finally {
      ThriftUtil.returnClient((TServiceClient) client, context);
    }
  }

  protected static TabletIngestClientService.Iface getClient(ClientContext context,
      KeyExtent extent) throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
      TTransportException, InvalidTabletHostingRequestException {
    ClientTabletCache locator = ClientTabletCache.getInstance(context, extent.tableId());

    locator.invalidateCache(extent);

    HostAndPort location = HostAndPort.fromString(locator
        .findTabletWithRetry(context, new Text(""), false, ClientTabletCache.LocationNeed.REQUIRED)
        .getTserverLocation().orElseThrow());

    long timeInMillis = context.getConfiguration().getTimeInMillis(Property.MANAGER_BULK_TIMEOUT);
    TabletIngestClientService.Iface client =
        ThriftUtil.getClient(ThriftClientTypes.TABLET_INGEST, location, context, timeInMillis);
    return client;
  }
}
