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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.TabletLocator;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.MapFileInfo;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.BulkFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.manager.tableOps.bulkVer1.BulkImport;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.zookeeper.TransactionWatcher.ZooArbitrator;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.Test;

public class BulkFailureIT extends AccumuloClusterHarness {

  interface Loader {
    void load(long txid, ClientContext context, KeyExtent extent, Path path, long size,
        boolean expectFailure) throws Exception;
  }

  @Test
  public void testImportCompactionImport() throws Exception {
    String[] tables = getUniqueNames(2);

    // run test calling old bulk import RPCs
    runTest(tables[0], 99999999L, BulkFailureIT::oldLoad);

    // run test calling new bulk import RPCs
    runTest(tables[1], 22222222L, BulkFailureIT::newLoad);
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
      String bulkDir =
          BulkImport.prepareBulkImport(asCtx, vm, testFile, TableId.of(tableId), fateTxid);

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
      files.add(new Path(entry.getKey().getColumnQualifierData().toString()));
    }

    return files;
  }

  private static void oldLoad(long txid, ClientContext context, KeyExtent extent, Path path,
      long size, boolean expectFailure) throws Exception {

    TabletClientService.Iface client = getClient(context, extent);
    try {

      Map<String,MapFileInfo> val = Map.of(path.toString(), new MapFileInfo(size));
      Map<KeyExtent,Map<String,MapFileInfo>> files = Map.of(extent, val);

      client.bulkImport(TraceUtil.traceInfo(), context.rpcCreds(), txid, files.entrySet().stream()
          .collect(Collectors.toMap(entry -> entry.getKey().toThrift(), Entry::getValue)), false);
      if (expectFailure) {
        fail("Expected RPC to fail");
      }
    } catch (TApplicationException tae) {
      if (!expectFailure) {
        throw tae;
      }
    } finally {
      ThriftUtil.returnClient((TServiceClient) client, context);
    }
  }

  private static void newLoad(long txid, ClientContext context, KeyExtent extent, Path path,
      long size, boolean expectFailure) throws Exception {

    TabletClientService.Iface client = getClient(context, extent);
    try {

      Map<String,MapFileInfo> val = Map.of(path.getName(), new MapFileInfo(size));
      Map<KeyExtent,Map<String,MapFileInfo>> files = Map.of(extent, val);

      client.loadFiles(TraceUtil.traceInfo(), context.rpcCreds(), txid, path.getParent().toString(),
          files.entrySet().stream().collect(
              Collectors.toMap(entry -> entry.getKey().toThrift(), Entry::getValue)),
          false);

      if (!expectFailure) {
        while (!getLoaded(context, extent).contains(path)) {
          Thread.sleep(100);
        }
      }

    } finally {
      ThriftUtil.returnClient((TServiceClient) client, context);
    }
  }

  protected static TabletClientService.Iface getClient(ClientContext context, KeyExtent extent)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
      TTransportException {
    TabletLocator locator = TabletLocator.getLocator(context, extent.tableId());

    locator.invalidateCache(extent);

    HostAndPort location = HostAndPort
        .fromString(locator.locateTablet(context, new Text(""), false, true).tablet_location);

    long timeInMillis = context.getConfiguration().getTimeInMillis(Property.TSERV_BULK_TIMEOUT);
    TabletClientService.Iface client =
        ThriftUtil.getClient(ThriftClientTypes.TABLET_SERVER, location, context, timeInMillis);
    return client;
  }
}
