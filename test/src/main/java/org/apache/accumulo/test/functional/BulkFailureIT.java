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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.TabletLocator;
import org.apache.accumulo.core.client.impl.Translator;
import org.apache.accumulo.core.client.impl.Translators;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.data.thrift.MapFileInfo;
import org.apache.accumulo.core.data.thrift.TKeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.BulkFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.trace.Tracer;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.master.tableOps.BulkImport;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.zookeeper.TransactionWatcher.ZooArbitrator;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TServiceClient;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class BulkFailureIT extends AccumuloClusterHarness {

  /**
   * This test verifies two things. First it ensures that after a bulk imported file is compacted
   * that import request are ignored. Second it ensures that after the bulk import transaction is
   * canceled that import request fail. The public API for bulk import can not be used for this
   * test. Internal (non public API) RPCs and Zookeeper state is manipulated directly. This is the
   * only way to interleave compactions with multiple, duplicate import RPC request.
   */
  @Test
  public void testImportCompactionImport() throws Exception {
    Connector c = getConnector();
    String table = getUniqueNames(1)[0];

    SortedMap<Key,Value> testData = createTestData();

    FileSystem fs = getCluster().getFileSystem();
    String testFile = createTestFile(testData, fs);

    c.tableOperations().create(table);
    String tableId = c.tableOperations().tableIdMap().get(table);

    // Table has no splits, so this extent corresponds to the tables single tablet
    KeyExtent extent = new KeyExtent(tableId.toString(), null, null);

    // Set up site configuration because this test uses server side code that expects it.
    setupSiteConfig();

    long fateTxid = 99999999L;

    AccumuloServerContext asCtx = new AccumuloServerContext(
        new ServerConfigurationFactory(HdfsZooInstance.getInstance()));
    ZooArbitrator.start(Constants.BULK_ARBITRATOR_TYPE, fateTxid);

    VolumeManager vm = VolumeManagerImpl.get();

    // move the file into a directory for the table and rename the file to something unique
    String bulkDir = BulkImport.prepareBulkImport(asCtx, vm, testFile, tableId);

    // determine the files new name and path
    FileStatus status = fs.listStatus(new Path(bulkDir))[0];
    Path bulkLoadPath = fs.makeQualified(status.getPath());

    // Directly ask the tablet to load the file.
    assignMapFiles(fateTxid, asCtx, extent, bulkLoadPath.toString(), status.getLen());

    assertEquals(ImmutableSet.of(bulkLoadPath), getFiles(c, extent));
    assertEquals(ImmutableSet.of(bulkLoadPath), getLoaded(c, extent));
    assertEquals(testData, readTable(table, c));

    // Compact the bulk imported file. Subsequent request to load the file should be ignored.
    c.tableOperations().compact(table, new CompactionConfig().setWait(true));

    Set<Path> tabletFiles = getFiles(c, extent);
    assertFalse(tabletFiles.contains(bulkLoadPath));
    assertEquals(1, tabletFiles.size());
    assertEquals(ImmutableSet.of(bulkLoadPath), getLoaded(c, extent));
    assertEquals(testData, readTable(table, c));

    // this request should be ignored by the tablet
    assignMapFiles(fateTxid, asCtx, extent, bulkLoadPath.toString(), status.getLen());

    assertEquals(tabletFiles, getFiles(c, extent));
    assertEquals(ImmutableSet.of(bulkLoadPath), getLoaded(c, extent));
    assertEquals(testData, readTable(table, c));

    // this is done to ensure the tablet reads the load flags from the metadata table when it loads
    c.tableOperations().offline(table, true);
    c.tableOperations().online(table, true);

    // this request should be ignored by the tablet
    assignMapFiles(fateTxid, asCtx, extent, bulkLoadPath.toString(), status.getLen());

    assertEquals(tabletFiles, getFiles(c, extent));
    assertEquals(ImmutableSet.of(bulkLoadPath), getLoaded(c, extent));
    assertEquals(testData, readTable(table, c));

    // After this, all load request should fail.
    ZooArbitrator.stop(Constants.BULK_ARBITRATOR_TYPE, fateTxid);

    try {
      // expect this to fail
      assignMapFiles(fateTxid, asCtx, extent, bulkLoadPath.toString(), status.getLen());
      fail();
    } catch (TApplicationException tae) {

    }

    assertEquals(tabletFiles, getFiles(c, extent));
    assertEquals(ImmutableSet.of(bulkLoadPath), getLoaded(c, extent));
    assertEquals(testData, readTable(table, c));
  }

  private SortedMap<Key,Value> createTestData() {
    SortedMap<Key,Value> testData = new TreeMap<>();
    testData.put(new Key("r001", "f002", "q009", 56), new Value("v001"));
    testData.put(new Key("r001", "f002", "q019", 56), new Value("v002"));
    testData.put(new Key("r002", "f002", "q009", 57), new Value("v003"));
    testData.put(new Key("r002", "f002", "q019", 57), new Value("v004"));
    return testData;
  }

  private String createTestFile(SortedMap<Key,Value> testData, FileSystem fs) throws IOException {
    Path base = new Path(getCluster().getTemporaryPath(), "testBulk_ICI");

    fs.delete(base, true);
    fs.mkdirs(base);
    Path files = new Path(base, "files");

    try (RFileWriter writer = RFile.newWriter().to(new Path(files, "ici_01.rf").toString())
        .withFileSystem(fs).build()) {
      writer.append(testData.entrySet());
    }

    String filesStr = fs.makeQualified(files).toString();
    return filesStr;
  }

  private SortedMap<Key,Value> readTable(String table, Connector connector)
      throws TableNotFoundException {
    Scanner scanner = connector.createScanner(table, Authorizations.EMPTY);

    SortedMap<Key,Value> actual = new TreeMap<>();

    for (Entry<Key,Value> entry : scanner) {
      actual.put(entry.getKey(), entry.getValue());
    }

    return actual;
  }

  public Set<Path> getLoaded(Connector connector, KeyExtent extent) throws TableNotFoundException {
    return getPaths(connector, extent, BulkFileColumnFamily.NAME);
  }

  public Set<Path> getFiles(Connector connector, KeyExtent extent) throws TableNotFoundException {
    return getPaths(connector, extent, DataFileColumnFamily.NAME);
  }

  private Set<Path> getPaths(Connector connector, KeyExtent extent, Text fam)
      throws TableNotFoundException {
    HashSet<Path> files = new HashSet<>();

    Scanner scanner = connector.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    scanner.setRange(extent.toMetadataRange());
    scanner.fetchColumnFamily(fam);

    for (Entry<Key,Value> entry : scanner) {
      files.add(new Path(entry.getKey().getColumnQualifierData().toString()));
    }

    return files;
  }

  private List<KeyExtent> assignMapFiles(long txid, ClientContext context, KeyExtent extent,
      String path, long size) throws Exception {

    TabletLocator locator = TabletLocator.getLocator(context, extent.getTableId());

    locator.invalidateCache(extent);

    HostAndPort location = HostAndPort
        .fromString(locator.locateTablet(context, new Text(""), false, true).tablet_location);

    long timeInMillis = context.getConfiguration().getTimeInMillis(Property.TSERV_BULK_TIMEOUT);
    TabletClientService.Iface client = ThriftUtil.getTServerClient(location, context, timeInMillis);
    try {

      Map<String,MapFileInfo> val = ImmutableMap.of(path, new MapFileInfo(size));
      Map<KeyExtent,Map<String,MapFileInfo>> files = ImmutableMap.of(extent, val);

      List<TKeyExtent> failures = client.bulkImport(Tracer.traceInfo(), context.rpcCreds(), txid,
          Translator.translate(files, Translators.KET), false);

      return Translator.translate(failures, Translators.TKET);
    } finally {
      ThriftUtil.returnClient((TServiceClient) client);
    }

  }

  private void setupSiteConfig() throws AccumuloException, AccumuloSecurityException {
    for (Entry<String,String> entry : getCluster().getSiteConfiguration()) {
      SiteConfiguration.getInstance().set(entry.getKey(), entry.getValue());
    }
  }
}
