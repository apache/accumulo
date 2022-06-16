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
package org.apache.accumulo.manager.upgrade;

import static org.apache.accumulo.core.Constants.BULK_PREFIX;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.gc.ReferenceFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.core.volume.VolumeImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.gc.AllVolumesDirectory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Upgrader9to10Test {
  private static Logger log = LoggerFactory.getLogger(Upgrader9to10Test.class);

  private static final String VOL_PROP = "hdfs://nn1:8020/accumulo";
  private static final TableId tableId5a = TableId.of("5a");

  @Test
  public void testSwitchRelativeDeletes() {
    Path resolved = Upgrader9to10.resolveRelativeDelete("/5a/t-0005", VOL_PROP);
    assertEquals(new Path(VOL_PROP + "/tables/5a/t-0005"), resolved);
    var allVolumesDir = new AllVolumesDirectory(tableId5a, "t-0005");
    var ref1 = Upgrader9to10.switchToAllVolumes(resolved);
    compareReferences(allVolumesDir, ref1);

    resolved = Upgrader9to10.resolveRelativeDelete("/5a/" + BULK_PREFIX + "0005", VOL_PROP);
    assertEquals(new Path(VOL_PROP + "/tables/5a/" + BULK_PREFIX + "0005"), resolved);
    ref1 = new ReferenceFile(tableId5a, VOL_PROP + "/tables/5a/" + BULK_PREFIX + "0005");
    var ref2 = Upgrader9to10.switchToAllVolumes(resolved);
    compareReferences(ref1, ref2);

    resolved = Upgrader9to10.resolveRelativeDelete("/5a/t-0005/F0009.rf", VOL_PROP);
    assertEquals(new Path(VOL_PROP + "/tables/5a/t-0005/F0009.rf"), resolved);
    ref1 = new ReferenceFile(tableId5a, VOL_PROP + "/tables/5a/t-0005/F0009.rf");
    ref2 = Upgrader9to10.switchToAllVolumes(resolved);
    compareReferences(ref1, ref2);
  }

  private void compareReferences(ReferenceFile ref1, ReferenceFile ref2) {
    assertEquals(ref1.getMetadataEntry(), ref2.getMetadataEntry());
    assertEquals(ref1.tableId, ref2.tableId);
  }

  @Test
  public void testBadRelativeDeleteTooShort() {
    assertThrows(IllegalStateException.class,
        () -> Upgrader9to10.resolveRelativeDelete("/5a", VOL_PROP));
  }

  @Test
  public void testBadRelativeDeleteTooLong() throws Exception {
    assertThrows(IllegalStateException.class,
        () -> Upgrader9to10.resolveRelativeDelete("/5a/5a/t-0005/F0009.rf", VOL_PROP));
  }

  @Test
  public void testSwitchAllVolumes() {
    Path resolved = Upgrader9to10
        .resolveRelativeDelete("hdfs://localhost:9000/accumulo/tables/5a/t-0005", VOL_PROP);
    var allVolumesDir = new AllVolumesDirectory(tableId5a, "t-0005");
    var ref1 = Upgrader9to10.switchToAllVolumes(resolved);
    compareReferences(allVolumesDir, ref1);

    resolved = Upgrader9to10.resolveRelativeDelete(
        "hdfs://localhost:9000/accumulo/tables/5a/" + BULK_PREFIX + "0005", VOL_PROP);
    ref1 = new ReferenceFile(tableId5a,
        "hdfs://localhost:9000/accumulo/tables/5a/" + BULK_PREFIX + "0005");
    var ref2 = Upgrader9to10.switchToAllVolumes(resolved);
    compareReferences(ref1, ref2);

    resolved = Upgrader9to10.resolveRelativeDelete(
        "hdfs://localhost:9000/accumulo/tables/5a/t-0005/C0009.rf", VOL_PROP);
    ref1 = new ReferenceFile(tableId5a, "hdfs://localhost:9000/accumulo/tables/5a/t-0005/C0009.rf");
    ref2 = Upgrader9to10.switchToAllVolumes(resolved);
    compareReferences(ref1, ref2);
  }

  @Test
  public void testUpgradeDir() {
    assertEquals("t-0005",
        Upgrader9to10.upgradeDirColumn("hdfs://localhost:9000/accumulo/tables/5a/t-0005"));
    assertEquals("t-0005", Upgrader9to10.upgradeDirColumn("../5a/t-0005"));
    assertEquals("t-0005", Upgrader9to10.upgradeDirColumn("/t-0005"));
    assertEquals("t-0005", Upgrader9to10.upgradeDirColumn("t-0005"));
  }

  String tableName = Ample.DataLevel.USER.metaTable();
  String volumeUpgrade = "file:///accumulo";

  // mock objects for testing relative path replacement
  private void setupMocks(AccumuloClient c, VolumeManager fs, SortedMap<Key,Value> map,
      List<Mutation> results) throws Exception {
    Scanner scanner = createMock(Scanner.class);
    // buffer all the mutations that are created so we can verify they are correct
    BatchWriter writer = new BatchWriter() {
      List<Mutation> buffer = new ArrayList<>();

      @Override
      public void addMutation(Mutation m) throws MutationsRejectedException {
        buffer.add(m);
      }

      @Override
      public void addMutations(Iterable<Mutation> iterable) throws MutationsRejectedException {
        iterable.forEach(buffer::add);
      }

      @Override
      public void flush() throws MutationsRejectedException {}

      @Override
      // simulate the close by adding all to results and preventing anymore adds
      public void close() throws MutationsRejectedException {
        results.addAll(buffer);
        buffer = null;
      }
    };

    expect(c.createScanner(tableName, Authorizations.EMPTY)).andReturn(scanner).anyTimes();
    expect(c.createBatchWriter(tableName)).andReturn(writer).anyTimes();
    expect(scanner.iterator()).andReturn(map.entrySet().iterator()).anyTimes();

    // void methods
    scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
    expectLastCall().anyTimes();
    scanner.close();
    expectLastCall().anyTimes();

    replay(c, fs, scanner);
  }

  @Test
  public void noRelativePaths() throws Exception {
    VolumeManager fs = createMock(VolumeManager.class);
    AccumuloClient c = createMock(AccumuloClient.class);
    expect(fs.exists(anyObject())).andReturn(true).anyTimes();

    SortedMap<Key,Value> map = new TreeMap<>();
    map.put(new Key("1<", "file", "hdfs://nn1:8020/accumulo/tables/1/default_tablet/A000001c.rf"),
        new Value());
    map.put(new Key("1<", "file", "hdfs://nn1:8020/accumulo/tables/1/default_tablet/F000001m.rf"),
        new Value());
    map.put(new Key("1<", "file", "file://nn1:8020/accumulo/tables/1/t-0005/F000004x.rf"),
        new Value());
    map.put(new Key("1<", "file", "file://volume23:8000/accumulo/tables/1/t-1234/F0000054.rf"),
        new Value());

    List<Mutation> results = new ArrayList<>();

    setupMocks(c, fs, map, results);
    assertFalse(Upgrader9to10.checkForRelativePaths(c, fs, tableName, volumeUpgrade),
        "Invalid Relative path check");
    assertTrue(results.isEmpty());
  }

  @Test
  public void filesDontExistAfterReplacingRelatives() throws Exception {
    AccumuloClient c = createMock(AccumuloClient.class);
    VolumeManager fs = createMock(VolumeManager.class);
    SortedMap<Key,Value> map = new TreeMap<>();
    map.put(new Key("1b;row_000050", "file", "../1b/default_tablet/A000001c.rf"), new Value("1"));
    map.put(new Key("1b;row_000050", "file", "../1b/default_tablet/F000001m.rf"), new Value("2"));

    expect(fs.exists(anyObject(Path.class))).andReturn(false).anyTimes();

    setupMocks(c, fs, map, new ArrayList<>());
    assertThrows(IllegalArgumentException.class,
        () -> Upgrader9to10.checkForRelativePaths(c, fs, tableName, volumeUpgrade));
  }

  @Test
  public void missingUpgradeRelativeProperty() throws Exception {
    AccumuloClient c = createMock(AccumuloClient.class);
    VolumeManager fs = createMock(VolumeManager.class);
    SortedMap<Key,Value> map = new TreeMap<>();
    map.put(new Key("1b;row_000050", "file", "../1b/default_tablet/A000001c.rf"), new Value("1"));
    map.put(new Key("1b;row_000050", "file", "../1b/default_tablet/F000001m.rf"), new Value("2"));

    expect(fs.exists(anyObject(Path.class))).andReturn(false).anyTimes();

    setupMocks(c, fs, map, new ArrayList<>());
    assertThrows(IllegalArgumentException.class,
        () -> Upgrader9to10.checkForRelativePaths(c, fs, tableName, ""));
  }

  @Test
  public void replaceRelatives() throws Exception {
    AccumuloClient c = createMock(AccumuloClient.class);
    VolumeManager fs = createMock(VolumeManager.class);
    expect(fs.exists(anyObject())).andReturn(true).anyTimes();

    SortedMap<Key,Value> map = new TreeMap<>();
    map.put(new Key("1b;row_000050", "file", "../1b/default_tablet/A000001c.rf"), new Value("1"));
    map.put(new Key("1b;row_000050", "file", "../1b/default_tablet/F000001m.rf"), new Value("2"));
    map.put(new Key("1b;row_000050", "file", "../1b/t-000008t/F000004x.rf"), new Value("3"));
    map.put(new Key("1b;row_000050", "file", "/t-000008t/F0000054.rf"), new Value("4"));
    map.put(new Key("1b<", "file", "../1b/default_tablet/A000001c.rf"), new Value("1"));
    map.put(new Key("1b<", "file", "../1b/default_tablet/F000001m.rf"), new Value("2"));
    map.put(new Key("1b<", "file", "../1b/t-000008t/F000004x.rf"), new Value("3"));
    map.put(new Key("1b<", "file", "/t-000008t/F0000054.rf"), new Value("4"));
    map.put(new Key("1b<", "file", "hdfs://nn1:8020/accumulo/tables/1b/t-000008t/A0000098.rf"),
        new Value("5"));
    map.put(new Key("1b<", "file", "hdfs://nn1:8020/accumulo/tables/1b/t-000008t/F0000098.rf"),
        new Value("5"));

    List<Mutation> expected = new ArrayList<>();
    expected.add(replaceMut("1b;row_000050", "file:/accumulo/tables/1b/default_tablet/A000001c.rf",
        "1", "../1b/default_tablet/A000001c.rf"));
    expected.add(replaceMut("1b;row_000050", "file:/accumulo/tables/1b/default_tablet/F000001m.rf",
        "2", "../1b/default_tablet/F000001m.rf"));
    expected.add(replaceMut("1b;row_000050", "file:/accumulo/tables/1b/t-000008t/F000004x.rf", "3",
        "../1b/t-000008t/F000004x.rf"));
    expected.add(replaceMut("1b;row_000050", "file:/accumulo/tables/1b/t-000008t/F0000054.rf", "4",
        "/t-000008t/F0000054.rf"));
    expected.add(replaceMut("1b<", "file:/accumulo/tables/1b/default_tablet/A000001c.rf", "1",
        "../1b/default_tablet/A000001c.rf"));
    expected.add(replaceMut("1b<", "file:/accumulo/tables/1b/default_tablet/F000001m.rf", "2",
        "../1b/default_tablet/F000001m.rf"));
    expected.add(replaceMut("1b<", "file:/accumulo/tables/1b/t-000008t/F000004x.rf", "3",
        "../1b/t-000008t/F000004x.rf"));
    expected.add(replaceMut("1b<", "file:/accumulo/tables/1b/t-000008t/F0000054.rf", "4",
        "/t-000008t/F0000054.rf"));

    List<Mutation> results = new ArrayList<>();

    setupMocks(c, fs, map, results);
    Upgrader9to10.replaceRelativePaths(c, fs, tableName, volumeUpgrade);
    verifyPathsReplaced(expected, results);
  }

  @Test
  public void normalizeVolume() throws Exception {
    String uglyVolume = "hdfs://nn.somewhere.com:86753/accumulo/blah/.././/bad/bad2/../.././/////";

    AccumuloClient c = createMock(AccumuloClient.class);
    VolumeManager fs = createMock(VolumeManager.class);
    expect(fs.exists(anyObject())).andReturn(true).anyTimes();
    SortedMap<Key,Value> map = new TreeMap<>();
    map.put(new Key("1b<", "file", "../1b/t-000008t/F000004x.rf"), new Value("1"));
    map.put(new Key("1b<", "file", "/t-000008t/F0000054.rf"), new Value("2"));
    List<Mutation> results = new ArrayList<>();
    List<Mutation> expected = new ArrayList<>();
    expected.add(
        replaceMut("1b<", "hdfs://nn.somewhere.com:86753/accumulo/tables/1b/t-000008t/F000004x.rf",
            "1", "../1b/t-000008t/F000004x.rf"));
    expected.add(
        replaceMut("1b<", "hdfs://nn.somewhere.com:86753/accumulo/tables/1b/t-000008t/F0000054.rf",
            "2", "/t-000008t/F0000054.rf"));

    setupMocks(c, fs, map, results);
    Upgrader9to10.replaceRelativePaths(c, fs, tableName, uglyVolume);
    verifyPathsReplaced(expected, results);
  }

  private Mutation replaceMut(String row, String cq, String val, String delete) {
    Mutation m = new Mutation(row);
    m.at().family("file").qualifier(cq).put(new Value(val));
    m.at().family("file").qualifier(delete).delete();
    return m;
  }

  /**
   * Make sure mutations are all the same, in the correct order
   */
  private void verifyPathsReplaced(List<Mutation> expected, List<Mutation> results) {
    Iterator<Mutation> expectIter = expected.iterator();
    int deleteCount = 0;
    int updateCount = 0;
    for (Mutation mut : results) {
      Mutation next = expectIter.next();
      Iterator<ColumnUpdate> nextUpdates = next.getUpdates().iterator();
      assertEquals(next.getUpdates().size(), mut.getUpdates().size());
      assertEquals(new Text(next.getRow()), new Text(mut.getRow()));

      // check updates are all the same
      for (ColumnUpdate update : mut.getUpdates()) {
        ColumnUpdate nextUpdate = nextUpdates.next();
        Text cq = new Text(nextUpdate.getColumnQualifier());
        log.debug("Checking for expected columnUpdate: " + cq + " deleted? " + update.isDeleted());
        assertEquals(cq, new Text(update.getColumnQualifier()));
        if (update.isDeleted()) {
          deleteCount++;
        } else {
          updateCount++;
          assertEquals(new Text(nextUpdate.getValue()), new Text(update.getValue()));
        }
      }
    }

    assertEquals(deleteCount, updateCount, "Replacements should have update for every delete");
  }

  @Test
  public void testDropSortedMapWALs() throws IOException {
    Configuration hadoopConf = new Configuration();
    ConfigurationCopy conf = new ConfigurationCopy();
    FileSystem fs = new Path("file:///").getFileSystem(hadoopConf);

    List<String> volumes = Arrays.asList("/vol1/", "/vol2/");
    Collection<Volume> vols =
        volumes.stream().map(s -> new VolumeImpl(fs, s)).collect(Collectors.toList());
    Set<String> fullyQualifiedVols = Set.of("file://vol1/", "file://vol2/");
    Set<String> recoveryDirs =
        Set.of("file://vol1/accumulo/recovery", "file://vol2/accumulo/recovery");
    conf.set(Property.INSTANCE_VOLUMES, String.join(",", fullyQualifiedVols));

    ServerContext context = createMock(ServerContext.class);
    Path recoveryDir1 = new Path("file://vol1/accumulo/recovery");
    Path recoveryDir2 = new Path("file://vol2/accumulo/recovery");
    VolumeManager volumeManager = createMock(VolumeManager.class);

    FileStatus[] dirs = new FileStatus[2];
    dirs[0] = createMock(FileStatus.class);
    Path dir0 = new Path("file://vol1/accumulo/recovery/A123456789");
    FileStatus[] dir0Files = new FileStatus[1];
    dir0Files[0] = createMock(FileStatus.class);
    dirs[1] = createMock(FileStatus.class);
    Path dir1 = new Path("file://vol1/accumulo/recovery/B123456789");
    FileStatus[] dir1Files = new FileStatus[1];
    dir1Files[0] = createMock(FileStatus.class);
    Path part1Dir = new Path("file://vol1/accumulo/recovery/B123456789/part-r-0000");

    expect(context.getVolumeManager()).andReturn(volumeManager).once();
    expect(context.getConfiguration()).andReturn(conf).once();
    expect(context.getHadoopConf()).andReturn(hadoopConf).once();
    expect(context.getRecoveryDirs()).andReturn(recoveryDirs).once();
    expect(volumeManager.getVolumes()).andReturn(vols).once();

    expect(volumeManager.exists(recoveryDir1)).andReturn(true).once();
    expect(volumeManager.exists(recoveryDir2)).andReturn(false).once();
    expect(volumeManager.listStatus(recoveryDir1)).andReturn(dirs).once();
    expect(dirs[0].getPath()).andReturn(dir0).once();
    expect(volumeManager.listStatus(dir0)).andReturn(dir0Files).once();
    expect(dir0Files[0].isDirectory()).andReturn(false).once();

    expect(dirs[1].getPath()).andReturn(dir1).once();
    expect(volumeManager.listStatus(dir1)).andReturn(dir1Files).once();
    expect(dir1Files[0].isDirectory()).andReturn(true).once();
    expect(dir1Files[0].getPath()).andReturn(part1Dir).once();
    expect(volumeManager.deleteRecursively(dir1)).andReturn(true).once();

    replay(context, volumeManager, dirs[0], dirs[1], dir0Files[0], dir1Files[0]);
    Upgrader9to10.dropSortedMapWALFiles(context);
  }
}
