/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.master.upgrade;

import static org.apache.accumulo.core.Constants.BULK_PREFIX;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.gc.GcVolumeUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Upgrader9to10Test {
  private static Logger log = LoggerFactory.getLogger(Upgrader9to10Test.class);

  @Test
  public void testSwitchRelative() {
    assertEquals(GcVolumeUtil.getDeleteTabletOnAllVolumesUri(TableId.of("5a"), "t-0005"),
        Upgrader9to10.switchToAllVolumes("/5a/t-0005"));
    assertEquals("/5a/" + BULK_PREFIX + "0005",
        Upgrader9to10.switchToAllVolumes("/5a/" + BULK_PREFIX + "0005"));
    assertEquals("/5a/t-0005/F0009.rf", Upgrader9to10.switchToAllVolumes("/5a/t-0005/F0009.rf"));
  }

  @Test(expected = IllegalStateException.class)
  public void testBadRelativeTooShort() {
    Upgrader9to10.switchToAllVolumes("/5a");
  }

  @Test(expected = IllegalStateException.class)
  public void testBadRelativeTooLong() {
    Upgrader9to10.switchToAllVolumes("/5a/5a/t-0005/F0009.rf");
  }

  @Test
  public void testSwitch() {
    assertEquals(GcVolumeUtil.getDeleteTabletOnAllVolumesUri(TableId.of("5a"), "t-0005"),
        Upgrader9to10.switchToAllVolumes("hdfs://localhost:9000/accumulo/tables/5a/t-0005"));
    assertEquals("hdfs://localhost:9000/accumulo/tables/5a/" + BULK_PREFIX + "0005", Upgrader9to10
        .switchToAllVolumes("hdfs://localhost:9000/accumulo/tables/5a/" + BULK_PREFIX + "0005"));
    assertEquals("hdfs://localhost:9000/accumulo/tables/5a/t-0005/C0009.rf", Upgrader9to10
        .switchToAllVolumes("hdfs://localhost:9000/accumulo/tables/5a/t-0005/C0009.rf"));
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
    scanner.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);
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
    assertEquals("Invalid Relative path check",
        Upgrader9to10.checkForRelativePaths(c, fs, tableName, volumeUpgrade), false);
    assertTrue(results.isEmpty());
  }

  @Test
  public void filesDontExistAfterReplacingRelatives() throws Exception {
    AccumuloClient c = createMock(AccumuloClient.class);
    VolumeManager fs = createMock(VolumeManager.class);
    SortedMap<Key,Value> map = new TreeMap<>();
    map.put(new Key("1b;row_0000000050", "file", "../1b/default_tablet/A000001c.rf"),
        new Value("1"));
    map.put(new Key("1b;row_0000000050", "file", "../1b/default_tablet/F000001m.rf"),
        new Value("2"));

    expect(fs.exists(anyObject(Path.class))).andReturn(false).anyTimes();

    setupMocks(c, fs, map, new ArrayList<>());
    try {
      Upgrader9to10.checkForRelativePaths(c, fs, tableName, volumeUpgrade);
      fail("Expected IllegalArgumentException to be thrown");
    } catch (IllegalArgumentException e) {}
  }

  @Test
  public void missingUpgradeRelativeProperty() throws Exception {
    AccumuloClient c = createMock(AccumuloClient.class);
    VolumeManager fs = createMock(VolumeManager.class);
    SortedMap<Key,Value> map = new TreeMap<>();
    map.put(new Key("1b;row_0000000050", "file", "../1b/default_tablet/A000001c.rf"),
        new Value("1"));
    map.put(new Key("1b;row_0000000050", "file", "../1b/default_tablet/F000001m.rf"),
        new Value("2"));

    expect(fs.exists(anyObject(Path.class))).andReturn(false).anyTimes();

    setupMocks(c, fs, map, new ArrayList<>());
    try {
      Upgrader9to10.checkForRelativePaths(c, fs, tableName, "");
      fail("Expected IllegalArgumentException to be thrown");
    } catch (IllegalArgumentException e) {}
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

    assertEquals("Replacements should have update for every delete", deleteCount, updateCount);
  }
}
