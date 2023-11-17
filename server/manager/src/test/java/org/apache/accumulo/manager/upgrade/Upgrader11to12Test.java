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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ExternalCompactionColumnFamily;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LastLocationColumnFamily;
import static org.apache.accumulo.core.metadata.schema.UpgraderDeprecatedConstants.ChoppedColumnFamily;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.newCapture;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.data.Stat;
import org.easymock.Capture;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Upgrader11to12Test {

  private static final Logger LOG = LoggerFactory.getLogger(Upgrader11to12Test.class);

  @Test
  void upgradeDataFileCF2Test() {
    Upgrader11to12 upgrader = new Upgrader11to12();

    String fileName = "hdfs://localhost:8020/accumulo/tables/12/default_tablet/A000000v.rf";
    Key k = Key.builder().row(new Text("12;")).family(DataFileColumnFamily.NAME)
        .qualifier(new Text(fileName)).build();
    Value v = new Value("1234,5678");

    Mutation upgrade = new Mutation(k.getRow());
    upgrader.upgradeDataFileCF(k, v, upgrade);

    var pending = upgrade.getUpdates();

    // leverage sort order for "expected" values
    // check file entry converted is in the mutation
    Iterator<ColumnUpdate> m = pending.iterator();
    var cu1 = m.next();
    assertEquals("file", new Text(cu1.getColumnFamily()).toString());

    StoredTabletFile oldFileEntry = StoredTabletFile.of(new Path(fileName));
    StoredTabletFile updateEnry = StoredTabletFile.of(new String(cu1.getColumnQualifier(), UTF_8));

    assertEquals(oldFileEntry, updateEnry);
    assertFalse(cu1.isDeleted());

    // check old file entry is deleted is in the mutation

    var cu2 = m.next();
    assertEquals("file", new Text(cu1.getColumnFamily()).toString());
    assertEquals(fileName, new String(cu2.getColumnQualifier(), UTF_8));
    assertTrue(cu2.isDeleted());

  }

  @Test
  public void processReferencesTest() throws Exception {
    BatchWriter batchWriter = mock(BatchWriter.class);
    Capture<Mutation> capturedUpdate1 = newCapture();
    batchWriter.addMutation(capture(capturedUpdate1));
    expectLastCall().once();

    Capture<Mutation> capturedUpdate2 = newCapture();
    batchWriter.addMutation(capture(capturedUpdate2));
    expectLastCall().once();

    // create sample data "served" by the mocked scanner
    TreeMap<Key,Value> scanData = new TreeMap<>();
    Text row1 = new Text("123");

    String fileName1 = "hdfs://localhost:8020/accumulo/tables/12/default_tablet/A000000v.rf";
    Key key1 =
        Key.builder(false).row(row1).family(DataFileColumnFamily.NAME).qualifier(fileName1).build();
    Value value1 = new Value("123,456");
    scanData.put(key1, value1);

    String fileName2 = "hdfs://localhost:8020/accumulo/tables/12/default_tablet/B000000v.rf";
    Key key2 =
        Key.builder(false).row(row1).family(DataFileColumnFamily.NAME).qualifier(fileName2).build();
    Value value2 = new Value("321,654");
    scanData.put(key2, value2);

    Key chop1 = Key.builder(false).row(row1).family(ChoppedColumnFamily.NAME)
        .qualifier(ChoppedColumnFamily.NAME).build();
    scanData.put(chop1, null);

    Key extern1 = Key.builder(false).row(row1).family(ExternalCompactionColumnFamily.NAME)
        .qualifier(ExternalCompactionColumnFamily.NAME).build();
    scanData.put(extern1, null);

    Text row2 = new Text("234");

    String fileName3 = "hdfs://localhost:8020/accumulo/tables/13/default_tablet/C000000v.rf";
    Key key3 =
        Key.builder(false).row(row2).family(DataFileColumnFamily.NAME).qualifier(fileName3).build();
    Value value3 = new Value("1,2");
    scanData.put(key3, value3);

    Scanner scanner = mock(Scanner.class);
    scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
    expectLastCall();
    scanner.fetchColumnFamily(ChoppedColumnFamily.NAME);
    expectLastCall();
    scanner.fetchColumnFamily(ExternalCompactionColumnFamily.NAME);
    expectLastCall();

    expect(scanner.iterator()).andReturn(scanData.entrySet().iterator()).once();
    replay(batchWriter, scanner);

    Upgrader11to12 upgrader = new Upgrader11to12();
    upgrader.processReferences(batchWriter, scanner, "accumulo.metadata");

    LOG.info("c:{}", capturedUpdate1.getValue().prettyPrint());
    var u1 = capturedUpdate1.getValue();
    // 2 file add, 2 file delete. 1 chop delete, 1 ext comp delete
    assertEquals(6, u1.getUpdates().size());

    LOG.info("c:{}", capturedUpdate2.getValue().prettyPrint());
    var u2 = capturedUpdate2.getValue();
    // 1 add, 1 delete
    assertEquals(2, u2.getUpdates().size());

    verify(batchWriter, scanner);

  }

  @Test
  public void skipConvertedFileTest() throws Exception {
    BatchWriter batchWriter = mock(BatchWriter.class);
    Capture<Mutation> capturedUpdate1 = newCapture();
    batchWriter.addMutation(capture(capturedUpdate1));
    expectLastCall().once();
    // create sample data "served" by the mocked scanner
    TreeMap<Key,Value> scanData = new TreeMap<>();
    Text row1 = new Text("123");

    // reference already in expected form with fence info.
    String fileName1 =
        "{\"path\":\"hdfs://localhost:8020/accumulo/tables/12/default_tablet/A000000v.rf\",\"startRow\":\"\",\"endRow\":\"\"}";
    Key key1 =
        Key.builder(false).row(row1).family(DataFileColumnFamily.NAME).qualifier(fileName1).build();
    Value value1 = new Value("123,456");
    scanData.put(key1, value1);

    String fileName2 = "hdfs://localhost:8020/accumulo/tables/12/default_tablet/B000000v.rf";
    Key key2 =
        Key.builder(false).row(row1).family(DataFileColumnFamily.NAME).qualifier(fileName2).build();
    Value value2 = new Value("321,654");
    scanData.put(key2, value2);

    Scanner scanner = mock(Scanner.class);
    scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
    expectLastCall();
    scanner.fetchColumnFamily(ChoppedColumnFamily.NAME);
    expectLastCall();
    scanner.fetchColumnFamily(ExternalCompactionColumnFamily.NAME);
    expectLastCall();

    expect(scanner.iterator()).andReturn(scanData.entrySet().iterator()).once();
    replay(batchWriter, scanner);

    Upgrader11to12 upgrader = new Upgrader11to12();
    upgrader.processReferences(batchWriter, scanner, "accumulo.metadata");

    LOG.info("c:{}", capturedUpdate1.getValue().prettyPrint());
    var u1 = capturedUpdate1.getValue();
    // 1 add, 1 delete
    assertEquals(2, u1.getUpdates().size());

    verify(batchWriter, scanner);
  }

  @Test
  void failOnMutationErrorTest() throws Exception {

    BatchWriter batchWriter = mock(BatchWriter.class);
    Capture<Mutation> capturedUpdate1 = newCapture();
    batchWriter.addMutation(capture(capturedUpdate1));
    expectLastCall().andThrow(new MutationsRejectedException(null, List.of(), Map.of(), List.of(),
        0, new NullPointerException())).once();

    TreeMap<Key,Value> scanData = new TreeMap<>();
    Text row1 = new Text("123");

    // reference already in expected form with fence info.
    String fileName1 = "hdfs://localhost:8020/accumulo/tables/12/default_tablet/A000000v.rf";
    Key key1 =
        Key.builder(false).row(row1).family(DataFileColumnFamily.NAME).qualifier(fileName1).build();
    Value value1 = new Value("123,456");
    scanData.put(key1, value1);

    Scanner scanner = mock(Scanner.class);
    scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
    expectLastCall();
    scanner.fetchColumnFamily(ChoppedColumnFamily.NAME);
    expectLastCall();
    scanner.fetchColumnFamily(ExternalCompactionColumnFamily.NAME);
    expectLastCall();

    expect(scanner.iterator()).andReturn(scanData.entrySet().iterator()).once();
    replay(batchWriter, scanner);
    Upgrader11to12 upgrader = new Upgrader11to12();

    assertThrows(IllegalStateException.class,
        () -> upgrader.processReferences(batchWriter, scanner, "accumulo.metadata"));

    verify(batchWriter, scanner);
  }

  @Test
  void upgradeDataFileCFInvalidPathTest() throws Exception {

    BatchWriter batchWriter = mock(BatchWriter.class);
    Capture<Mutation> capturedUpdate1 = newCapture();
    batchWriter.addMutation(capture(capturedUpdate1));
    // expecting that exception will be called before mutation is updated.
    expectLastCall().andThrow(new UnsupportedOperationException()).anyTimes();

    // create sample data "served" by the mocked scanner
    TreeMap<Key,Value> scanData = new TreeMap<>();
    Text row1 = new Text("123");

    String fileName1 = "bad path";
    Key key1 =
        Key.builder(false).row(row1).family(DataFileColumnFamily.NAME).qualifier(fileName1).build();
    Value value1 = new Value("123,456");
    scanData.put(key1, value1);

    String fileName2 = "hdfs://localhost:8020/accumulo/tables/12/default_tablet/B000000v.rf";
    Key key2 =
        Key.builder(false).row(row1).family(DataFileColumnFamily.NAME).qualifier(fileName2).build();
    Value value2 = new Value("321,654");
    scanData.put(key2, value2);

    Scanner scanner = mock(Scanner.class);
    scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
    expectLastCall();
    scanner.fetchColumnFamily(ChoppedColumnFamily.NAME);
    expectLastCall();
    scanner.fetchColumnFamily(ExternalCompactionColumnFamily.NAME);
    expectLastCall();

    expect(scanner.iterator()).andReturn(scanData.entrySet().iterator()).once();
    replay(batchWriter, scanner);

    Upgrader11to12 upgrader = new Upgrader11to12();
    assertThrows(IllegalArgumentException.class,
        () -> upgrader.processReferences(batchWriter, scanner, "accumulo.metadata"));

    verify(batchWriter, scanner);
  }

  @Test
  void unexpectedColFailsTest() throws Exception {

    BatchWriter batchWriter = mock(BatchWriter.class);
    Capture<Mutation> capturedUpdate1 = newCapture();
    batchWriter.addMutation(capture(capturedUpdate1));
    // expecting that exception will be called before mutation is updated.
    expectLastCall().andThrow(new UnsupportedOperationException()).anyTimes();

    // create sample data "served" by the mocked scanner
    TreeMap<Key,Value> scanData = new TreeMap<>();
    Text row1 = new Text("123");

    Key key1 = Key.builder(false).row(row1).family(LastLocationColumnFamily.NAME).qualifier("srv1")
        .build();
    Value value1 = new Value("123,456");
    scanData.put(key1, value1);

    Scanner scanner = mock(Scanner.class);
    scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
    expectLastCall();
    scanner.fetchColumnFamily(ChoppedColumnFamily.NAME);
    expectLastCall();
    scanner.fetchColumnFamily(ExternalCompactionColumnFamily.NAME);
    expectLastCall();

    expect(scanner.iterator()).andReturn(scanData.entrySet().iterator()).once();
    replay(batchWriter, scanner);

    Upgrader11to12 upgrader = new Upgrader11to12();
    assertThrows(IllegalStateException.class,
        () -> upgrader.processReferences(batchWriter, scanner, "accumulo.metadata"));

    verify(batchWriter, scanner);
  }

  /**
   * process 3 rows, 2 should result in no mutations and batch writer addMutation should not be
   * called for those rows
   */
  @Test
  public void verifyEmptyMutation() throws Exception {
    BatchWriter batchWriter = mock(BatchWriter.class);
    Capture<Mutation> capturedUpdate1 = newCapture();
    batchWriter.addMutation(capture(capturedUpdate1));
    expectLastCall().once();
    // create sample data "served" by the mocked scanner
    TreeMap<Key,Value> scanData = new TreeMap<>();

    Text row1 = new Text("1");

    String fileName1 = "hdfs://localhost:8020/accumulo/tables/12/default_tablet/1111000v.rf";
    Key key1 =
        Key.builder(false).row(row1).family(DataFileColumnFamily.NAME).qualifier(fileName1).build();
    Value value1 = new Value("111,222");
    scanData.put(key1, value1);

    Text row2 = new Text("a");

    // reference already in expected form with fence info.
    String fileName2 =
        "{\"path\":\"hdfs://localhost:8020/accumulo/tables/12/default_tablet/A000000v.rf\",\"startRow\":\"\",\"endRow\":\"\"}";
    Key key2 =
        Key.builder(false).row(row2).family(DataFileColumnFamily.NAME).qualifier(fileName2).build();
    Value value2 = new Value("222,333");
    scanData.put(key2, value2);

    Text row3 = new Text("b");

    // reference already in expected form with fence info.
    String fileName3 =
        "{\"path\":\"hdfs://localhost:8020/accumulo/tables/12/default_tablet/BBBB000v.rf\",\"startRow\":\"\",\"endRow\":\"\"}";
    Key key3 =
        Key.builder(false).row(row3).family(DataFileColumnFamily.NAME).qualifier(fileName3).build();
    Value value3 = new Value("333,444");
    scanData.put(key3, value3);

    Scanner scanner = mock(Scanner.class);
    scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
    expectLastCall();
    scanner.fetchColumnFamily(ChoppedColumnFamily.NAME);
    expectLastCall();
    scanner.fetchColumnFamily(ExternalCompactionColumnFamily.NAME);
    expectLastCall();

    expect(scanner.iterator()).andReturn(scanData.entrySet().iterator()).once();
    replay(batchWriter, scanner);

    Upgrader11to12 upgrader = new Upgrader11to12();
    upgrader.processReferences(batchWriter, scanner, "accumulo.metadata");

    LOG.info("c:{}", capturedUpdate1.getValue().prettyPrint());
    var u1 = capturedUpdate1.getValue();
    // 1 add, 1 delete
    assertEquals(2, u1.getUpdates().size());

    verify(batchWriter, scanner);
  }

  @Test
  public void upgradeZooKeeperTest() throws Exception {

    // taken from an uno instance.
    final byte[] zKRootV1 =
        "{\"version\":1,\"columnValues\":{\"file\":{\"hdfs://localhost:8020/accumulo/tables/+r/root_tablet/A0000030.rf\":\"856,15\",\"hdfs://localhost:8020/accumulo/tables/+r/root_tablet/F000000r.rf\":\"308,2\"},\"last\":{\"100017f46240004\":\"localhost:9997\"},\"loc\":{\"100017f46240004\":\"localhost:9997\"},\"srv\":{\"dir\":\"root_tablet\",\"flush\":\"16\",\"lock\":\"tservers/localhost:9997/zlock#f6a582b9-9583-4553-b179-a7a3852c8332#0000000000$100017f46240004\",\"time\":\"L42\"},\"~tab\":{\"~pr\":\"\\u0000\"}}}\n"
            .getBytes(UTF_8);
    final String zKRootV2 =
        "{\"version\":2,\"columnValues\":{\"file\":{\"{\\\"path\\\":\\\"hdfs://localhost:8020/accumulo/tables/+r/root_tablet/A0000030.rf\\\",\\\"startRow\\\":\\\"\\\",\\\"endRow\\\":\\\"\\\"}\":\"856,15\",\"{\\\"path\\\":\\\"hdfs://localhost:8020/accumulo/tables/+r/root_tablet/F000000r.rf\\\",\\\"startRow\\\":\\\"\\\",\\\"endRow\\\":\\\"\\\"}\":\"308,2\"},\"last\":{\"100017f46240004\":\"localhost:9997\"},\"loc\":{\"100017f46240004\":\"localhost:9997\"},\"srv\":{\"dir\":\"root_tablet\",\"flush\":\"16\",\"lock\":\"tservers/localhost:9997/zlock#f6a582b9-9583-4553-b179-a7a3852c8332#0000000000$100017f46240004\",\"time\":\"L42\"},\"~tab\":{\"~pr\":\"\\u0000\"}}}";

    InstanceId iid = InstanceId.of(UUID.randomUUID());
    Upgrader11to12 upgrader = new Upgrader11to12();

    ServerContext context = createMock(ServerContext.class);
    ZooReaderWriter zrw = createMock(ZooReaderWriter.class);

    expect(context.getInstanceID()).andReturn(iid).anyTimes();
    expect(context.getZooReaderWriter()).andReturn(zrw).anyTimes();

    Capture<Stat> statCapture = newCapture();
    expect(zrw.getData(eq("/accumulo/" + iid.canonical() + "/root_tablet"), capture(statCapture)))
        .andAnswer(() -> {
          Stat stat = statCapture.getValue();
          stat.setCtime(System.currentTimeMillis());
          stat.setMtime(System.currentTimeMillis());
          stat.setVersion(123); // default version
          stat.setDataLength(zKRootV1.length);
          statCapture.setValue(stat);
          return zKRootV1;
        }).once();

    Capture<byte[]> byteCapture = newCapture();
    expect(zrw.overwritePersistentData(eq("/accumulo/" + iid.canonical() + "/root_tablet"),
        capture(byteCapture), eq(123))).andReturn(true).once();

    replay(context, zrw);

    upgrader.upgradeZookeeper(context);

    assertEquals(zKRootV2, new String(byteCapture.getValue(), UTF_8));

    verify(context, zrw);
  }

  @Test
  public void fileConversionTest() {
    String s21 = "hdfs://localhost:8020/accumulo/tables/1/t-0000000/A000003v.rf";
    String s31 =
        "{\"path\":\"hdfs://localhost:8020/accumulo/tables/1/t-0000000/A000003v.rf\",\"startRow\":\"\",\"endRow\":\"\"}";
    String s31_untrimmed =
        "   {  \"path\":\"hdfs://localhost:8020/accumulo/tables/1/t-0000000/A000003v.rf\",\"startRow\":\"\",\"endRow\":\"\"  }   ";

    Upgrader11to12 upgrader = new Upgrader11to12();

    assertTrue(upgrader.fileNeedsConversion(s21));
    assertFalse(upgrader.fileNeedsConversion(s31));
    assertFalse(upgrader.fileNeedsConversion(s31_untrimmed));
  }
}
