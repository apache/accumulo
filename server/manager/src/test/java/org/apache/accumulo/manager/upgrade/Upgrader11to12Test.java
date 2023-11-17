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
import static org.apache.accumulo.core.metadata.schema.UpgraderDeprecatedConstants.ChoppedColumnFamily;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.newCapture;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
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
  void upgradeDataFileCFTest() throws Exception {
    Upgrader11to12 upgrader = new Upgrader11to12();

    BatchWriter bw = createMock(BatchWriter.class);
    Capture<Mutation> capturedAdd = newCapture();
    bw.addMutation(capture(capturedAdd));
    expectLastCall();

    Capture<Mutation> capturedDelete = newCapture();
    bw.addMutation(capture(capturedDelete));
    expectLastCall();

    replay(bw);

    String fileName = "hdfs://localhost:8020/accumulo/tables/12/default_tablet/A000000v.rf";
    Key k = Key.builder().row(new Text("12;")).family(DataFileColumnFamily.NAME)
        .qualifier(new Text(fileName)).build();
    Value v = new Value("1234,5678");

    upgrader.upgradeDataFileCF(k, v, bw, "aTable");

    StoredTabletFile stf = StoredTabletFile.of(new Path(fileName));
    Mutation add = new Mutation(k.getRow()).at().family(DataFileColumnFamily.NAME)
        .qualifier(stf.getMetadataText()).put(v);
    LOG.debug("add mutation to be expected: {}", add.prettyPrint());

    Mutation delete = new Mutation(k.getRow()).at().family(DataFileColumnFamily.NAME)
        .qualifier(new Text(fileName)).delete();
    LOG.debug("delete mutation to be expected: {}", delete.prettyPrint());

    assertEquals(add, capturedAdd.getValue());
    assertEquals(delete, capturedDelete.getValue());

    verify(bw);
  }

  @Test
  void upgradeDataFileCFSkipConvertedTest() {
    Upgrader11to12 upgrader = new Upgrader11to12();

    BatchWriter bw = createMock(BatchWriter.class);

    replay(bw);

    String fileName = "hdfs://localhost:8020/accumulo/tables/12/default_tablet/A000000v.rf";
    StoredTabletFile stf = StoredTabletFile.of(new Path(fileName));

    Key k = Key.builder().row(new Text("12;")).family(DataFileColumnFamily.NAME)
        .qualifier(stf.getMetadataText()).build();
    Value v = new Value("1234,5678");

    upgrader.upgradeDataFileCF(k, v, bw, "aTable");

    // with file entry in correct formation, no mutations are expected.
    verify(bw);
  }

  @Test
  void upgradeDataFileCFInvalidMutationTest() throws Exception {
    Upgrader11to12 upgrader = new Upgrader11to12();

    BatchWriter bw = createMock(BatchWriter.class);
    Capture<Mutation> capturedAdd = newCapture();
    bw.addMutation(capture(capturedAdd));
    expectLastCall().andThrow(new MutationsRejectedException(null, List.of(), Map.of(), List.of(),
        0, new NullPointerException()));

    replay(bw);

    String fileName = "hdfs://localhost:8020/accumulo/tables/12/default_tablet/A000000v.rf";
    Key k = Key.builder().row(new Text("12;")).family(DataFileColumnFamily.NAME)
        .qualifier(new Text(fileName)).build();
    Value v = new Value("1234,5678");

    assertThrows(IllegalStateException.class, () -> upgrader.upgradeDataFileCF(k, v, bw, "aTable"));

    verify(bw);
  }

  @Test
  void upgradeDataFileCFInvalidPathTest() {
    Upgrader11to12 upgrader = new Upgrader11to12();

    BatchWriter bw = createMock(BatchWriter.class);

    replay(bw);

    String invalidPath = "badPath";

    Key k = Key.builder().row(new Text("12;")).family(DataFileColumnFamily.NAME)
        .qualifier(new Text(invalidPath)).build();
    Value v = new Value("1234,5678");

    assertThrows(IllegalArgumentException.class,
        () -> upgrader.upgradeDataFileCF(k, v, bw, "aTable"));

    verify(bw);
  }

  @Test
  void removeChoppedCFTest() throws Exception {
    Upgrader11to12 upgrader = new Upgrader11to12();

    Key k = Key.builder().row(new Text("12;")).family(ExternalCompactionColumnFamily.NAME)
        .qualifier(ExternalCompactionColumnFamily.NAME).build();

    BatchWriter bw = createMock(BatchWriter.class);
    Capture<Mutation> captured = newCapture();
    bw.addMutation(capture(captured));
    expectLastCall();

    replay(bw);

    upgrader.removeChoppedCF(k, bw, "aTable");

    Mutation delete = new Mutation(k.getRow()).at().family(ChoppedColumnFamily.NAME)
        .qualifier(ChoppedColumnFamily.NAME).delete();

    assertEquals(delete, captured.getValue());

    verify(bw);
  }

  @Test
  void removeChoppedCFContinuesTest() throws Exception {
    Upgrader11to12 upgrader = new Upgrader11to12();

    Key k = Key.builder().row(new Text("12;")).family(ExternalCompactionColumnFamily.NAME)
        .qualifier(ExternalCompactionColumnFamily.NAME).build();

    BatchWriter bw = createMock(BatchWriter.class);
    Capture<Mutation> captured = newCapture();
    bw.addMutation(capture(captured));
    expectLastCall().andThrow(new MutationsRejectedException(null, List.of(), Map.of(), List.of(),
        0, new NullPointerException()));

    replay(bw);

    assertThrows(IllegalStateException.class, () -> upgrader.removeChoppedCF(k, bw, "aTable"));

    verify(bw);
  }

  @Test
  void removeExternalCompactionCFTest() throws Exception {
    Upgrader11to12 upgrader = new Upgrader11to12();

    Key k = Key.builder().row(new Text("12;")).family(ExternalCompactionColumnFamily.NAME)
        .qualifier(new Text("ECID:1234")).build();

    BatchWriter bw = createMock(BatchWriter.class);
    Capture<Mutation> captured = newCapture();
    bw.addMutation(capture(captured));
    expectLastCall();

    replay(bw);

    upgrader.removeExternalCompactionCF(k, bw, "aTable");

    Mutation delete = new Mutation(k.getRow()).at().family(ExternalCompactionColumnFamily.NAME)
        .qualifier(new Text("ECID:1234")).delete();

    assertEquals(delete, captured.getValue());

    for (ColumnUpdate update : captured.getValue().getUpdates()) {
      assertEquals(ExternalCompactionColumnFamily.STR_NAME,
          new String(update.getColumnFamily(), UTF_8));
      assertEquals("ECID:1234", new String(update.getColumnQualifier(), UTF_8));
      assertTrue(update.isDeleted());
    }
    verify(bw);
  }

  @Test
  void removeExternalCompactionCFContinuesTest() throws Exception {
    Upgrader11to12 upgrader = new Upgrader11to12();

    Key k = Key.builder().row(new Text("12;")).family(ExternalCompactionColumnFamily.NAME)
        .qualifier(new Text("ECID:1234")).build();

    BatchWriter bw = createMock(BatchWriter.class);
    Capture<Mutation> captured = newCapture();
    bw.addMutation(capture(captured));
    expectLastCall().andThrow(new MutationsRejectedException(null, List.of(), Map.of(), List.of(),
        0, new NullPointerException()));

    replay(bw);

    assertThrows(IllegalStateException.class,
        () -> upgrader.removeExternalCompactionCF(k, bw, "aTable"));

    verify(bw);
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
