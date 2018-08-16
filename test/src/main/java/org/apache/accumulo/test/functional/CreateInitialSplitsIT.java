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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.master.tableOps.Utils;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateInitialSplitsIT extends AccumuloClusterHarness {

  public static final Logger log = LoggerFactory.getLogger(CreateInitialSplitsIT.class);

  private Connector connector;
  private String tableName;
  private SortedSet<Text> splitsInFile;
  private SortedSet<Text> encodedSplitsInFile;
  private FileSystem fs;
  final private String SPLITS = "/tmp/splits";
  final private String ENCODEDSPLITS = "/tmp/encodedSplits";

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration conf) {
    cfg.setMemory(ServerType.TABLET_SERVER, 128 * 4, MemoryUnit.MEGABYTE);

    // use raw local file system
    conf.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 2 * 60;
  }

  @Before
  public void setupInitialSplits() throws IOException {
    connector = getConnector();
    fs = getCluster().getFileSystem();
  }

  @After
  public void cleanUp() throws IOException {
    fs.delete(new Path(SPLITS), true);
    fs.delete(new Path(ENCODEDSPLITS), true);
  }

  /**
   * Verify normal table creation is not broken.
   */
  @Test
  public void testCreateTableWithNoSplits()
      throws TableExistsException, AccumuloSecurityException, AccumuloException {
    tableName = getUniqueNames(1)[0];
    connector.tableOperations().create(tableName);
    assertTrue(connector.tableOperations().exists(tableName));
  }

  /**
   * Create initial splits by providing splits from a Java Collection.
   */
  @Test
  public void testCreateInitialSplitFromList() throws TableExistsException,
      AccumuloSecurityException, AccumuloException, TableNotFoundException {
    tableName = getUniqueNames(1)[0];
    int numSplitsToCreate = 3000;
    SortedSet<Text> expectedSplits = generateSplitList(numSplitsToCreate, 32);
    NewTableConfiguration ntc = new NewTableConfiguration();
    ntc.withSplits(expectedSplits);
    connector.tableOperations().create(tableName, ntc);
    assertTrue(connector.tableOperations().exists(tableName));
    Collection<Text> createdSplits = connector.tableOperations().listSplits(tableName);
    verifySplitsMatch(expectedSplits, new TreeSet<Text>(createdSplits));
  }

  //@Test
  public void nonbinary() throws TableExistsException,
      AccumuloSecurityException, AccumuloException, TableNotFoundException {
    tableName = getUniqueNames(1)[0];
    SortedSet<Text> expectedSplits = new TreeSet<>();
    expectedSplits.add(new Text("aaaaa"));
    expectedSplits.add(new Text("ddddd"));
    expectedSplits.add(new Text("fffff"));

    NewTableConfiguration ntc = new NewTableConfiguration();
    ntc.withSplits(expectedSplits);
    connector.tableOperations().create(tableName, ntc);
    assertTrue(connector.tableOperations().exists(tableName));
    Collection<Text> createdSplits = connector.tableOperations().listSplits(tableName);
    verifySplitsMatch(expectedSplits, new TreeSet<Text>(createdSplits));
  }

  @Test
  public void testCreateInitialBinarySplits()
      throws TableExistsException, AccumuloSecurityException, AccumuloException,
      TableNotFoundException {
    tableName = getUniqueNames(1)[0];
    SortedSet<Text> expectedSplits = new TreeSet<>();
    Random rand = new Random();

    for(int i = 0 ; i < 1000; i++) {
      byte[] split = new byte[16];
      rand.nextBytes(split);
      expectedSplits.add(new Text(split));
    }

//      byte[][] data = {
//          {(byte)0x0c, (byte)0x23, (byte)0x1c, (byte)0x43},
//          {(byte)0x0f, (byte)0x23, (byte)0x5b, (byte)0xb3},
//          {(byte)0x12, (byte)0xa9, (byte)0x44, (byte)0xea},
//          {(byte)0x14, (byte)0x28, (byte)0x36, (byte)0xf9},
//          {(byte)0x75, (byte)0xb1, (byte)0x18, (byte)0x62},
//          {(byte)0x76, (byte)0x1f, (byte)0xab, (byte)0xa5},
//          {(byte)0x7d, (byte)0x69, (byte)0x38, (byte)0x57},
//          {(byte)0x9a, (byte)0x6d, (byte)0x07, (byte)0x7f},
//          {(byte)0xd5, (byte)0x80, (byte)0xe3, (byte)0x2e},
//          {(byte)0xee, (byte)0x25, (byte)0x8d, (byte)0x80}
//      };
//      for (byte[] b : data) {
//        expectedSplits.add(new Text(Base64.getEncoder().encodeToString(b)));
//      }

//    for (Text split : expectedSplits) {
//      log.info(">>>> ========================");
//      ByteBuffer wrap = ByteBuffer.wrap(new Text(split).getBytes(), 0, new Text(split).getLength());
//      Text text = ByteBufferUtil.toText(wrap);
//      log.info("BB TEXT: " + text);
//      byte[] bytes = ByteBufferUtil.toBytes(wrap);
//      log.info("BB BYTE: " + getBytesAsString(bytes, text.getLength()));
//      log.info("encoded: " + Base64.getEncoder().encodeToString(bytes));
//    }

    //TODO create table using splits
    NewTableConfiguration ntc = new NewTableConfiguration();
    ntc.withSplits(expectedSplits);
    connector.tableOperations().create(tableName, ntc);
    assertTrue(connector.tableOperations().exists(tableName));
    Collection<Text> createdSplits = connector.tableOperations().listSplits(tableName);

    //TODO get splits for table and ensure same as splits
//    for (Text created : createdSplits) {
//      log.info(">>>> -----------------------");
//      ByteBuffer wrap = ByteBuffer.wrap(created.getBytes(), 0, created.getLength());
//      Text text = ByteBufferUtil.toText(wrap);
//      log.info("BB TEXT: " + text);
//      byte[] bytes = ByteBufferUtil.toBytes(wrap);
//      log.info("BB BYTE: " + getBytesAsString(bytes, text.getLength()));
//    }
    verifySplitsMatch(expectedSplits, new TreeSet<>(createdSplits));
  }

  @Test
  public void testCreateInitialSplitsCopiedFromAnotherTable() throws TableExistsException,
      AccumuloSecurityException, AccumuloException, TableNotFoundException {
    // create first table with some splits. Do it the old way just for test purposes to verify
    // older way works.
    tableName = getUniqueNames(1)[0];
    NewTableConfiguration ntc = new NewTableConfiguration();
    connector.tableOperations().create(tableName, ntc);
    SortedSet<Text> splits = new TreeSet<>();
    splits.add(new Text("ccccc"));
    splits.add(new Text("mmmmm"));
    splits.add(new Text("ttttt"));
    connector.tableOperations().addSplits(tableName, splits);
    // now create another table using these splits from this table
    Collection<Text> otherSplits = connector.tableOperations().listSplits(tableName);
    String tableName2 = getUniqueNames(2)[1];
    NewTableConfiguration ntc2 = new NewTableConfiguration();
    ntc2.withSplits(new TreeSet<Text>(otherSplits));
    connector.tableOperations().create(tableName2, ntc);
    assertTrue(connector.tableOperations().exists(tableName));
    verifySplitsMatch(splits, new TreeSet<Text>(otherSplits));
  }

  // @Test
  // public void testCreateSplitsViaShellCommands() {
  // // See ShellServerIT for IT tests using shell commands.
  // }

  private void verifySplitsMatch(final SortedSet<Text> expected, final SortedSet<Text> created)
      throws TableNotFoundException {
    verifyTableCreated();
    assertEquals("created splits size does not match expected splits size", expected.size(),
        created.size());
    assertEquals("created splits do not match expected splits", expected, created);
    
  }

  private void verifyTableCreated() {
    SortedSet<String> tableList = connector.tableOperations().list();
    assertTrue(tableList.contains(tableName));
  }

  private SortedSet<Text> generateSplitList(final int numItems, final int len) {
    SortedSet<Text> splits = new TreeSet<>();
    for (int i = 0; i < numItems; i++) {
      splits.add(new Text(getRandomString(len)));
    }
    return splits;
  }

  private String getRandomString(final int len) {
    int desiredLen = len;
    if (len > 32)
      desiredLen = 32;
    return String.valueOf(UUID.randomUUID()).replaceAll("-", "").substring(0, desiredLen - 1);
  }

  private String encode(final Text text, final boolean encode) {
    if (text == null) {
      return null;
    }
    final int length = text.getLength();
    return encode ? Base64.getEncoder().encodeToString(TextUtil.getBytes(text))
        : DefaultFormatter.appendText(new StringBuilder(), text, length).toString();
  }
}
