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
import java.util.Base64;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
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
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
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
    int numSplitsToCreate = 5000;
    SortedSet<Text> expectedSplits = generateSplitList(numSplitsToCreate);
    NewTableConfiguration ntc = new NewTableConfiguration();
    ntc.withSplits(expectedSplits);
    connector.tableOperations().create(tableName, ntc);
    assertTrue(connector.tableOperations().exists(tableName));
    Collection<Text> createdSplits = connector.tableOperations().listSplits(tableName);
    verifySplitsMatch(expectedSplits, new TreeSet<Text>(createdSplits));

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
    splits.add(new Text("abcde"));
    splits.add(new Text("bcdef"));
    splits.add(new Text("cdefg"));
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
    Iterator<Text> expectedIterator = expected.iterator();
    Iterator<Text> createdIterator = created.iterator();
    Text currentExpected = new Text("");
    Text currentCreated = new Text("");
    while (expectedIterator.hasNext() || createdIterator.hasNext()) {
      Text nextExpected = expectedIterator.next();
      Text nextCreated = createdIterator.next();
      assertEquals("expected split value '" + nextExpected + "' does not equal "
          + "created split value '" + nextCreated + "'", nextExpected, nextCreated);
      assertTrue("expected splits are not ordered properly",
          currentExpected.toString().compareTo(nextExpected.toString()) < 0);
      assertTrue("created splits are not ordered properly",
          currentCreated.toString().compareTo(nextCreated.toString()) < 0);
      currentExpected = nextExpected;
      currentCreated = nextCreated;
    }
    verifyMetadata(expected);
  }

  /**
   * Verify that the metadata table contains the expected splits.
   */
  private void verifyMetadata(SortedSet<Text> expected) throws TableNotFoundException {
    Map<String,String> tableIdMap = connector.tableOperations().tableIdMap();
    String id = tableIdMap.get(tableName);
    Scanner scan = connector.createScanner("accumulo.metadata", Authorizations.EMPTY);
    scan.fetchColumn(new Text("srv"), new Text("time"));
    for (Map.Entry<Key,Value> entry : scan) {
      Text row = entry.getKey().getRow();
      if (!row.toString().startsWith(id + ";"))
        continue;
      Text partialRow = new Text(row.toString().substring(2));
      assertTrue("partialRow not in splits array", expected.contains(partialRow));
    }
  }

  private void verifyTableCreated() {
    SortedSet<String> tableList = connector.tableOperations().list();
    assertTrue(tableList.contains(tableName));
  }

  private SortedSet<Text> generateSplitList(final int numItems) {
    SortedSet<Text> splits = new TreeSet<>();
    for (int i = 0; i < numItems; i++) {
      splits.add(new Text(getRandomString(32)));
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
