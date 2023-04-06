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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Base64;
import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CreateInitialSplitsIT extends AccumuloClusterHarness {

  private AccumuloClient client;
  private String tableName;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration conf) {
    cfg.setMemory(ServerType.TABLET_SERVER, 512, MemoryUnit.MEGABYTE);

    // use raw local file system
    conf.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  @BeforeEach
  public void setupInitialSplits() {
    client = Accumulo.newClient().from(getClientProps()).build();
  }

  @AfterEach
  public void closeClient() {
    client.close();
  }

  /**
   * Verify normal table creation did not get broken.
   */
  @Test
  public void testCreateTableWithNoSplits()
      throws TableExistsException, AccumuloSecurityException, AccumuloException {
    tableName = getUniqueNames(1)[0];
    client.tableOperations().create(tableName);
    assertTrue(client.tableOperations().exists(tableName));
  }

  private void runTest(SortedSet<Text> expectedSplits) throws AccumuloSecurityException,
      TableNotFoundException, AccumuloException, TableExistsException {
    NewTableConfiguration ntc = new NewTableConfiguration().withSplits(expectedSplits);
    assertFalse(client.tableOperations().exists(tableName));
    client.tableOperations().create(tableName, ntc);
    assertTrue(client.tableOperations().exists(tableName));
    Collection<Text> createdSplits = client.tableOperations().listSplits(tableName);
    assertEquals(expectedSplits, new TreeSet<>(createdSplits));
  }

  /**
   * Create initial splits by providing splits from a Java Collection.
   */
  @Test
  public void testCreateInitialSplits() throws TableExistsException, AccumuloSecurityException,
      AccumuloException, TableNotFoundException {
    tableName = getUniqueNames(1)[0];
    runTest(generateNonBinarySplits(3000, 32));
  }

  @Test
  public void testCreateInitialSplitsWithEncodedSplits() throws TableExistsException,
      AccumuloSecurityException, AccumuloException, TableNotFoundException {
    tableName = getUniqueNames(1)[0];
    runTest(generateNonBinarySplits(3000, 32, true));
  }

  /**
   * Test that binary split data is handled property.
   */
  @Test
  public void testCreateInitialBinarySplits() throws TableExistsException,
      AccumuloSecurityException, AccumuloException, TableNotFoundException {
    tableName = getUniqueNames(1)[0];
    runTest(generateBinarySplits(1000, 16));
  }

  @Test
  public void testCreateInitialBinarySplitsWithEncodedSplits() throws TableExistsException,
      AccumuloSecurityException, AccumuloException, TableNotFoundException {
    tableName = getUniqueNames(1)[0];
    runTest(generateBinarySplits(1000, 16, true));
  }

  /**
   * Create splits based upon splits from another table.
   */
  @Test
  public void testCreateInitialSplitsCopiedFromAnotherTable() throws TableExistsException,
      AccumuloSecurityException, AccumuloException, TableNotFoundException {
    // create first table with some splits. Do it the older way just for test purposes to verify
    // older method was not affected.
    tableName = getUniqueNames(1)[0];
    NewTableConfiguration ntc = new NewTableConfiguration();
    client.tableOperations().create(tableName, ntc);
    assertTrue(client.tableOperations().exists(tableName));
    SortedSet<Text> splits = new TreeSet<>();
    splits.add(new Text("ccccc"));
    splits.add(new Text("mmmmm"));
    splits.add(new Text("ttttt"));
    client.tableOperations().addSplits(tableName, splits);
    // now create another table using the splits from this table
    Collection<Text> otherSplits = client.tableOperations().listSplits(tableName);
    assertEquals(splits, new TreeSet<>(otherSplits));
    String tableName2 = getUniqueNames(2)[1];
    NewTableConfiguration ntc2 = new NewTableConfiguration();
    ntc2.withSplits(new TreeSet<>(otherSplits));
    assertFalse(client.tableOperations().exists(tableName2));
    client.tableOperations().create(tableName2, ntc);
    assertTrue(client.tableOperations().exists(tableName2));
    Collection<Text> createdSplits = client.tableOperations().listSplits(tableName);
    assertEquals(splits, new TreeSet<>(createdSplits));
  }

  /**
   *
   * Write some data to multiple tablets Verify data Compact table Verify data Delete table.
   */
  @Test
  public void testMultipleOperationsFunctionality() throws TableExistsException,
      AccumuloSecurityException, AccumuloException, TableNotFoundException {
    // Write data to mulitple tablets
    tableName = getUniqueNames(1)[0];
    SortedSet<Text> expectedSplits = generateNonBinarySplits(1000, 32);
    runTest(expectedSplits);
    client.tableOperations().flush(tableName);
    // compact data
    client.tableOperations().compact(tableName, null, null, true, true);
    // verify data
    Collection<Text> createdSplits = client.tableOperations().listSplits(tableName);
    assertEquals(expectedSplits, new TreeSet<>(createdSplits));
    // delete table
    client.tableOperations().delete(tableName);
    assertFalse(client.tableOperations().exists(tableName));
  }

  // @Test
  // public void testCreateSplitsViaShellCommands() {
  // // See ShellServerIT for IT tests using shell commands.
  // }

  private SortedSet<Text> generateNonBinarySplits(final int numItems, final int len) {
    return generateNonBinarySplits(numItems, len, false);
  }

  private SortedSet<Text> generateNonBinarySplits(final int numItems, final int len,
      final boolean useB64) {
    SortedSet<Text> splits = new TreeSet<>();
    for (int i = 0; i < numItems; i++) {
      splits.add(encode(getRandomText(len), useB64));
    }
    return splits;
  }

  private SortedSet<Text> generateBinarySplits(final int numItems, final int len) {
    return generateBinarySplits(numItems, len, false);
  }

  private SortedSet<Text> generateBinarySplits(final int numItems, final int len,
      final boolean useB64) {
    SortedSet<Text> splits = new TreeSet<>();
    for (int i = 0; i < numItems; i++) {
      byte[] split = new byte[len];
      random.nextBytes(split);
      splits.add(encode(new Text(split), useB64));
    }
    return splits;
  }

  private Text encode(final Text text, final boolean encode) {
    if (text == null) {
      return null;
    }
    return encode ? new Text(Base64.getEncoder().encodeToString(TextUtil.getBytes(text))) : text;
  }

  private Text getRandomText(final int len) {
    int desiredLen = len;
    if (len > 32) {
      desiredLen = 32;
    }
    return new Text(
        String.valueOf(UUID.randomUUID()).replaceAll("-", "").substring(0, desiredLen - 1));
  }
}
