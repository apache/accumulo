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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collection;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("SpellCheckingInspection")
public class CreateInitialSplitsIT extends AccumuloClusterHarness {

  public static final Logger log = LoggerFactory.getLogger(CreateInitialSplitsIT.class);

  private Connector connector;
  private String tableName;

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
    FileSystem fs = getCluster().getFileSystem();
  }

  /**
   * Verify normal table creation did not get broken.
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
  public void testCreateInitialSplits() throws TableExistsException, AccumuloSecurityException,
      AccumuloException, TableNotFoundException {
    tableName = getUniqueNames(1)[0];
    SortedSet<Text> expectedSplits = generateNonBinarySplits(3000, 32);
    NewTableConfiguration ntc = new NewTableConfiguration().withSplits(expectedSplits);
    assertFalse(connector.tableOperations().exists(tableName));
    connector.tableOperations().create(tableName, ntc);
    assertTrue(connector.tableOperations().exists(tableName));
    Collection<Text> createdSplits = connector.tableOperations().listSplits(tableName);
    assertEquals(expectedSplits, new TreeSet<>(createdSplits));
  }

  /**
   * Test that binary split data is handled property.
   */
  @Test
  public void testCreateInitialBinarySplits() throws TableExistsException,
      AccumuloSecurityException, AccumuloException, TableNotFoundException {
    tableName = getUniqueNames(1)[0];
    SortedSet<Text> expectedSplits = generateBinarySplits(1000, 16);
    NewTableConfiguration ntc = new NewTableConfiguration().withSplits(expectedSplits);
    assertFalse(connector.tableOperations().exists(tableName));
    connector.tableOperations().create(tableName, ntc);
    assertTrue(connector.tableOperations().exists(tableName));
    Collection<Text> createdSplits = connector.tableOperations().listSplits(tableName);
    assertEquals(expectedSplits, new TreeSet<>(createdSplits));
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
    connector.tableOperations().create(tableName, ntc);
    assertTrue(connector.tableOperations().exists(tableName));
    SortedSet<Text> splits = new TreeSet<>();
    splits.add(new Text("ccccc"));
    splits.add(new Text("mmmmm"));
    splits.add(new Text("ttttt"));
    connector.tableOperations().addSplits(tableName, splits);
    // now create another table using the splits from this table
    Collection<Text> otherSplits = connector.tableOperations().listSplits(tableName);
    assertEquals(splits, new TreeSet<>(otherSplits));
    String tableName2 = getUniqueNames(2)[1];
    NewTableConfiguration ntc2 = new NewTableConfiguration();
    ntc2.withSplits(new TreeSet<>(otherSplits));
    assertFalse(connector.tableOperations().exists(tableName2));
    connector.tableOperations().create(tableName2, ntc);
    assertTrue(connector.tableOperations().exists(tableName2));
    Collection<Text> createdSplits = connector.tableOperations().listSplits(tableName);
    assertEquals(splits, new TreeSet<>(createdSplits));
  }

  // @Test
  // public void testCreateSplitsViaShellCommands() {
  // // See ShellServerIT for IT tests using shell commands.
  // }

  private SortedSet<Text> generateNonBinarySplits(final int numItems, final int len) {
    SortedSet<Text> splits = new TreeSet<>();
    for (int i = 0; i < numItems; i++) {
      splits.add(new Text(getRandomString(len)));
    }
    return splits;
  }

  private SortedSet<Text> generateBinarySplits(final int numItems, final int len) {
    SortedSet<Text> splits = new TreeSet<>();
    Random rand = new Random();
    for (int i = 0; i < numItems; i++) {
      byte[] split = new byte[len];
      rand.nextBytes(split);
      splits.add(new Text(split));
    }
    return splits;
  }

  private String getRandomString(final int len) {
    int desiredLen = len;
    if (len > 32)
      desiredLen = 32;
    return String.valueOf(UUID.randomUUID()).replaceAll("-", "").substring(0, desiredLen - 1);
  }
}
