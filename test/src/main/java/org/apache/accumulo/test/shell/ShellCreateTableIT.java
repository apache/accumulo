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
package org.apache.accumulo.test.shell;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.newBufferedReader;
import static java.util.Objects.requireNonNull;
import static org.apache.accumulo.core.util.LazySingletons.RANDOM;
import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MINI_CLUSTER_ONLY)
public class ShellCreateTableIT extends SharedMiniClusterBase {

  private MockShell ts;

  private static class SSCTITCallback implements MiniClusterConfigurationCallback {
    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
      // Only one tserver to avoid race conditions on ZK propagation (auths and configuration)
      cfg.getClusterServerConfiguration().setNumDefaultTabletServers(1);
      // Set the min span to 0 so we will definitely get all the traces back. See ACCUMULO-4365
      Map<String,String> siteConf = cfg.getSiteConfig();
      cfg.setSiteConfig(siteConf);
    }
  }

  @BeforeAll
  public static void setupMiniCluster() throws Exception {
    SharedMiniClusterBase.startMiniClusterWithConfig(new SSCTITCallback());
  }

  @BeforeEach
  public void setupShell() throws Exception {
    ts = new MockShell(getPrincipal(), getRootPassword(),
        getCluster().getConfig().getInstanceName(), getCluster().getConfig().getZooKeepers(),
        getCluster().getConfig().getClientPropsFile());
  }

  @AfterAll
  public static void tearDownAfterAll() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @AfterEach
  public void tearDownShell() {
    ts.shell.shutdown();
  }

  @Test
  public void testCreateTableWithLocalityGroups() throws Exception {
    final String table = getUniqueNames(1)[0];
    ts.exec("createtable " + table + " -l locg1=fam1,fam2", true);
    try (AccumuloClient accumuloClient = Accumulo.newClient().from(getClientProps()).build()) {
      Map<String,Set<Text>> lMap = accumuloClient.tableOperations().getLocalityGroups(table);
      Set<Text> expectedColFams = Set.of(new Text("fam1"), new Text("fam2"));
      for (Map.Entry<String,Set<Text>> entry : lMap.entrySet()) {
        assertEquals("locg1", entry.getKey());
        assertTrue(entry.getValue().containsAll(expectedColFams));
      }
      ts.exec("deletetable -f " + table);
    }
  }

  /**
   * Due to the existing complexity of the createtable command, the createtable help only displays
   * an example of setting one locality group. It is possible to set multiple groups if needed. This
   * test verifies that capability.
   */
  @Test
  public void testCreateTableWithMultipleLocalityGroups() throws Exception {
    final String table = getUniqueNames(1)[0];
    ts.exec("createtable " + table + " -l locg1=fam1,fam2 locg2=colfam1", true);
    try (AccumuloClient accumuloClient = Accumulo.newClient().from(getClientProps()).build()) {
      Map<String,Set<Text>> lMap = accumuloClient.tableOperations().getLocalityGroups(table);
      assertTrue(lMap.containsKey("locg1"));
      assertTrue(lMap.containsKey("locg2"));
      Set<Text> expectedColFams1 = Set.of(new Text("fam1"), new Text("fam2"));
      Set<Text> expectedColFams2 = Set.of(new Text("colfam1"));
      assertTrue(lMap.get("locg1").containsAll(expectedColFams1));
      assertTrue(lMap.get("locg2").containsAll(expectedColFams2));
      ts.exec("deletetable -f " + table);
    }
  }

  @Test
  public void testCreateTableWithLocalityGroupsBadArguments() throws IOException {
    final String table = getUniqueNames(1)[0];
    ts.exec("createtable " + table + " -l locg1 fam1,fam2", false);
    ts.exec("createtable " + table + "-l", false);
    ts.exec("createtable " + table + " -l locg1 = fam1,fam2", false);
    ts.exec("createtable " + table + " -l locg1=fam1 ,fam2", false);
    ts.exec("createtable " + table + " -l locg1=fam1,fam2 locg1=fam3,fam4", false);
    ts.exec("createtable " + table + " -l locg1=fam1,fam2 locg2=fam1", false);
    ts.exec("createtable " + table + " -l locg1", false);
    ts.exec("createtable " + table + " group=fam1", false);
    ts.exec("createtable " + table + "-l fam1,fam2", false);
  }

  @Test
  public void testCreateTableWithIterators() throws Exception {
    final String tmpTable = "tmpTable";
    final String table = getUniqueNames(1)[0];

    // create iterator profile
    // Will use tmpTable for creating profile since setshelliter is requiring a table
    // even though its command line help indicates that it is optional. Likely due to
    // the fact that setshelliter extends setiter, which does require a table argument.
    ts.exec("createtable " + tmpTable, true);
    String output = ts.exec("tables");
    assertTrue(output.contains(tmpTable));

    ts.input.set("\n5000\n\n");
    ts.exec("setshelliter -n itname -p 10 -pn profile1 -ageoff", true);
    output = ts.exec("listshelliter");
    assertTrue(output.contains("Profile : profile1"));

    // create table making use of the iterator profile
    ts.exec("createtable " + table + " -i profile1:scan,minc", true);
    ts.exec("insert foo a b c", true);
    ts.exec("scan", true, "foo a:b []\tc");
    ts.exec("sleep 6", true);
    ts.exec("scan", true, "", true);
    ts.exec("deletetable -f " + table);
    ts.exec("deletetable -f " + tmpTable);
  }

  /**
   * Due to the existing complexity of the createtable command, the createtable help only displays
   * an example of setting one iterator upon table creation. It is possible to set multiple if
   * needed. This test verifies that capability.
   */
  @Test
  public void testCreateTableWithMultipleIterators() throws Exception {
    final String tmpTable = "tmpTable";
    final String table = getUniqueNames(1)[0];

    // create iterator profile
    // Will use tmpTable for creating profile since setshelliter is requiring a table
    // even though its command line help indicates that it is optional. Likely due to
    // the fact that setshelliter extends setiter, which does require a table argument.
    ts.exec("createtable " + tmpTable, true);
    String output = ts.exec("tables");
    assertTrue(output.contains(tmpTable));

    ts.input.set("\n5000\n\n");
    ts.exec("setshelliter -n itname -p 10 -pn profile1 -ageoff", true);
    output = ts.exec("listshelliter");
    assertTrue(output.contains("Profile : profile1"));

    ts.input.set("2\n");
    ts.exec("setshelliter -n iter2 -p 11 -pn profile2 -vers", true);
    output = ts.exec("listshelliter");
    assertTrue(output.contains("Profile : profile2"));

    // create table making use of the iterator profiles
    ts.exec("createtable " + table + " -i profile1:scan,minc profile2:all ", true);
    ts.exec("insert foo a b c", true);
    ts.exec("scan", true, "foo a:b []\tc");
    ts.exec("sleep 6", true);
    ts.exec("scan", true, "", true);
    output = ts.exec("listiter -t " + table + " -all");
    assertTrue(output.contains("Iterator itname, scan scope options"));
    assertTrue(output.contains("Iterator itname, minc scope options"));
    assertFalse(output.contains("Iterator itname, majc scope options"));
    assertTrue(output.contains("Iterator iter2, scan scope options"));
    assertTrue(output.contains("Iterator iter2, minc scope options"));
    assertTrue(output.contains("Iterator iter2, majc scope options"));
    ts.exec("deletetable -f " + table);
    ts.exec("deletetable -f " + tmpTable);
  }

  @Test
  public void testCreateTableWithIteratorsBadArguments() throws IOException {
    final String tmpTable = "tmpTable";
    final String table = getUniqueNames(1)[0];
    ts.exec("createtable " + tmpTable, true);
    String output = ts.exec("tables");
    assertTrue(output.contains(tmpTable));
    ts.input.set("\n5000\n\n");
    ts.exec("setshelliter -n itname -p 10 -pn profile1 -ageoff", true);
    output = ts.exec("listshelliter");
    assertTrue(output.contains("Profile : profile1"));
    // test various bad argument calls
    ts.exec("createtable " + table + " -i noprofile:scan,minc", false);
    ts.exec("createtable " + table + " -i profile1:scan,minc,all,majc", false);
    ts.exec("createtable " + table + " -i profile1:scan,all,majc", false);
    ts.exec("createtable " + table + " -i profile1:scan,min,majc", false);
    ts.exec("createtable " + table + " -i profile1:scan,max,all", false);
    ts.exec("createtable " + table + " -i profile1:", false);
    ts.exec("createtable " + table + " -i profile1: ", false);
    ts.exec("createtable " + table + " -i profile1:-scan", false);
    ts.exec("createtable " + table + " profile1:majc", false);
    ts.exec("createtable " + table + " -i profile1: all", false);
    ts.exec("createtable " + table + " -i profile1: All", false);
    ts.exec("createtable " + table + " -i profile1: scan", false);
    ts.exec("createtable " + table + " -i profile1:minc scan", false);
    ts.exec("createtable " + table + " -i profile1:minc,Scan", false);
    ts.exec("createtable " + table + " -i profile1:minc, scan", false);
    ts.exec("createtable " + table + " -i profile1:minc,,scan", false);
    ts.exec("createtable " + table + " -i profile1:minc,minc", false);
    ts.exec("createtable " + table + " -i profile1:minc,Minc", false);
    ts.exec("createtable " + table + " -i profile1:minc, ,scan", false);
    ts.exec("createtable " + table + "-i", false);
    ts.exec("createtable " + table + "-i ", false);
    ts.exec("deletetable -f " + tmpTable);
  }

  /**
   * Verify that table can be created in offline status and then be brought online.
   */
  @Test
  public void testCreateTableOffline() throws IOException {
    final String tableName = getUniqueNames(1)[0];
    ts.exec("createtable " + tableName + " -o", true);
    String output = ts.exec("tables");
    assertTrue(output.contains(tableName));
    output = ts.exec("scan -t " + tableName, false, "is offline", true);
    assertTrue(output.contains("TableOfflineException"));
    ts.exec("table " + tableName, true);
    ts.exec("online", true);
    ts.exec("scan", true);
    ts.exec("deletetable -f " + tableName, true);
  }

  /**
   * Use shell to create a table with a supplied file containing splits.
   * <p>
   * The splits will be contained in a file, sorted and un-encoded with no repeats or blank lines.
   */
  @Test
  public void testCreateTableWithSplitsFile1()
      throws IOException, AccumuloSecurityException, TableNotFoundException, AccumuloException {
    String splitsFile = System.getProperty("user.dir") + "/target/splitFile";
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      generateSplitsFile(splitsFile, 1000, 12, false, false, true, false, false);
      SortedSet<Text> expectedSplits = readSplitsFromFile(splitsFile);
      final String tableName = getUniqueNames(1)[0];
      ts.exec("createtable " + tableName + " -sf " + splitsFile, true);
      Collection<Text> createdSplits = client.tableOperations().listSplits(tableName);
      assertEquals(expectedSplits, new TreeSet<>(createdSplits));
    } finally {
      Files.delete(Paths.get(splitsFile));
    }
  }

  /**
   * Use shell to create a table with a supplied file containing splits.
   * <p>
   * The splits will be contained in a file, unsorted and un-encoded with no repeats or blank lines.
   */
  @Test
  public void testCreateTableWithSplitsFile2()
      throws IOException, AccumuloSecurityException, TableNotFoundException, AccumuloException {
    String splitsFile = System.getProperty("user.dir") + "/target/splitFile";
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      generateSplitsFile(splitsFile, 300, 12, false, false, false, false, false);
      SortedSet<Text> expectedSplits = readSplitsFromFile(splitsFile);
      final String tableName = getUniqueNames(1)[0];
      ts.exec("createtable " + tableName + " -sf " + splitsFile, true);
      Collection<Text> createdSplits = client.tableOperations().listSplits(tableName);
      assertEquals(expectedSplits, new TreeSet<>(createdSplits));
    } finally {
      Files.delete(Paths.get(splitsFile));
    }
  }

  /**
   * Use shell to create a table with a supplied file containing splits.
   * <p>
   * The splits will be contained in a file, sorted and encoded with no repeats or blank lines.
   */
  @Test
  public void testCreateTableWithSplitsFile3()
      throws IOException, AccumuloSecurityException, TableNotFoundException, AccumuloException {
    String splitsFile = System.getProperty("user.dir") + "/target/splitFile";
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      generateSplitsFile(splitsFile, 100, 23, false, true, true, false, false);
      SortedSet<Text> expectedSplits = readSplitsFromFile(splitsFile);
      final String tableName = getUniqueNames(1)[0];
      ts.exec("createtable " + tableName + " -sf " + splitsFile, true);
      Collection<Text> createdSplits = client.tableOperations().listSplits(tableName);
      assertEquals(expectedSplits, new TreeSet<>(createdSplits));
    } finally {
      Files.delete(Paths.get(splitsFile));
    }
  }

  /**
   * Use shell to create a table with a supplied file containing splits.
   * <p>
   * The splits will be contained in a file, sorted and un-encoded with a blank line and no repeats.
   */
  @Test
  public void testCreateTableWithSplitsFile4()
      throws IOException, AccumuloSecurityException, TableNotFoundException, AccumuloException {
    String splitsFile = System.getProperty("user.dir") + "/target/splitFile";
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      generateSplitsFile(splitsFile, 100, 31, false, false, true, true, false);
      SortedSet<Text> expectedSplits = readSplitsFromFile(splitsFile);
      final String tableName = getUniqueNames(1)[0];
      ts.exec("createtable " + tableName + " -sf " + splitsFile, true);
      Collection<Text> createdSplits = client.tableOperations().listSplits(tableName);
      assertEquals(expectedSplits, new TreeSet<>(createdSplits));
    } finally {
      Files.delete(Paths.get(splitsFile));
    }
  }

  /**
   * Use shell to create a table with a supplied file containing splits.
   * <p>
   * The splits will be contained in a file, sorted and un-encoded with a blank line and no repeats.
   */
  @Test
  public void testCreateTableWithSplitsFile5()
      throws IOException, AccumuloSecurityException, TableNotFoundException, AccumuloException {
    String splitsFile = System.getProperty("user.dir") + "/target/splitFile";
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      generateSplitsFile(splitsFile, 100, 32, false, false, true, false, true);
      SortedSet<Text> expectedSplits = readSplitsFromFile(splitsFile);
      final String tableName = getUniqueNames(1)[0];
      ts.exec("createtable " + tableName + " -sf " + splitsFile, true);
      Collection<Text> createdSplits = client.tableOperations().listSplits(tableName);
      assertEquals(expectedSplits, new TreeSet<>(createdSplits));
    } finally {
      Files.delete(Paths.get(splitsFile));
    }
  }

  /**
   * Use shell to create a table with a supplied file containing splits.
   * <p>
   * The splits will be contained in a file, unsorted and un-encoded with a blank line and repeats.
   */
  @Test
  public void testCreateTableWithSplitsFile6()
      throws IOException, AccumuloSecurityException, TableNotFoundException, AccumuloException {
    String splitsFile = System.getProperty("user.dir") + "/target/splitFile";
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      generateSplitsFile(splitsFile, 100, 12, false, false, false, true, true);
      SortedSet<Text> expectedSplits = readSplitsFromFile(splitsFile);
      final String tableName = getUniqueNames(1)[0];
      ts.exec("createtable " + tableName + " -sf " + splitsFile, true);
      Collection<Text> createdSplits = client.tableOperations().listSplits(tableName);
      assertEquals(expectedSplits, new TreeSet<>(createdSplits));
    } finally {
      Files.delete(Paths.get(splitsFile));
    }
  }

  /**
   * Use shell to create a table with a supplied file containing splits.
   * <p>
   * The splits will be contained in a file, sorted and encoded with a blank line and repeats.
   */
  @Test
  public void testCreateTableWithSplitsFile7()
      throws IOException, AccumuloSecurityException, TableNotFoundException, AccumuloException {
    String splitsFile = System.getProperty("user.dir") + "/target/splitFile";
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      generateSplitsFile(splitsFile, 100, 12, false, false, true, true, true);
      SortedSet<Text> expectedSplits = readSplitsFromFile(splitsFile);
      final String tableName = getUniqueNames(1)[0];
      ts.exec("createtable " + tableName + " -sf " + splitsFile, true);
      Collection<Text> createdSplits = client.tableOperations().listSplits(tableName);
      assertEquals(expectedSplits, new TreeSet<>(createdSplits));
    } finally {
      Files.delete(Paths.get(splitsFile));
    }
  }

  /**
   * Use shell to create a table with a supplied file containing splits.
   * <p>
   * The splits file will be empty.
   */
  @Test
  public void testCreateTableWithEmptySplitFile() throws IOException {
    String splitsFile = System.getProperty("user.dir") + "/target/splitFile";
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      generateSplitsFile(splitsFile, 0, 0, false, false, false, false, false);
      final String tableName = getUniqueNames(1)[0];
      ts.exec("createtable " + tableName + " -sf " + splitsFile, false);
      assertThrows(TableNotFoundException.class,
          () -> client.tableOperations().listSplits(tableName));
    } finally {
      Files.delete(Paths.get(splitsFile));
    }
  }

  /**
   * Use shell to create a table that used splits from another table.
   */
  @Test
  public void testCreateTableWithCopySplitsFromOtherTable()
      throws IOException, AccumuloSecurityException, TableNotFoundException, AccumuloException {
    // create a table and add some splits
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      final String[] tableNames = getUniqueNames(2);
      final String tableName0 = tableNames[0];
      final String tableName2 = tableNames[1];

      ts.exec("createtable " + tableName0, true);
      String output = ts.exec("tables", true);
      assertTrue(output.contains(tableName0));
      ts.exec("table " + tableName0, true);
      // add splits to this table using the addsplits command.
      List<Text> splits = new ArrayList<>();
      splits.add(new Text("ccccc"));
      splits.add(new Text("fffff"));
      splits.add(new Text("mmmmm"));
      splits.add(new Text("sssss"));
      ts.exec("addsplits " + splits.get(0) + " " + splits.get(1) + " " + splits.get(2) + " "
          + splits.get(3), true);
      // Now create a table that will used the previous tables splits and create them at table
      // creation
      ts.exec("createtable " + tableName2 + " --copy-splits " + tableName0, true);
      ts.exec("table " + tableName0, true);
      String tablesOutput = ts.exec("tables", true);
      assertTrue(tablesOutput.contains(tableName2));
      Collection<Text> createdSplits = client.tableOperations().listSplits(tableName2);
      assertEquals(new TreeSet<>(splits), new TreeSet<>(createdSplits));
      ts.exec("deletetable -f " + tableName0, true);
      ts.exec("deletetable -f " + tableName2, true);
    }
  }

  /**
   * Use shell to create a table with a supplied file containing splits.
   * <p>
   * The splits will be contained in a file, sorted and encoded with no repeats or blank lines.
   */
  @Test
  public void testCreateTableWithBinarySplitsFile1()
      throws IOException, AccumuloSecurityException, TableNotFoundException, AccumuloException {
    String splitsFile = System.getProperty("user.dir") + "/target/splitFile";
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      generateSplitsFile(splitsFile, 200, 12, true, true, true, false, false);
      SortedSet<Text> expectedSplits = readSplitsFromFile(splitsFile);
      final String tableName = getUniqueNames(1)[0];
      ts.exec("createtable " + tableName + " -sf " + splitsFile, true);
      Collection<Text> createdSplits = client.tableOperations().listSplits(tableName);
      assertEquals(expectedSplits, new TreeSet<>(createdSplits));
    } finally {
      Files.delete(Paths.get(splitsFile));
    }
  }

  /**
   * Use shell to create a table with a supplied file containing splits.
   * <p>
   * The splits will be contained in a file, unsorted and encoded with no repeats or blank lines.
   */
  @Test
  public void testCreateTableWithBinarySplitsFile2()
      throws IOException, AccumuloSecurityException, TableNotFoundException, AccumuloException {
    String splitsFile = System.getProperty("user.dir") + "/target/splitFile";
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      generateSplitsFile(splitsFile, 300, 12, true, true, false, false, false);
      SortedSet<Text> expectedSplits = readSplitsFromFile(splitsFile);
      final String tableName = getUniqueNames(1)[0];
      ts.exec("createtable " + tableName + " -sf " + splitsFile, true);
      Collection<Text> createdSplits = client.tableOperations().listSplits(tableName);
      assertEquals(expectedSplits, new TreeSet<>(createdSplits));
    } finally {
      Files.delete(Paths.get(splitsFile));
    }
  }

  /**
   * Use shell to create a table with a supplied file containing splits.
   * <p>
   * The splits will be contained in a file, sorted and encoded with no repeats or blank lines.
   */
  @Test
  public void testCreateTableWithBinarySplitsFile3()
      throws IOException, AccumuloSecurityException, TableNotFoundException, AccumuloException {
    String splitsFile = System.getProperty("user.dir") + "/target/splitFile";
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      generateSplitsFile(splitsFile, 100, 23, true, true, true, false, false);
      SortedSet<Text> expectedSplits = readSplitsFromFile(splitsFile);
      final String tableName = getUniqueNames(1)[0];
      ts.exec("createtable " + tableName + " -sf " + splitsFile, true);
      Collection<Text> createdSplits = client.tableOperations().listSplits(tableName);
      assertEquals(expectedSplits, new TreeSet<>(createdSplits));
    } finally {
      Files.delete(Paths.get(splitsFile));
    }
  }

  /**
   * Use shell to create a table with a supplied file containing splits.
   * <p>
   * The splits will be contained in a file, sorted and encoded with a blank line and no repeats.
   */
  @Test
  public void testCreateTableWithBinarySplitsFile4()
      throws IOException, AccumuloSecurityException, TableNotFoundException, AccumuloException {
    String splitsFile = System.getProperty("user.dir") + "/target/splitFile";
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      generateSplitsFile(splitsFile, 100, 31, true, true, true, true, false);
      SortedSet<Text> expectedSplits = readSplitsFromFile(splitsFile);
      final String tableName = getUniqueNames(1)[0];
      ts.exec("createtable " + tableName + " -sf " + splitsFile, true);
      Collection<Text> createdSplits = client.tableOperations().listSplits(tableName);
      assertEquals(expectedSplits, new TreeSet<>(createdSplits));
    } finally {
      Files.delete(Paths.get(splitsFile));
    }
  }

  /**
   * Use shell to create a table with a supplied file containing splits.
   * <p>
   * The splits will be contained in a file, sorted and encoded with a blank line and no repeats.
   */
  @Test
  public void testCreateTableWithBinarySplitsFile5()
      throws IOException, AccumuloSecurityException, TableNotFoundException, AccumuloException {
    String splitsFile = System.getProperty("user.dir") + "/target/splitFile";
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      generateSplitsFile(splitsFile, 100, 32, true, true, true, false, true);
      SortedSet<Text> expectedSplits = readSplitsFromFile(splitsFile);
      final String tableName = getUniqueNames(1)[0];
      ts.exec("createtable " + tableName + " -sf " + splitsFile, true);
      Collection<Text> createdSplits = client.tableOperations().listSplits(tableName);
      assertEquals(expectedSplits, new TreeSet<>(createdSplits));
    } finally {
      Files.delete(Paths.get(splitsFile));
    }
  }

  /**
   * Use shell to create a table with a supplied file containing splits.
   * <p>
   * The splits will be contained in a file, unsorted and encoded with a blank line and repeats.
   */
  @Test
  public void testCreateTableWithBinarySplitsFile6()
      throws IOException, AccumuloSecurityException, TableNotFoundException, AccumuloException {
    String splitsFile = System.getProperty("user.dir") + "/target/splitFile";
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      generateSplitsFile(splitsFile, 100, 12, true, true, false, true, true);
      SortedSet<Text> expectedSplits = readSplitsFromFile(splitsFile);
      final String tableName = getUniqueNames(1)[0];
      ts.exec("createtable " + tableName + " -sf " + splitsFile, true);
      Collection<Text> createdSplits = client.tableOperations().listSplits(tableName);
      assertEquals(expectedSplits, new TreeSet<>(createdSplits));
    } finally {
      Files.delete(Paths.get(splitsFile));
    }
  }

  /**
   * Use shell to create a table with a supplied file containing splits.
   * <p>
   * The splits will be contained in a file, sorted and encoded with a blank line and repeats.
   */
  @Test
  public void testCreateTableWithBinarySplitsFile7()
      throws IOException, AccumuloSecurityException, TableNotFoundException, AccumuloException {
    String splitsFile = System.getProperty("user.dir") + "/target/splitFile";
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      generateSplitsFile(splitsFile, 100, 12, true, true, true, true, true);
      SortedSet<Text> expectedSplits = readSplitsFromFile(splitsFile);
      final String tableName = getUniqueNames(1)[0];
      ts.exec("createtable " + tableName + " -sf " + splitsFile, true);
      Collection<Text> createdSplits = client.tableOperations().listSplits(tableName);
      assertEquals(expectedSplits, new TreeSet<>(createdSplits));
    } finally {
      Files.delete(Paths.get(splitsFile));
    }
  }

  // Verify that createtable handles initial TabletAvailability parameters.
  // Argument should handle upper/lower/mixed case as value.
  // If splits are supplied, each created tablet should contain the ~tab:availability value in
  // the
  // metadata table.
  @Test
  public void testCreateTableWithInitialTabletAvailability() throws Exception {
    final String[] tables = getUniqueNames(5);

    // createtable with no tablet availability argument supplied
    String createCmd = "createtable " + tables[0];
    verifyTableWithTabletAvailability(createCmd, tables[0], "ONDEMAND", 1);

    // createtable with '-a' argument supplied
    createCmd = "createtable " + tables[1] + " -a hosted";
    verifyTableWithTabletAvailability(createCmd, tables[1], "HOSTED", 1);

    // using --availability
    createCmd = "createtable " + tables[2] + " --availability unHosted";
    verifyTableWithTabletAvailability(createCmd, tables[2], "UNHOSTED", 1);

    String splitsFile = System.getProperty("user.dir") + "/target/splitsFile";
    Path splitFilePath = Paths.get(splitsFile);
    try {
      generateSplitsFile(splitsFile, 10, 12, false, false, true, false, false);
      createCmd = "createtable " + tables[3] + " -a Hosted -sf " + splitsFile;
      verifyTableWithTabletAvailability(createCmd, tables[3], "HOSTED", 11);
    } finally {
      Files.delete(splitFilePath);
    }

    try {
      generateSplitsFile(splitsFile, 5, 5, true, true, true, false, false);
      createCmd = "createtable " + tables[4] + " -a unhosted -sf " + splitsFile;
      verifyTableWithTabletAvailability(createCmd, tables[4], "UNHOSTED", 6);
    } finally {
      Files.delete(splitFilePath);
    }
  }

  private void verifyTableWithTabletAvailability(String cmd, String tableName,
      String tabletAvailability, int expectedTabletCnt) throws Exception {
    ts.exec(cmd);
    String tableId = getTableId(tableName);
    String result = ts.exec(
        "scan -t accumulo.metadata -b " + tableId + " -e " + tableId + "< -c ~tab:availability");
    // the ~tab:availability entry should be created at table creation
    assertTrue(result.contains("~tab:availability"));
    // There should be a corresponding tablet availability value for each expected tablet
    assertEquals(expectedTabletCnt, StringUtils.countMatches(result, tabletAvailability));
  }

  private String getTableId(String tableName) throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      Map<String,String> idMap = client.tableOperations().tableIdMap();
      return idMap.get(tableName);
    }
  }

  private SortedSet<Text> readSplitsFromFile(final String splitsFile) throws IOException {
    SortedSet<Text> splits = new TreeSet<>();
    try (BufferedReader reader = newBufferedReader(Paths.get(splitsFile))) {
      String split;
      while ((split = reader.readLine()) != null) {
        Text unencodedString = decode(split);
        if (unencodedString != null) {
          splits.add(unencodedString);
        }
      }
    }
    return splits;
  }

  private void generateSplitsFile(final String splitsFile, final int numItems, final int len,
      final boolean binarySplits, final boolean encoded, final boolean sort,
      final boolean addBlankLine, final boolean repeat) throws IOException {

    java.nio.file.Path splitsPath = java.nio.file.Paths.get(splitsFile);
    int insertAt = (len % 2 == 0) ? len / 2 : (len + 1) / 2;
    Collection<Text> sortedSplits = null;
    Collection<Text> randomSplits;

    if (binarySplits) {
      randomSplits = generateBinarySplits(numItems, len);
    } else {
      randomSplits = generateNonBinarySplits(numItems, len);
    }

    if (sort) {
      sortedSplits = new TreeSet<>(randomSplits);
    }

    try (BufferedWriter writer = Files.newBufferedWriter(splitsPath, UTF_8)) {
      int cnt = 0;
      Collection<Text> splits;
      if (sort) {
        splits = sortedSplits;
      } else {
        splits = randomSplits;
      }

      for (Text text : splits) {
        if (addBlankLine && cnt++ == insertAt) {
          writer.write('\n');
        }
        writer.write(encode(text, encoded) + '\n');
        if (repeat) {
          writer.write(encode(text, encoded) + '\n');
        }
      }
    }
  }

  /**
   * This test confirms the behaviour that when a table is created with the copy-configuration
   * option that the properties that get set on the new table are the effective properties - that is
   * the table properties include the system and namespace are copied into the table properties.
   */
  @Test
  public void copyConfigOptionsTest() throws Exception {
    String[] names = getUniqueNames(2);
    String srcNS = "ns1"; // + names[0];

    String srcTable = srcNS + ".src_table_" + names[1];
    String destTable = srcNS + ".dest_table_" + names[1];

    // define constants
    final String sysPropName = "table.custom.my_sys_prop";
    final String sysPropValue1 = "sys_value1";
    final String sysPropValue2 = "sys_value2";
    final String nsPropName = "table.custom.my_ns_prop";
    final String nsPropValue1 = "ns_value1";
    final String nsPropValue2 = "ns_value2";

    ts.exec("config -s " + sysPropName + "=" + sysPropValue1);

    ts.exec("createnamespace " + srcNS);
    ts.exec("config -s " + nsPropName + "=" + nsPropValue1 + " -ns " + srcNS);

    ts.exec("createtable " + srcTable);
    ts.exec("createtable -cc " + srcTable + " " + destTable);

    try (AccumuloClient accumuloClient = Accumulo.newClient().from(getClientProps()).build()) {
      Map<String,String> tids = accumuloClient.tableOperations().tableIdMap();

      // used to grab values directly from ZooKeeper to bypass hierarchy
      PropStore propStore = getCluster().getServerContext().getPropStore();

      TableId destId = TableId.of(accumuloClient.tableOperations().tableIdMap().get(destTable));

      // the Zk node should have all effective properties copied from configuration
      var vp1 = propStore.get(TablePropKey.of(getCluster().getServerContext(), destId));
      assertEquals(sysPropValue1, vp1.asMap().get(sysPropName));
      assertEquals(nsPropValue1, vp1.asMap().get(nsPropName));

      // check getTableProperties also inherits the effective config
      Map<String,String> tableEffective =
          accumuloClient.tableOperations().getTableProperties(destTable);
      assertEquals(sysPropValue1, tableEffective.get(sysPropName));
      assertEquals(nsPropValue1, tableEffective.get(nsPropName));

      // changing the system and namespace props should leave the copied effective props unchanged
      ts.exec("config -s " + sysPropName + "=" + sysPropValue2);
      ts.exec("config -s " + nsPropName + "=" + nsPropValue2 + " -ns " + srcNS);

      // source will still inherit from sys and namespace (no prop values)
      var vp2 = propStore
          .get(TablePropKey.of(getCluster().getServerContext(), TableId.of(tids.get(srcTable))));
      assertNull(vp2.asMap().get(sysPropName));
      assertNull(vp2.asMap().get(nsPropName));

      // dest (copied props) should remain local to the table, overriding sys and namespace
      var vp3 = propStore
          .get(TablePropKey.of(getCluster().getServerContext(), TableId.of(tids.get(destTable))));
      assertEquals(sysPropValue1, vp3.asMap().get(sysPropName));
      assertEquals(nsPropValue1, vp3.asMap().get(nsPropName));

      // show change propagated in source table effective hierarchy
      tableEffective = accumuloClient.tableOperations().getConfiguration(srcTable);

      assertEquals(sysPropValue2, tableEffective.get(sysPropName));
      assertEquals(nsPropValue2, tableEffective.get(nsPropName));

      // because effective config was copied, the change should not propagate to effective hierarchy
      tableEffective = accumuloClient.tableOperations().getConfiguration(destTable);
      assertEquals(sysPropValue1, tableEffective.get(sysPropName));
      assertEquals(nsPropValue1, tableEffective.get(nsPropName));
    }
  }

  @Test
  public void copyTablePropsOnlyOptionsTest() throws Exception {
    String[] names = getUniqueNames(2);
    String srcNS = "src_ns_" + names[0];

    String srcTable = srcNS + ".src_table_" + names[1];
    String destTable = srcNS + ".dest_table_" + names[1];

    // define constants
    final String sysPropName = "table.custom.my_sys_prop";
    final String sysPropValue1 = "sys_value1";
    final String sysPropValue2 = "sys_value2";
    final String nsPropName = "table.custom.my_ns_prop";
    final String nsPropValue1 = "ns_value1";
    final String nsPropValue2 = "ns_value2";

    ts.exec("config -s " + sysPropName + "=" + sysPropValue1);

    ts.exec("createnamespace " + srcNS);
    ts.exec("config -s " + nsPropName + "=" + nsPropValue1 + " -ns " + srcNS);

    ts.exec("createtable " + srcTable);
    ts.exec("createtable --exclude-parent-properties --copy-config " + srcTable + " " + destTable,
        true);

    try (AccumuloClient accumuloClient = Accumulo.newClient().from(getClientProps()).build()) {
      Map<String,String> tids = accumuloClient.tableOperations().tableIdMap();

      // only table unique values should be stored in Zk node for the table.
      var vp1 = getCluster().getServerContext().getPropStore()
          .get(TablePropKey.of(getCluster().getServerContext(), TableId.of(tids.get(destTable))));
      assertNull(vp1.asMap().get(sysPropName));
      assertNull(vp1.asMap().get(nsPropName));

      // check props were inherited in effective props
      Map<String,String> tableEffective =
          accumuloClient.tableOperations().getConfiguration(destTable);
      assertEquals(sysPropValue1, tableEffective.get(sysPropName));
      assertEquals(nsPropValue1, tableEffective.get(nsPropName));

      // changing the system and namespace props should leave the effective props copied unchanged
      ts.exec("config -s " + sysPropName + "=" + sysPropValue2);
      ts.exec("config -s " + nsPropName + "=" + nsPropValue2 + " -ns " + srcNS);

      // source will still inherit from sys and namespace (no prop values)
      var vp2 = getCluster().getServerContext().getPropStore()
          .get(TablePropKey.of(getCluster().getServerContext(), TableId.of(tids.get(srcTable))));
      assertNull(vp2.asMap().get(sysPropName));
      assertNull(vp2.asMap().get(nsPropName));

      // dest (copied props) should remain local to the table, overriding sys and namespace
      var vp3 = getCluster().getServerContext().getPropStore()
          .get(TablePropKey.of(getCluster().getServerContext(), TableId.of(tids.get(destTable))));
      assertNull(vp3.asMap().get(sysPropName));
      assertNull(vp3.asMap().get(nsPropName));

      // because effective config was not copied, the changes should propagate to effective
      // hierarchy
      tableEffective = accumuloClient.tableOperations().getConfiguration(destTable);
      assertEquals(sysPropValue2, tableEffective.get(sysPropName));
      assertEquals(nsPropValue2, tableEffective.get(nsPropName));
    }
  }

  @Test
  public void copyTablePropsInvalidOptsTest() throws Exception {
    String[] names = getUniqueNames(2);

    ts.exec("createtable " + names[0]);
    ts.exec("createtable " + names[1]);

    // test --expect-parent requires-cc expect this fail
    ts.exec("createtable --exclude-parent " + names[0] + "dest", false);
  }

  @Test
  public void missingSrcCopyPropsTest() throws Exception {
    String[] names = getUniqueNames(2);
    // test command fail if src is not available
    ts.exec("createtable --exclude-parent -cc " + names[0] + " " + names[1], false);
  }

  @Test
  public void missingSrcCopyConfigTest() throws Exception {
    String[] names = getUniqueNames(2);
    /// test command fail if src is not available
    ts.exec("createtable -cc " + names[0] + " " + names[1], false);
  }

  @Test
  public void destExistsTest() throws Exception {
    String[] names = getUniqueNames(2);

    ts.exec("createtable " + names[0]);
    ts.exec("createtable " + names[1]);

    // expect to fail because target already exists
    ts.exec("createtable -cc " + names[0] + " " + names[1], false);
  }

  @Test
  public void optionOrderingTest() throws Exception {
    String[] names = getUniqueNames(3);

    ts.exec("createtable " + names[0]);

    ts.exec("createtable --exclude-parent --copy-config " + names[0] + " " + names[1], true);
    ts.exec("createtable --copy-config " + names[0] + " --exclude-parent " + names[2], true);
  }

  private Collection<Text> generateNonBinarySplits(final int numItems, final int len) {
    Set<Text> splits = new HashSet<>();
    for (int i = 0; i < numItems; i++) {
      splits.add(getRandomText(len));
    }
    return splits;
  }

  private Collection<Text> generateBinarySplits(final int numItems, final int len) {
    Set<Text> splits = new HashSet<>();
    for (int i = 0; i < numItems; i++) {
      byte[] split = new byte[len];
      RANDOM.get().nextBytes(split);
      splits.add(new Text(split));
    }
    return splits;
  }

  private Text getRandomText(final int len) {
    int desiredLen = Math.min(len, 32);
    return new Text(
        String.valueOf(UUID.randomUUID()).replaceAll("-", "").substring(0, desiredLen - 1));
  }

  private static String encode(final Text text, final boolean encode) {
    if (text.toString().isBlank()) {
      return null;
    }
    return encode ? Base64.getEncoder().encodeToString(TextUtil.getBytes(text)) : text.toString();
  }

  private Text decode(final String text) {
    if (requireNonNull(text).isBlank()) {
      return null;
    }
    return new Text(text);
  }
}
