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
import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;
import static org.apache.accumulo.harness.AccumuloITBase.SUNNY_DAY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
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
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MINI_CLUSTER_ONLY)
@Tag(SUNNY_DAY)
public class ShellCreateTableIT extends SharedMiniClusterBase {

  private MockShell ts;

  private static class SSCTITCallback implements MiniClusterConfigurationCallback {
    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
      // Only one tserver to avoid race conditions on ZK propagation (auths and configuration)
      cfg.setNumTservers(1);
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
   *
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
   *
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
   *
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
   *
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
   *
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
   *
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
   *
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
   *
   * The splits file will be empty.
   */
  @Test
  public void testCreateTableWithEmptySplitFile()
      throws IOException, AccumuloSecurityException, TableNotFoundException, AccumuloException {
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
   *
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
   *
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
   *
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
   *
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
   *
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
   *
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
   *
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
      random.nextBytes(split);
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
