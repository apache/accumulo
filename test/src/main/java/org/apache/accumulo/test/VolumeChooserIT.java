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
package org.apache.accumulo.test;

import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.fs.DelegatingChooser;
import org.apache.accumulo.core.spi.fs.PreferredVolumeChooser;
import org.apache.accumulo.core.spi.fs.RandomVolumeChooser;
import org.apache.accumulo.core.spi.fs.VolumeChooserEnvironment.Scope;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.log.WalStateManager;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class VolumeChooserIT extends ConfigurableMacBase {

  private static final String TP = Property.TABLE_ARBITRARY_PROP_PREFIX.getKey();
  private static final String PREFERRED_CHOOSER_PROP = TP + "volume.preferred";
  public static final String PERTABLE_CHOOSER_PROP = TP + "volume.chooser";

  private static final String GP = Property.GENERAL_ARBITRARY_PROP_PREFIX.getKey();

  private static final String getPreferredProp(Scope scope) {
    return GP + "volume.preferred." + scope.name().toLowerCase();
  }

  private static final String getPerTableProp(Scope scope) {
    return GP + "volume.chooser." + scope.name().toLowerCase();
  }

  private static final Text EMPTY = new Text();
  private static final Value EMPTY_VALUE = new Value();
  private java.nio.file.Path volDirBase;
  private Path v1;
  private Path v2;
  private Path v3_disallowed;
  private Path v4;
  private static final TreeSet<Text> alpha_rows =
      Stream.of("a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z".split(",")).map(Text::new)
          .collect(Collectors.toCollection(TreeSet::new));

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    // Get 2 tablet servers
    cfg.getClusterServerConfiguration().setNumDefaultTabletServers(2);

    // Set the general volume chooser to the DelegatingChooser so that different choosers can be
    // specified
    Map<String,String> siteConfig = new HashMap<>();
    siteConfig.put(Property.GENERAL_VOLUME_CHOOSER.getKey(), DelegatingChooser.class.getName());

    // Set up 4 different volume paths
    java.nio.file.Path baseDir = cfg.getDir().toPath();
    volDirBase = baseDir.resolve("volumes");
    java.nio.file.Path v1f = volDirBase.resolve("v1");
    java.nio.file.Path v2f = volDirBase.resolve("v2");
    java.nio.file.Path v3f = volDirBase.resolve("v3");
    java.nio.file.Path v4f = volDirBase.resolve("v4");
    v1 = new Path("file://" + v1f.toAbsolutePath());
    v2 = new Path("file://" + v2f.toAbsolutePath());
    v3_disallowed = new Path("file://" + v3f.toAbsolutePath());
    v4 = new Path("file://" + v4f.toAbsolutePath());

    // delegate to the preferred volume v2 for logs
    siteConfig.put(getPerTableProp(Scope.LOGGER), PreferredVolumeChooser.class.getName());
    siteConfig.put(getPreferredProp(Scope.LOGGER), v2.toString());
    // delegate to the preferred volumes v1 and v2 for init
    siteConfig.put(getPerTableProp(Scope.INIT), PreferredVolumeChooser.class.getName());
    siteConfig.put(getPreferredProp(Scope.INIT), v1 + "," + v2);
    // delegate to the preferred volumes v4
    siteConfig.put(getPerTableProp(Scope.DEFAULT), PreferredVolumeChooser.class.getName());
    siteConfig.put(getPreferredProp(Scope.DEFAULT), v4.toString());

    cfg.setSiteConfig(siteConfig);

    // Only add volumes 1, 2, and 4 to the list of instance volumes to have one volume that isn't in
    // the options list when they are choosing
    cfg.setProperty(Property.INSTANCE_VOLUMES, v1 + "," + v2 + "," + v4);

    // use raw local file system so walogs sync and flush will work
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());

    super.configure(cfg, hadoopCoreSite);

  }

  static TableId createAndVerifyTable(AccumuloClient client, String tableName,
      SortedSet<Text> splits, boolean flush) throws Exception {
    // create the table
    var ntc = new NewTableConfiguration().withSplits(splits);
    client.tableOperations().create(tableName, ntc);

    // write some data
    try (BatchWriter bw = client.createBatchWriter(tableName)) {
      for (Text row : alpha_rows) {
        Mutation m = new Mutation(row);
        m.put(EMPTY, EMPTY, EMPTY_VALUE);
        bw.addMutation(m);
      }
    }

    // optionally, flush before verification
    if (flush) {
      client.tableOperations().flush(tableName, null, null, true);
    }

    // verify it can be read back
    try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
      var row_iter = alpha_rows.iterator();
      for (Entry<Key,Value> entry : scanner) {
        assertEquals(row_iter.next(), entry.getKey().getRow(), "Data read is not data written");
      }
    }
    return TableId.of(client.tableOperations().tableIdMap().get(tableName));
  }

  private void createNamespaceWithPreferredChooser(AccumuloClient client, String namespace,
      Path preferredVolume) throws Exception {
    client.namespaceOperations().create(namespace);
    client.namespaceOperations().setProperty(namespace, PERTABLE_CHOOSER_PROP,
        PreferredVolumeChooser.class.getName());
    if (preferredVolume != null) {
      client.namespaceOperations().setProperty(namespace, PREFERRED_CHOOSER_PROP,
          preferredVolume.toString());
    }
  }

  private void createNamespaceWithRandomChooser(AccumuloClient client, String namespace)
      throws Exception {
    client.namespaceOperations().create(namespace);
    client.namespaceOperations().setProperty(namespace, PERTABLE_CHOOSER_PROP,
        RandomVolumeChooser.class.getName());
    // The random volume chooser should ignore this property
    client.namespaceOperations().setProperty(namespace, PREFERRED_CHOOSER_PROP, "ignored");
  }

  private void createTableAndVerifyVolumesUsed(AccumuloClient client, String namespace,
      Path... expectedVolumes) throws Exception {
    String tableName = namespace + ".1";
    TableId tableID = createAndVerifyTable(client, tableName, alpha_rows, true);

    // Verify the new files are written only to the expected volumes
    Set<String> allTableFiles = getServerContext().getAmple().readTablets().forTable(tableID)
        .fetch(ColumnType.FILES).build().stream().flatMap(tm -> tm.getFiles().stream())
        .map(StoredTabletFile::getPath).map(Path::toString).collect(toSet());
    assertEquals(alpha_rows.size(), allTableFiles.size(), "Wrong number of files");
    Set<Path> expectedVolumesSeen = new TreeSet<>();
    allTableFiles.forEach(file -> {
      boolean foundMatchingExpectedVolume = false;
      for (Path volume : expectedVolumes) {
        if (file.startsWith(volume.toString())) {
          expectedVolumesSeen.add(volume);
          foundMatchingExpectedVolume = true;
        }
      }
      assertTrue(foundMatchingExpectedVolume, "Data not written to the correct volumes: " + file);
    });
    assertEquals(expectedVolumes.length, expectedVolumesSeen.size(),
        "Did not see all the expected volumes. volumes: " + Set.of(expectedVolumes)
            + " volumes seen: " + expectedVolumesSeen);
  }

  // Test that creates two tables, with different preferred volumes
  @Test
  public void twoTablesPreferredVolumeChooser() throws Exception {
    String namespace1 = "ns_" + getUniqueNames(2)[0];
    String namespace2 = "ns_" + getUniqueNames(2)[1];

    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      createNamespaceWithPreferredChooser(c, namespace1, v2);
      createNamespaceWithPreferredChooser(c, namespace2, v1);

      createTableAndVerifyVolumesUsed(c, namespace1, v2);
      createTableAndVerifyVolumesUsed(c, namespace2, v1);
    }
  }

  // Test that creates two tables, both with random volumes
  @Test
  public void twoTablesRandomVolumeChooser() throws Exception {
    String namespace1 = "ns_" + getUniqueNames(2)[0];
    String namespace2 = "ns_" + getUniqueNames(2)[1];

    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      createNamespaceWithRandomChooser(client, namespace1);
      createNamespaceWithRandomChooser(client, namespace2);

      createTableAndVerifyVolumesUsed(client, namespace1, v1, v2, v4);
      createTableAndVerifyVolumesUsed(client, namespace2, v1, v2, v4);
    }
  }

  // Test that creates two tables with different choosers, one random and the other preferred
  @Test
  public void twoTablesDiffChoosers() throws Exception {
    String namespace1 = "ns_" + getUniqueNames(2)[0];
    String namespace2 = "ns_" + getUniqueNames(2)[1];

    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      createNamespaceWithRandomChooser(client, namespace1);
      createNamespaceWithPreferredChooser(client, namespace2, v1);

      createTableAndVerifyVolumesUsed(client, namespace1, v1, v2, v4);
      createTableAndVerifyVolumesUsed(client, namespace2, v1);
    }
  }

  // Test that attempts to create a table that prefers a volume not currently configured.
  // This does not work, because it is a hard-coded requirement that chosen volumes exist in the
  // instance volumes set; the test is preserved here to document this restriction
  @Disabled
  @Test
  public void includeSpecialVolumeForTable() throws Exception {
    String namespace = "ns_" + getUniqueNames(1)[0];

    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      // the following table will be configured to go to the volume excluded from the instance
      // volumes set that are normally randomly selected from
      createNamespaceWithPreferredChooser(client, namespace, v3_disallowed);
      createTableAndVerifyVolumesUsed(client, namespace, v3_disallowed);
    }
  }

  // Test that relies on falling back to the system default for its preferred volume, because it
  // does not specify one in its per-table settings.
  @Test
  public void generalDefaultPreferredVolume() throws Exception {
    String namespace = "ns_" + getUniqueNames(1)[0];

    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      // create a namespace with the preferred volume chooser, but without specifying a preferred
      // volume; this should force it to use the general default preferred volume set in the
      // configure method
      createNamespaceWithPreferredChooser(client, namespace, null);
      createTableAndVerifyVolumesUsed(client, namespace, v4);
    }
  }

  // Test that verifies the log files are placed in the volume configured for the logger scope
  @Test
  public void waLogsSentToConfiguredVolumes() throws Exception {
    String namespace = "ns_" + getUniqueNames(1)[0];
    String tableName = namespace + "." + testName();

    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      // create namespace with preference for a volume other than where logs will be stored
      createNamespaceWithPreferredChooser(client, namespace, v1);
      // don't flush, in order to ensure WALs exist
      createAndVerifyTable(client, tableName, alpha_rows, false);
      // should only go to v2 as per configuration in configure()
      var walMgr = new WalStateManager(getServerContext());
      Map<Path,WalStateManager.WalState> allLogs = walMgr.getAllState();
      assertFalse(allLogs.isEmpty());
      String volume = v2.toString();
      allLogs.keySet().stream().map(Path::toString).forEach(path -> {
        assertTrue(path.startsWith(volume), () -> path + " did not contain " + volume);
      });
    }
  }
}
