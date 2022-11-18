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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.fs.DelegatingChooser;
import org.apache.accumulo.core.spi.fs.PreferredVolumeChooser;
import org.apache.accumulo.core.spi.fs.RandomVolumeChooser;
import org.apache.accumulo.core.spi.fs.VolumeChooserEnvironment.Scope;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class VolumeChooserIT extends ConfigurableMacBase {

  private static final String TP = Property.TABLE_ARBITRARY_PROP_PREFIX.getKey();
  static final String PREFERRED_CHOOSER_PROP = TP + "volume.preferred";
  public static final String PERTABLE_CHOOSER_PROP = TP + "volume.chooser";

  private static final String GP = Property.GENERAL_ARBITRARY_PROP_PREFIX.getKey();

  static final String getPreferredProp(Scope scope) {
    return GP + "volume.preferred." + scope.name().toLowerCase();
  }

  static final String getPerTableProp(Scope scope) {
    return GP + "volume.chooser." + scope.name().toLowerCase();
  }

  private static final Text EMPTY = new Text();
  private static final Value EMPTY_VALUE = new Value();
  private File volDirBase;
  private Path v1, v2, v3;
  public static String[] alpha_rows =
      "a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z".split(",");
  private String namespace1;
  private String namespace2;
  private String systemPreferredVolumes;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    // Get 2 tablet servers
    cfg.setNumTservers(2);
    namespace1 = "ns_" + getUniqueNames(2)[0];
    namespace2 = "ns_" + getUniqueNames(2)[1];

    // Set the general volume chooser to the DelegatingChooser so that different choosers can be
    // specified
    Map<String,String> siteConfig = new HashMap<>();
    siteConfig.put(Property.GENERAL_VOLUME_CHOOSER.getKey(), DelegatingChooser.class.getName());
    // if a table doesn't have a volume chooser, use the preferred volume chooser
    siteConfig.put(PERTABLE_CHOOSER_PROP, PreferredVolumeChooser.class.getName());

    // Set up 4 different volume paths
    File baseDir = cfg.getDir();
    volDirBase = new File(baseDir, "volumes");
    File v1f = new File(volDirBase, "v1");
    File v2f = new File(volDirBase, "v2");
    File v3f = new File(volDirBase, "v3");
    v1 = new Path("file://" + v1f.getAbsolutePath());
    v2 = new Path("file://" + v2f.getAbsolutePath());
    v3 = new Path("file://" + v3f.getAbsolutePath());

    systemPreferredVolumes = v1 + "," + v2;
    // exclude v3
    siteConfig.put(PREFERRED_CHOOSER_PROP, systemPreferredVolumes);
    cfg.setSiteConfig(siteConfig);

    siteConfig.put(getPerTableProp(Scope.LOGGER), PreferredVolumeChooser.class.getName());
    siteConfig.put(getPreferredProp(Scope.LOGGER), v2.toString());
    siteConfig.put(getPerTableProp(Scope.INIT), PreferredVolumeChooser.class.getName());
    siteConfig.put(getPreferredProp(Scope.INIT), systemPreferredVolumes);
    cfg.setSiteConfig(siteConfig);

    // Only add volumes 1, 2, and 4 to the list of instance volumes to have one volume that isn't in
    // the options list when they are choosing
    cfg.setProperty(Property.INSTANCE_VOLUMES, v1 + "," + v2 + "," + v3);

    // use raw local file system so walogs sync and flush will work
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());

    super.configure(cfg, hadoopCoreSite);

  }

  public static void addSplits(AccumuloClient accumuloClient, String tableName)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    // Add 10 splits to the table
    SortedSet<Text> partitions = new TreeSet<>();
    for (String s : alpha_rows) {
      partitions.add(new Text(s));
    }
    accumuloClient.tableOperations().addSplits(tableName, partitions);
  }

  public static void writeAndReadData(AccumuloClient accumuloClient, String tableName)
      throws Exception {
    writeDataToTable(accumuloClient, tableName, alpha_rows);

    // Write the data to disk, read it back
    accumuloClient.tableOperations().flush(tableName, null, null, true);
    try (Scanner scanner = accumuloClient.createScanner(tableName, Authorizations.EMPTY)) {
      int i = 0;
      for (Entry<Key,Value> entry : scanner) {
        assertEquals(alpha_rows[i++], entry.getKey().getRow().toString(),
            "Data read is not data written");
      }
    }
  }

  public static void writeDataToTable(AccumuloClient accumuloClient, String tableName,
      String[] rows) throws Exception {
    // Write some data to the table
    try (BatchWriter bw = accumuloClient.createBatchWriter(tableName)) {
      for (String s : rows) {
        Mutation m = new Mutation(new Text(s));
        m.put(EMPTY, EMPTY, EMPTY_VALUE);
        bw.addMutation(m);
      }
    }
  }

  public static void verifyVolumes(AccumuloClient accumuloClient, Range tableRange, String vol)
      throws Exception {
    // Verify the new files are written to the Volumes specified
    ArrayList<String> volumes = new ArrayList<>();
    Collections.addAll(volumes, vol.split(","));

    TreeSet<String> volumesSeen = new TreeSet<>();
    int fileCount = 0;
    try (Scanner scanner = accumuloClient.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      scanner.setRange(tableRange);
      scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
      for (Entry<Key,Value> entry : scanner) {
        boolean inVolume = false;
        for (String volume : volumes) {
          if (entry.getKey().getColumnQualifier().toString().contains(volume)) {
            volumesSeen.add(volume);
            inVolume = true;
          }
        }
        assertTrue(inVolume,
            "Data not written to the correct volumes.  " + entry.getKey().getColumnQualifier());
        fileCount++;
      }
    }
    assertEquals(volumes.size(), volumesSeen.size(),
        "Did not see all the volumes. volumes: " + volumes + " volumes seen: " + volumesSeen);
    assertEquals(26, fileCount, "Wrong number of files");
  }

  public static void verifyNoVolumes(AccumuloClient accumuloClient, Range tableRange)
      throws Exception {
    try (Scanner scanner = accumuloClient.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      scanner.setRange(tableRange);
      scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
      for (Entry<Key,Value> entry : scanner) {
        fail("Data incorrectly written to " + entry.getKey().getColumnQualifier());
      }
    }
  }

  private void configureNamespace(AccumuloClient accumuloClient, String volumeChooserClassName,
      String configuredVolumes, String namespace) throws Exception {
    accumuloClient.namespaceOperations().create(namespace);
    // Set properties on the namespace
    accumuloClient.namespaceOperations().setProperty(namespace, PERTABLE_CHOOSER_PROP,
        volumeChooserClassName);
    accumuloClient.namespaceOperations().setProperty(namespace, PREFERRED_CHOOSER_PROP,
        configuredVolumes);
  }

  private void verifyVolumesForWritesToNewTable(AccumuloClient accumuloClient, String myNamespace,
      String expectedVolumes) throws Exception {
    String tableName = myNamespace + ".1";

    accumuloClient.tableOperations().create(tableName);
    TableId tableID = TableId.of(accumuloClient.tableOperations().tableIdMap().get(tableName));

    // Add 10 splits to the table
    addSplits(accumuloClient, tableName);
    // Write some data to the table
    writeAndReadData(accumuloClient, tableName);
    // Verify the new files are written to the Volumes specified
    verifyVolumes(accumuloClient, TabletsSection.getRange(tableID), expectedVolumes);
  }

  public static void verifyWaLogVolumes(AccumuloClient accumuloClient, Range tableRange, String vol)
      throws TableNotFoundException {
    // Verify the new files are written to the Volumes specified
    ArrayList<String> volumes = new ArrayList<>();
    Collections.addAll(volumes, vol.split(","));

    TreeSet<String> volumesSeen = new TreeSet<>();
    try (Scanner scanner = accumuloClient.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      scanner.setRange(tableRange);
      scanner.fetchColumnFamily(LogColumnFamily.NAME);
      for (Entry<Key,Value> entry : scanner) {
        boolean inVolume = false;
        for (String volume : volumes) {
          if (entry.getKey().getColumnQualifier().toString().contains(volume)) {
            volumesSeen.add(volume);
          }
          inVolume = true;
        }
        assertTrue(inVolume,
            "Data not written to the correct volumes.  " + entry.getKey().getColumnQualifier());
      }
    }
  }

  // Test that uses two tables with 10 split points each. They each use the PreferredVolumeChooser
  // to choose volumes.
  @Test
  public void twoTablesPreferredVolumeChooser() throws Exception {
    log.info("Starting twoTablesPreferredVolumeChooser");

    // Create namespace
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      // Set properties on the namespace
      // namespace 1 -> v2
      configureNamespace(c, PreferredVolumeChooser.class.getName(), v2.toString(), namespace1);

      // Create table1 on namespace1
      verifyVolumesForWritesToNewTable(c, namespace1, v2.toString());

      configureNamespace(c, PreferredVolumeChooser.class.getName(), v1.toString(), namespace2);

      // Create table2 on namespace2
      verifyVolumesForWritesToNewTable(c, namespace2, v1.toString());
    }
  }

  // Test that uses two tables with 10 split points each. They each use the RandomVolumeChooser to
  // choose volumes.
  @Test
  public void twoTablesRandomVolumeChooser() throws Exception {
    log.info("Starting twoTablesRandomVolumeChooser()");

    // Create namespace
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      createAndVerify(client, namespace1, v1 + "," + v2 + "," + v3);
      createAndVerify(client, namespace2, v1 + "," + v2 + "," + v3);
    }
  }

  private void createAndVerify(AccumuloClient client, String ns, String expectedVolumes)
      throws Exception {
    client.namespaceOperations().create(ns);

    // Set properties on the namespace
    client.namespaceOperations().setProperty(ns, PERTABLE_CHOOSER_PROP,
        RandomVolumeChooser.class.getName());

    verifyVolumesForWritesToNewTable(client, ns, expectedVolumes);
  }

  // Test that uses 2 tables with 10 split points each. The first uses the RandomVolumeChooser and
  // the second uses the StaticVolumeChooser to choose volumes.
  @Test
  public void twoTablesDiffChoosers() throws Exception {
    log.info("Starting twoTablesDiffChoosers");

    // Create namespace
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      createAndVerify(c, namespace1, v1 + "," + v2 + "," + v3);
      configureNamespace(c, PreferredVolumeChooser.class.getName(), v1.toString(), namespace2);
      // Create table2 on namespace2
      verifyVolumesForWritesToNewTable(c, namespace2, v1.toString());
    }
  }

  @Test
  public void includeSpecialVolumeForTable() throws Exception {
    log.info("Starting includeSpecialVolumeForTable");
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {

      // the following table will be configured to go to the excluded volume
      String configuredVolumes = v3.toString();
      configureNamespace(client, PreferredVolumeChooser.class.getName(), configuredVolumes,
          namespace2);
      verifyVolumesForWritesToNewTable(client, namespace2, configuredVolumes);
    }
  }

  @Test
  public void waLogsSentToConfiguredVolumes() throws Exception {
    log.info("Starting waLogsSentToConfiguredVolumes");

    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      String tableName = "anotherTable";
      client.tableOperations().create(tableName);

      VolumeChooserIT.addSplits(client, tableName);
      VolumeChooserIT.writeDataToTable(client, tableName, alpha_rows);
      // should only go to v2 as per configuration in configure()
      VolumeChooserIT.verifyWaLogVolumes(client, new Range(), v2.toString());
    }
  }
}
