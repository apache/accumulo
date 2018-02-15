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
package org.apache.accumulo.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.fs.PerTableVolumeChooser;
import org.apache.accumulo.server.fs.PreferredVolumeChooser;
import org.apache.accumulo.server.fs.RandomVolumeChooser;
import org.apache.accumulo.server.fs.VolumeChooserEnvironment.ChooserScope;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 *
 */
public class VolumeChooserIT extends ConfigurableMacBase {

  private static final Text EMPTY = new Text();
  private static final Value EMPTY_VALUE = new Value(new byte[] {});
  private File volDirBase;
  @SuppressWarnings("unused")
  private Path v1, v2, v3, v4;
  private static String[] rows = "a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z".split(",");
  private String namespace1;
  private String namespace2;
  private String systemPreferredVolumes;

  @Override
  protected int defaultTimeoutSeconds() {
    return 60;
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    // Get 2 tablet servers
    cfg.setNumTservers(2);
    namespace1 = "ns_" + getUniqueNames(2)[0];
    namespace2 = "ns_" + getUniqueNames(2)[1];

    // Set the general volume chooser to the PerTableVolumeChooser so that different choosers can be specified
    Map<String,String> siteConfig = new HashMap<>();
    siteConfig.put(Property.GENERAL_VOLUME_CHOOSER.getKey(), PerTableVolumeChooser.class.getName());
    // if a table doesn't have a volume chooser, use the preferred volume chooser
    siteConfig.put(PerTableVolumeChooser.TABLE_VOLUME_CHOOSER, PreferredVolumeChooser.class.getName());

    // Set up 4 different volume paths
    File baseDir = cfg.getDir();
    volDirBase = new File(baseDir, "volumes");
    File v1f = new File(volDirBase, "v1");
    File v2f = new File(volDirBase, "v2");
    File v3f = new File(volDirBase, "v3");
    File v4f = new File(volDirBase, "v4");
    v1 = new Path("file://" + v1f.getAbsolutePath());
    v2 = new Path("file://" + v2f.getAbsolutePath());
    v3 = new Path("file://" + v3f.getAbsolutePath());
    v4 = new Path("file://" + v4f.getAbsolutePath());

    systemPreferredVolumes = v1.toString() + "," + v2.toString();
    siteConfig.put(PreferredVolumeChooser.TABLE_PREFERRED_VOLUMES, systemPreferredVolumes); // exclude v4
    cfg.setSiteConfig(siteConfig);

    siteConfig.put(PerTableVolumeChooser.getPropertyNameForScope(ChooserScope.LOGGER), PreferredVolumeChooser.class.getName());
    siteConfig.put(PreferredVolumeChooser.getPropertyNameForScope(ChooserScope.LOGGER), v2.toString());
    cfg.setSiteConfig(siteConfig);

    // Only add volumes 1, 2, and 4 to the list of instance volumes to have one volume that isn't in the options list when they are choosing
    cfg.setProperty(Property.INSTANCE_VOLUMES, v1.toString() + "," + v2.toString() + "," + v4.toString());

    // use raw local file system so walogs sync and flush will work
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());

    super.configure(cfg, hadoopCoreSite);

  }

  public static void addSplits(Connector connector, String tableName) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    // Add 10 splits to the table
    SortedSet<Text> partitions = new TreeSet<>();
    for (String s : rows)
      partitions.add(new Text(s));
    connector.tableOperations().addSplits(tableName, partitions);
  }

  public static void writeAndReadData(Connector connector, String tableName) throws Exception {
    writeDataToTable(connector, tableName);

    // Write the data to disk, read it back
    connector.tableOperations().flush(tableName, null, null, true);
    try (Scanner scanner = connector.createScanner(tableName, Authorizations.EMPTY)) {
      int i = 0;
      for (Entry<Key,Value> entry : scanner) {
        assertEquals("Data read is not data written", rows[i++], entry.getKey().getRow().toString());
      }
    }
  }

  public static void writeDataToTable(Connector connector, String tableName) throws Exception {
    // Write some data to the table
    BatchWriter bw = connector.createBatchWriter(tableName, new BatchWriterConfig());
    for (String s : rows) {
      Mutation m = new Mutation(new Text(s));
      m.put(EMPTY, EMPTY, EMPTY_VALUE);
      bw.addMutation(m);
    }
    bw.close();
  }

  public static void verifyVolumes(Connector connector, String tableName, Range tableRange, String vol) throws Exception {
    // Verify the new files are written to the Volumes specified
    ArrayList<String> volumes = new ArrayList<>();
    for (String s : vol.split(","))
      volumes.add(s);

    TreeSet<String> volumesSeen = new TreeSet<>();
    int fileCount = 0;
    try (Scanner scanner = connector.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
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
        assertTrue("Data not written to the correct volumes.  " + entry.getKey().getColumnQualifier().toString(), inVolume);
        fileCount++;
      }
    }
    assertEquals("Did not see all the volumes. volumes: " + volumes.toString() + " volumes seen: " + volumesSeen.toString(), volumes.size(), volumesSeen.size());
    assertEquals("Wrong number of files", 26, fileCount);
  }

  public static void verifyNoVolumes(Connector connector, String tableName, Range tableRange) throws Exception {
    try (Scanner scanner = connector.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      scanner.setRange(tableRange);
      scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
      for (Entry<Key,Value> entry : scanner) {
        fail("Data incorrectly written to " + entry.getKey().getColumnQualifier().toString());
      }
    }
  }

  private void configureNamespace(Connector connector, String volumeChooserClassName, String configuredVolumes, String namespace) throws Exception {
    connector.namespaceOperations().create(namespace);
    // Set properties on the namespace
    connector.namespaceOperations().setProperty(namespace, PerTableVolumeChooser.TABLE_VOLUME_CHOOSER, volumeChooserClassName);
    connector.namespaceOperations().setProperty(namespace, PreferredVolumeChooser.TABLE_PREFERRED_VOLUMES, configuredVolumes);
  }

  private void verifyVolumesForWritesToNewTable(Connector connector, String myNamespace, String expectedVolumes) throws Exception {
    String tableName = myNamespace + ".1";

    connector.tableOperations().create(tableName);
    Table.ID tableID = Table.ID.of(connector.tableOperations().tableIdMap().get(tableName));

    // Add 10 splits to the table
    addSplits(connector, tableName);
    // Write some data to the table
    writeAndReadData(connector, tableName);
    // Verify the new files are written to the Volumes specified
    verifyVolumes(connector, tableName, TabletsSection.getRange(tableID), expectedVolumes);
  }

  public static void verifyWaLogVolumes(Connector connector, Range tableRange, String vol) throws TableNotFoundException {
    // Verify the new files are written to the Volumes specified
    ArrayList<String> volumes = new ArrayList<>();
    for (String s : vol.split(","))
      volumes.add(s);

    TreeSet<String> volumesSeen = new TreeSet<>();
    try (Scanner scanner = connector.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      scanner.setRange(tableRange);
      scanner.fetchColumnFamily(TabletsSection.LogColumnFamily.NAME);
      for (Entry<Key,Value> entry : scanner) {
        boolean inVolume = false;
        for (String volume : volumes) {
          if (entry.getKey().getColumnQualifier().toString().contains(volume))
            volumesSeen.add(volume);
          inVolume = true;
        }
        assertTrue("Data not written to the correct volumes.  " + entry.getKey().getColumnQualifier().toString(), inVolume);
      }
    }
  }

  // Test that uses two tables with 10 split points each. They each use the PreferredVolumeChooser to choose volumes.
  @Test
  public void twoTablesPreferredVolumeChooser() throws Exception {
    log.info("Starting twoTablesPreferredVolumeChooser");

    // Create namespace
    Connector connector = getConnector();
    connector.namespaceOperations().create(namespace1);

    // Set properties on the namespace
    // namespace 1 -> v2
    connector.namespaceOperations().setProperty(namespace1, PerTableVolumeChooser.TABLE_VOLUME_CHOOSER, PreferredVolumeChooser.class.getName());
    connector.namespaceOperations().setProperty(namespace1, PreferredVolumeChooser.TABLE_PREFERRED_VOLUMES, v2.toString());

    // Create table1 on namespace1
    verifyVolumesForWritesToNewTable(connector, namespace1, v2.toString());

    connector.namespaceOperations().create(namespace2);
    // Set properties on the namespace
    connector.namespaceOperations().setProperty(namespace2, PerTableVolumeChooser.TABLE_VOLUME_CHOOSER, PreferredVolumeChooser.class.getName());
    connector.namespaceOperations().setProperty(namespace2, PreferredVolumeChooser.TABLE_PREFERRED_VOLUMES, v1.toString());

    // Create table2 on namespace2
    verifyVolumesForWritesToNewTable(connector, namespace2, v1.toString());
  }

  // Test that uses two tables with 10 split points each. They each use the RandomVolumeChooser to choose volumes.
  @Test
  public void twoTablesRandomVolumeChooser() throws Exception {
    log.info("Starting twoTablesRandomVolumeChooser()");

    // Create namespace
    Connector connector = getConnector();
    connector.namespaceOperations().create(namespace1);

    // Set properties on the namespace
    connector.namespaceOperations().setProperty(namespace1, PerTableVolumeChooser.TABLE_VOLUME_CHOOSER, RandomVolumeChooser.class.getName());

    // Create table1 on namespace1
    String tableName = namespace1 + ".1";
    connector.tableOperations().create(tableName);
    Table.ID tableID = Table.ID.of(connector.tableOperations().tableIdMap().get(tableName));

    // Add 10 splits to the table
    addSplits(connector, tableName);
    // Write some data to the table
    writeAndReadData(connector, tableName);
    // Verify the new files are written to the Volumes specified

    verifyVolumes(connector, tableName, TabletsSection.getRange(tableID), v1.toString() + "," + v2.toString() + "," + v4.toString());

    connector.namespaceOperations().create(namespace2);

    // Set properties on the namespace
    connector.namespaceOperations().setProperty(namespace2, PerTableVolumeChooser.TABLE_VOLUME_CHOOSER, RandomVolumeChooser.class.getName());

    // Create table2 on namespace2
    String tableName2 = namespace2 + ".1";
    connector.tableOperations().create(tableName2);
    Table.ID tableID2 = Table.ID.of(connector.tableOperations().tableIdMap().get(tableName2));

    // / Add 10 splits to the table
    addSplits(connector, tableName2);
    // Write some data to the table
    writeAndReadData(connector, tableName2);
    // Verify the new files are written to the Volumes specified
    verifyVolumes(connector, tableName2, TabletsSection.getRange(tableID2), v1.toString() + "," + v2.toString() + "," + v4.toString());
  }

  // Test that uses two tables with 10 split points each. The first uses the RandomVolumeChooser and the second uses the
  // StaticVolumeChooser to choose volumes.
  @Test
  public void twoTablesDiffChoosers() throws Exception {
    log.info("Starting twoTablesDiffChoosers");

    // Create namespace
    Connector connector = getConnector();
    connector.namespaceOperations().create(namespace1);

    // Set properties on the namespace
    connector.namespaceOperations().setProperty(namespace1, PerTableVolumeChooser.TABLE_VOLUME_CHOOSER, RandomVolumeChooser.class.getName());

    // Create table1 on namespace1
    verifyVolumesForWritesToNewTable(connector, namespace1, v1.toString() + "," + v2.toString() + "," + v4.toString());
    connector.namespaceOperations().create(namespace2);

    connector.namespaceOperations().setProperty(namespace2, PerTableVolumeChooser.TABLE_VOLUME_CHOOSER, PreferredVolumeChooser.class.getName());
    connector.namespaceOperations().setProperty(namespace2, PreferredVolumeChooser.TABLE_PREFERRED_VOLUMES, v1.toString());

    // Create table2 on namespace2
    verifyVolumesForWritesToNewTable(connector, namespace2, v1.toString());
  }

  @Test
  public void includeSpecialVolumeForTable() throws Exception {
    log.info("Starting includeSpecialVolumeForTable");
    Connector connector = getConnector();

    // the following table will be configured to go to the excluded volume
    String configuredVolumes = v4.toString();
    configureNamespace(connector, PreferredVolumeChooser.class.getName(), configuredVolumes, namespace2);
    verifyVolumesForWritesToNewTable(connector, namespace2, configuredVolumes);
  }

  @Test
  public void waLogsSentToConfiguredVolumes() throws Exception {
    log.info("Starting waLogsSentToConfiguredVolumes");

    Connector connector = getConnector();
    String tableName = "anotherTable";
    connector.tableOperations().create(tableName);

    VolumeChooserIT.addSplits(connector, tableName);
    VolumeChooserIT.writeDataToTable(connector, tableName);
    // should only go to v2 as per configuration in configure()
    VolumeChooserIT.verifyWaLogVolumes(connector, new Range(), v2.toString());
  }
}
