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
  private Path v1, v2, v3, v4;
  private String[] rows = "a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z".split(",");
  private String namespace1;
  private String namespace2;

  @Override
  protected int defaultTimeoutSeconds() {
    return 30;
  };

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    // Get 2 tablet servers
    cfg.setNumTservers(2);
    namespace1 = "ns_" + getUniqueNames(2)[0];
    namespace2 = "ns_" + getUniqueNames(2)[1];

    // Set the general volume chooser to the PerTableVolumeChooser so that different choosers can be specified
    Map<String,String> siteConfig = new HashMap<String,String>();
    siteConfig.put(Property.GENERAL_VOLUME_CHOOSER.getKey(), PerTableVolumeChooser.class.getName());
    cfg.setSiteConfig(siteConfig);

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

    // Only add volumes 1, 2, and 4 to the list of instance volumes to have one volume that isn't in the options list when they are choosing
    cfg.setProperty(Property.INSTANCE_VOLUMES, v1.toString() + "," + v2.toString() + "," + v4.toString());

    // use raw local file system so walogs sync and flush will work
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());

    super.configure(cfg, hadoopCoreSite);

  }

  public void addSplits(Connector connector, String tableName) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    // Add 10 splits to the table
    SortedSet<Text> partitions = new TreeSet<Text>();
    for (String s : "b,e,g,j,l,o,q,t,v,y".split(","))
      partitions.add(new Text(s));
    connector.tableOperations().addSplits(tableName, partitions);
  }

  public void writeAndReadData(Connector connector, String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    // Write some data to the table
    BatchWriter bw = connector.createBatchWriter(tableName, new BatchWriterConfig());
    for (String s : rows) {
      Mutation m = new Mutation(new Text(s));
      m.put(EMPTY, EMPTY, EMPTY_VALUE);
      bw.addMutation(m);
    }
    bw.close();

    // Write the data to disk, read it back
    connector.tableOperations().flush(tableName, null, null, true);
    Scanner scanner = connector.createScanner(tableName, Authorizations.EMPTY);
    int i = 0;
    for (Entry<Key,Value> entry : scanner) {
      assertEquals("Data read is not data written", rows[i++], entry.getKey().getRow().toString());
    }
  }

  public void verifyVolumes(Connector connector, String tableName, Range tableRange, String vol) throws TableNotFoundException {
    // Verify the new files are written to the Volumes specified
    ArrayList<String> volumes = new ArrayList<String>();
    for (String s : vol.split(","))
      volumes.add(s);

    Scanner scanner = connector.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    scanner.setRange(tableRange);
    scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
    int fileCount = 0;
    for (Entry<Key,Value> entry : scanner) {
      boolean inVolume = false;
      for (String volume : volumes) {
        if (entry.getKey().getColumnQualifier().toString().contains(volume))
          inVolume = true;
      }
      assertTrue("Data not written to the correct volumes", inVolume);
      fileCount++;
    }
    assertEquals("Wrong number of files", 11, fileCount);
  }

  // Test that uses two tables with 10 split points each. They each use the PreferredVolumeChooser to choose volumes.
  @Test
  public void twoTablesPreferredVolumeChooser() throws Exception {
    log.info("Starting twoTablesPreferredVolumeChooser");

    // Create namespace
    Connector connector = getConnector();
    connector.namespaceOperations().create(namespace1);

    // Set properties on the namespace
    String propertyName = Property.TABLE_VOLUME_CHOOSER.getKey();
    String volume = PreferredVolumeChooser.class.getName();
    connector.namespaceOperations().setProperty(namespace1, propertyName, volume);

    propertyName = "table.custom.preferredVolumes";
    volume = v2.toString();
    connector.namespaceOperations().setProperty(namespace1, propertyName, volume);

    // Create table1 on namespace1
    String tableName = namespace1 + ".1";
    connector.tableOperations().create(tableName);
    String tableID = connector.tableOperations().tableIdMap().get(tableName);

    // Add 10 splits to the table
    addSplits(connector, tableName);
    // Write some data to the table
    writeAndReadData(connector, tableName);
    // Verify the new files are written to the Volumes specified
    verifyVolumes(connector, tableName, TabletsSection.getRange(tableID), volume);

    connector.namespaceOperations().create(namespace2);

    // Set properties on the namespace
    propertyName = Property.TABLE_VOLUME_CHOOSER.getKey();
    volume = PreferredVolumeChooser.class.getName();
    connector.namespaceOperations().setProperty(namespace2, propertyName, volume);

    propertyName = "table.custom.preferredVolumes";
    volume = v1.toString();
    connector.namespaceOperations().setProperty(namespace2, propertyName, volume);

    // Create table2 on namespace2
    String tableName2 = namespace2 + ".1";

    connector.tableOperations().create(tableName2);
    String tableID2 = connector.tableOperations().tableIdMap().get(tableName2);

    // Add 10 splits to the table
    addSplits(connector, tableName2);
    // Write some data to the table
    writeAndReadData(connector, tableName2);
    // Verify the new files are written to the Volumes specified
    verifyVolumes(connector, tableName2, TabletsSection.getRange(tableID2), volume);
  }

  // Test that uses two tables with 10 split points each. They each use the RandomVolumeChooser to choose volumes.
  @Test
  public void twoTablesRandomVolumeChooser() throws Exception {
    log.info("Starting twoTablesRandomVolumeChooser()");

    // Create namespace
    Connector connector = getConnector();
    connector.namespaceOperations().create(namespace1);

    // Set properties on the namespace
    String propertyName = Property.TABLE_VOLUME_CHOOSER.getKey();
    String volume = RandomVolumeChooser.class.getName();
    connector.namespaceOperations().setProperty(namespace1, propertyName, volume);

    // Create table1 on namespace1
    String tableName = namespace1 + ".1";
    connector.tableOperations().create(tableName);
    String tableID = connector.tableOperations().tableIdMap().get(tableName);

    // Add 10 splits to the table
    addSplits(connector, tableName);
    // Write some data to the table
    writeAndReadData(connector, tableName);
    // Verify the new files are written to the Volumes specified

    verifyVolumes(connector, tableName, TabletsSection.getRange(tableID), v1.toString() + "," + v2.toString() + "," + v4.toString());

    connector.namespaceOperations().create(namespace2);

    // Set properties on the namespace
    propertyName = Property.TABLE_VOLUME_CHOOSER.getKey();
    volume = RandomVolumeChooser.class.getName();
    connector.namespaceOperations().setProperty(namespace2, propertyName, volume);

    // Create table2 on namespace2
    String tableName2 = namespace2 + ".1";
    connector.tableOperations().create(tableName2);
    String tableID2 = connector.tableOperations().tableIdMap().get(tableName);

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
    String propertyName = Property.TABLE_VOLUME_CHOOSER.getKey();
    String volume = RandomVolumeChooser.class.getName();
    connector.namespaceOperations().setProperty(namespace1, propertyName, volume);

    // Create table1 on namespace1
    String tableName = namespace1 + ".1";
    connector.tableOperations().create(tableName);
    String tableID = connector.tableOperations().tableIdMap().get(tableName);

    // Add 10 splits to the table
    addSplits(connector, tableName);
    // Write some data to the table
    writeAndReadData(connector, tableName);
    // Verify the new files are written to the Volumes specified

    verifyVolumes(connector, tableName, TabletsSection.getRange(tableID), v1.toString() + "," + v2.toString() + "," + v4.toString());

    connector.namespaceOperations().create(namespace2);

    // Set properties on the namespace
    propertyName = Property.TABLE_VOLUME_CHOOSER.getKey();
    volume = PreferredVolumeChooser.class.getName();
    connector.namespaceOperations().setProperty(namespace2, propertyName, volume);

    propertyName = "table.custom.preferredVolumes";
    volume = v1.toString();
    connector.namespaceOperations().setProperty(namespace2, propertyName, volume);

    // Create table2 on namespace2
    String tableName2 = namespace2 + ".1";
    connector.tableOperations().create(tableName2);
    String tableID2 = connector.tableOperations().tableIdMap().get(tableName2);

    // Add 10 splits to the table
    addSplits(connector, tableName2);
    // Write some data to the table
    writeAndReadData(connector, tableName2);
    // Verify the new files are written to the Volumes specified
    verifyVolumes(connector, tableName2, TabletsSection.getRange(tableID2), volume);
  }

  // Test that uses one table with 10 split points each. It uses the StaticVolumeChooser, but no preferred volume is specified. This means that the volume
  // is chosen randomly from all instance volumes.
  @Test
  public void missingVolumePreferredVolumeChooser() throws Exception {
    log.info("Starting missingVolumePreferredVolumeChooser");

    // Create namespace
    Connector connector = getConnector();
    connector.namespaceOperations().create(namespace1);

    // Set properties on the namespace
    String propertyName = Property.TABLE_VOLUME_CHOOSER.getKey();
    String volume = PreferredVolumeChooser.class.getName();
    connector.namespaceOperations().setProperty(namespace1, propertyName, volume);

    // Create table1 on namespace1
    String tableName = namespace1 + ".1";
    connector.tableOperations().create(tableName);
    String tableID = connector.tableOperations().tableIdMap().get(tableName);

    // Add 10 splits to the table
    addSplits(connector, tableName);
    // Write some data to the table
    writeAndReadData(connector, tableName);
    // Verify the new files are written to the Volumes specified
    verifyVolumes(connector, tableName, TabletsSection.getRange(tableID), v1.toString() + "," + v2.toString() + "," + v4.toString());
  }

  // Test that uses one table with 10 split points each. It uses the PreferredVolumeChooser, but preferred volume is not an instance volume. This means that the
  // volume is chosen randomly from all instance volumes
  @Test
  public void notInstancePreferredVolumeChooser() throws Exception {
    log.info("Starting notInstancePreferredVolumeChooser");

    // Create namespace
    Connector connector = getConnector();
    connector.namespaceOperations().create(namespace1);

    // Set properties on the namespace
    String propertyName = Property.TABLE_VOLUME_CHOOSER.getKey();
    String volume = PreferredVolumeChooser.class.getName();
    connector.namespaceOperations().setProperty(namespace1, propertyName, volume);

    propertyName = "table.custom.preferredVolumes";
    volume = v3.toString();
    connector.namespaceOperations().setProperty(namespace1, propertyName, volume);

    // Create table1 on namespace1
    String tableName = namespace1 + ".1";
    connector.tableOperations().create(tableName);
    String tableID = connector.tableOperations().tableIdMap().get(tableName);

    // Add 10 splits to the table
    addSplits(connector, tableName);
    // Write some data to the table
    writeAndReadData(connector, tableName);
    // Verify the new files are written to the Volumes specified
    verifyVolumes(connector, tableName, TabletsSection.getRange(tableID), v1.toString() + "," + v2.toString() + "," + v4.toString());
  }

  // Test that uses one table with 10 split points each. It does not specify a specific chooser, so the volume is chosen randomly from all instance volumes.
  @Test
  public void chooserNotSpecified() throws Exception {
    log.info("Starting chooserNotSpecified");

    // Create a table
    Connector connector = getConnector();
    String tableName = getUniqueNames(2)[0];
    connector.tableOperations().create(tableName);
    String tableID = connector.tableOperations().tableIdMap().get(tableName);

    // Add 10 splits to the table
    addSplits(connector, tableName);
    // Write some data to the table
    writeAndReadData(connector, tableName);

    // Verify the new files are written to the Volumes specified
    verifyVolumes(connector, tableName, TabletsSection.getRange(tableID), v1.toString() + "," + v2.toString() + "," + v4.toString());
  }

}
