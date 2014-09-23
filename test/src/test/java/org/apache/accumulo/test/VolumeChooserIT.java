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

import static org.junit.Assert.*;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacIT;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 * 
 */
public class VolumeChooserIT extends ConfigurableMacIT {

  private static final Text EMPTY = new Text();
  private static final Value EMPTY_VALUE = new Value(new byte[] {});
  private File volDirBase;
  private Path v1, v2, v3, v4;

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    // Get 2 tablet servers
    cfg.setNumTservers(2);

    // Set the general volume chooser to the GeneralVolumeChooser so that different choosers can be specified
    Map<String,String> siteConfig = new HashMap<String,String>();
    siteConfig.put(Property.GENERAL_VOLUME_CHOOSER.getKey(), org.apache.accumulo.server.fs.GeneralVolumeChooser.class.getName());
    cfg.setSiteConfig(siteConfig);

    // Set up 4 different volume paths
    File baseDir = cfg.getDir();
    volDirBase = new File(baseDir, "volumes");
    File v1f = new File(volDirBase, "v1");
    File v2f = new File(volDirBase, "v2");
    File v3f = new File(volDirBase, "v3");
    File v4f = new File(volDirBase, "v4");
    v1f.mkdir();
    v2f.mkdir();
    v4f.mkdir();
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

  // Test that uses two tables with 10 split points each. They each use the StaticVolumeChooser to choose volumes.
  @Test(timeout = 60 * 1000)
  public void twoTablesStaticVolumeChooser() throws Exception {
    log.info("Starting StaticVolumeChooser");

    // Create and populate initial properties map for creating table 1
    Map<String,String> properties = new HashMap<String,String>();
    String propertyName = "table.custom.chooser";
    String volume = "org.apache.accumulo.server.fs.StaticVolumeChooser";
    properties.put(propertyName, volume);

    propertyName = "table.custom.preferredVolumes";
    volume = v2.toString();
    properties.put(propertyName, volume);

    // Create a table with the initial properties
    Connector connector = getConnector();
    String tableName = getUniqueNames(2)[0];
    connector.tableOperations().create(tableName, true, TimeType.MILLIS, properties);

    // Add 10 splits to the table
    SortedSet<Text> partitions = new TreeSet<Text>();
    for (String s : "b,e,g,j,l,o,q,t,v,y".split(","))
      partitions.add(new Text(s));
    connector.tableOperations().addSplits(tableName, partitions);
    // Write some data to the table
    BatchWriter bw = connector.createBatchWriter(tableName, new BatchWriterConfig());
    String[] rows = "a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z".split(",");
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
      assertEquals(rows[i++], entry.getKey().getRow().toString());
    }

    // Verify the new files are written to the Volumes specified
    scanner = connector.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    scanner.setRange(new Range("1", "1<"));
    scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
    int fileCount = 0;
    for (Entry<Key,Value> entry : scanner) {
      boolean inV2 = entry.getKey().getColumnQualifier().toString().contains(v2.toString());
      assertTrue(inV2);
      fileCount++;
    }
    assertEquals(11, fileCount);

    // Replace the current preferredVolumes property for table 2
    properties.remove("table.custom.preferredVolumes");
    propertyName = "table.custom.preferredVolumes";
    volume = v1.toString();
    properties.put(propertyName, volume);

    // Create table 2 with the new properties
    String tableName2 = getUniqueNames(2)[1];
    connector.tableOperations().create(tableName2, true, TimeType.MILLIS, properties);

    // Add 10 splits to the table
    partitions = new TreeSet<Text>();
    for (String s : "b,e,g,j,l,o,q,t,v,y".split(","))
      partitions.add(new Text(s));
    connector.tableOperations().addSplits(tableName2, partitions);
    // Write some data to the table
    bw = connector.createBatchWriter(tableName2, new BatchWriterConfig());
    rows = "a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z".split(",");
    for (String s : rows) {
      Mutation m = new Mutation(new Text(s));
      m.put(EMPTY, EMPTY, EMPTY_VALUE);
      bw.addMutation(m);
    }
    bw.close();
    // Write the data to disk, read it back
    connector.tableOperations().flush(tableName2, null, null, true);
    scanner = connector.createScanner(tableName2, Authorizations.EMPTY);
    i = 0;
    for (Entry<Key,Value> entry : scanner) {
      assertEquals(rows[i++], entry.getKey().getRow().toString());
    }

    // Verify the new files are written to the different volumes
    scanner = connector.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    scanner.setRange(new Range("2", "2<"));
    scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
    fileCount = 0;
    for (Entry<Key,Value> entry : scanner) {
      boolean inV1 = entry.getKey().getColumnQualifier().toString().contains(v1.toString());
      assertTrue(inV1);
      fileCount++;
    }
    assertEquals(11, fileCount);
  }

  // Test that uses two tables with 10 split points each. They each use the RandomVolumeChooser to choose volumes.
  @Test(timeout = 60 * 1000)
  public void twoTablesRandomVolumeChooser() throws Exception {
    log.info("Starting RandomVolumeChooser");

    // Create and populate initial properties map for creating table 1
    Map<String,String> properties = new HashMap<String,String>();
    String propertyName = "table.custom.chooser";
    String volume = "org.apache.accumulo.server.fs.RandomVolumeChooser";
    properties.put(propertyName, volume);

    propertyName = "table.custom.preferredVolumes";
    volume = v1.toString() + "," + v2.toString() + "," + v3.toString();
    properties.put(propertyName, volume);

    // Create a table with the initial properties
    Connector connector = getConnector();
    String tableName = getUniqueNames(2)[0];
    connector.tableOperations().create(tableName, true, TimeType.MILLIS, properties);

    // Add 10 splits to the table
    SortedSet<Text> partitions = new TreeSet<Text>();
    for (String s : "b,e,g,j,l,o,q,t,v,y".split(","))
      partitions.add(new Text(s));
    connector.tableOperations().addSplits(tableName, partitions);
    // Write some data to the table
    BatchWriter bw = connector.createBatchWriter(tableName, new BatchWriterConfig());
    String[] rows = "a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z".split(",");
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
      assertEquals(rows[i++], entry.getKey().getRow().toString());
    }

    // Verify the new files are written to the Volumes specified
    scanner = connector.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    scanner.setRange(new Range("1", "1<"));
    scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
    int fileCount = 0;
    for (Entry<Key,Value> entry : scanner) {
      boolean inV1 = entry.getKey().getColumnQualifier().toString().contains(v1.toString());
      boolean inV2 = entry.getKey().getColumnQualifier().toString().contains(v2.toString());
      assertTrue(inV1 || inV2);
      fileCount++;
    }
    assertEquals(11, fileCount);

    // Replace the current preferredVolumes property for table 2
    properties.remove("table.custom.preferredVolumes");
    propertyName = "table.custom.preferredVolumes";
    volume = v1.toString();
    properties.put(propertyName, volume);

    // Create table 2 with the new properties
    String tableName2 = getUniqueNames(2)[1];
    connector.tableOperations().create(tableName2, true, TimeType.MILLIS, properties);

    // Add 10 splits to the table
    partitions = new TreeSet<Text>();
    for (String s : "b,e,g,j,l,o,q,t,v,y".split(","))
      partitions.add(new Text(s));
    connector.tableOperations().addSplits(tableName2, partitions);
    // Write some data to the table
    bw = connector.createBatchWriter(tableName2, new BatchWriterConfig());
    rows = "a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z".split(",");
    for (String s : rows) {
      Mutation m = new Mutation(new Text(s));
      m.put(EMPTY, EMPTY, EMPTY_VALUE);
      bw.addMutation(m);
    }
    bw.close();
    // Write the data to disk, read it back
    connector.tableOperations().flush(tableName2, null, null, true);
    scanner = connector.createScanner(tableName2, Authorizations.EMPTY);
    i = 0;
    for (Entry<Key,Value> entry : scanner) {
      assertEquals(rows[i++], entry.getKey().getRow().toString());
    }

    // Verify the new files are written to the different volumes
    scanner = connector.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    scanner.setRange(new Range("2", "2<"));
    scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
    fileCount = 0;
    for (Entry<Key,Value> entry : scanner) {
      boolean inV1 = entry.getKey().getColumnQualifier().toString().contains(v1.toString());
      assertTrue(inV1);
      fileCount++;
    }
    assertEquals(11, fileCount);
  }

  // Test that uses two tables with 10 split points each. The first uses the RandomVolumeChooser and the second uses the
  // StaticVolumeChooser to choose volumes.
  @Test(timeout = 60 * 1000)
  public void twoTablesDiffChoosers() throws Exception {
    log.info("Starting twoTablesDiffChoosers");

    // Create and populate initial properties map for creating table 1
    Map<String,String> properties = new HashMap<String,String>();
    String propertyName = "table.custom.chooser";
    String volume = "org.apache.accumulo.server.fs.RandomVolumeChooser";
    properties.put(propertyName, volume);

    propertyName = "table.custom.preferredVolumes";
    volume = v1.toString() + "," + v2.toString() + "," + v3.toString();
    properties.put(propertyName, volume);

    // Create a table with the initial properties
    Connector connector = getConnector();
    String tableName = getUniqueNames(2)[0];
    connector.tableOperations().create(tableName, true, TimeType.MILLIS, properties);
    SortedSet<Text> partitions = new TreeSet<Text>();

    // Add 10 splits to the table
    for (String s : "b,e,g,j,l,o,q,t,v,y".split(","))
      partitions.add(new Text(s));
    connector.tableOperations().addSplits(tableName, partitions);
    // Write some data to the table
    BatchWriter bw = connector.createBatchWriter(tableName, new BatchWriterConfig());
    String[] rows = "a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z".split(",");
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
      assertEquals(rows[i++], entry.getKey().getRow().toString());
    }
    // Verify the new files are written to the different volumes
    scanner = connector.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    scanner.setRange(new Range("1", "1<"));
    scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
    int fileCount = 0;
    for (Entry<Key,Value> entry : scanner) {
      boolean inV1 = entry.getKey().getColumnQualifier().toString().contains(v1.toString());
      boolean inV2 = entry.getKey().getColumnQualifier().toString().contains(v2.toString());
      assertTrue(inV1 || inV2);
      fileCount++;
    }
    assertEquals(11, fileCount);

    // Replace the current chooser preferredVolumes property for table 2
    properties.remove("table.custom.chooser");
    propertyName = "table.custom.chooser";
    volume = "org.apache.accumulo.server.fs.StaticVolumeChooser";
    properties.put(propertyName, volume);

    properties.remove("table.custom.preferredVolumes");
    propertyName = "table.custom.preferredVolumes";
    volume = v1.toString();
    properties.put(propertyName, volume);

    // Create table 2 with the new properties
    String tableName2 = getUniqueNames(2)[1];
    connector.tableOperations().create(tableName2, true, TimeType.MILLIS, properties);

    // Add 10 splits to the table
    partitions = new TreeSet<Text>();
    for (String s : "b,e,g,j,l,o,q,t,v,y".split(","))
      partitions.add(new Text(s));
    connector.tableOperations().addSplits(tableName2, partitions);
    // Write some data to the table
    bw = connector.createBatchWriter(tableName2, new BatchWriterConfig());
    rows = "a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z".split(",");
    for (String s : rows) {
      Mutation m = new Mutation(new Text(s));
      m.put(EMPTY, EMPTY, EMPTY_VALUE);
      bw.addMutation(m);
    }
    bw.close();
    // Write the data to disk, read it back
    connector.tableOperations().flush(tableName2, null, null, true);
    scanner = connector.createScanner(tableName2, Authorizations.EMPTY);
    i = 0;
    for (Entry<Key,Value> entry : scanner) {
      assertEquals(rows[i++], entry.getKey().getRow().toString());
    }
    // Verify the new files are written to the different volumes
    scanner = connector.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    scanner.setRange(new Range("2", "2<"));
    scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
    fileCount = 0;
    for (Entry<Key,Value> entry : scanner) {
      boolean inV1 = entry.getKey().getColumnQualifier().toString().contains(v1.toString());
      assertTrue(inV1);
      fileCount++;
    }
    assertEquals(11, fileCount);
  }

  // Test that uses one table with 10 split points each. It uses the StaticVolumeChooser, but no preferred volume is specified. This means that the volume
  // is chosen randomly from all instance volumes.
  @Test(timeout = 60 * 1000)
  public void missingVolumeStaticVolumeChooser() throws Exception {
    log.info("Starting missingVolumeStaticVolumeChooser");

    // Create and populate initial properties map for creating table 1
    Map<String,String> properties = new HashMap<String,String>();
    String propertyName = "table.custom.chooser";
    String volume = "org.apache.accumulo.server.fs.StaticVolumeChooser";
    properties.put(propertyName, volume);

    // Create a table with the initial properties
    Connector connector = getConnector();
    String tableName = getUniqueNames(2)[0];
    connector.tableOperations().create(tableName, true, TimeType.MILLIS, properties);

    // Add 10 splits to the table
    SortedSet<Text> partitions = new TreeSet<Text>();
    for (String s : "b,e,g,j,l,o,q,t,v,y".split(","))
      partitions.add(new Text(s));
    connector.tableOperations().addSplits(tableName, partitions);
    // Write some data to the table
    BatchWriter bw = connector.createBatchWriter(tableName, new BatchWriterConfig());
    String[] rows = "a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z".split(",");
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
      assertEquals(rows[i++], entry.getKey().getRow().toString());
    }

    // Verify the new files are written to the Volumes specified
    scanner = connector.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    scanner.setRange(new Range("1", "1<"));
    scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
    int fileCount = 0;
    for (Entry<Key,Value> entry : scanner) {
      boolean inV1 = entry.getKey().getColumnQualifier().toString().contains(v1.toString());
      boolean inV2 = entry.getKey().getColumnQualifier().toString().contains(v2.toString());
      boolean inV4 = entry.getKey().getColumnQualifier().toString().contains(v4.toString());
      assertTrue(inV1 || inV2 || inV4);
      fileCount++;
    }
    assertEquals(11, fileCount);
  }

  // Test that uses one table with 10 split points each. It uses the RandomVolumeChooser, but no preferred volume is specified. This means that the volume
  // is chosen randomly from all instance volumes.
  @Test(timeout = 60 * 1000)
  public void missingVolumeRandomVolumeChooser() throws Exception {
    log.info("Starting missingVolumeRandomVolumeChooser");

    // Create and populate initial properties map for creating table 1
    Map<String,String> properties = new HashMap<String,String>();
    String propertyName = "table.custom.chooser";
    String volume = "org.apache.accumulo.server.fs.RandomVolumeChooser";
    properties.put(propertyName, volume);

    // Create a table with the initial properties
    Connector connector = getConnector();
    String tableName = getUniqueNames(2)[0];
    connector.tableOperations().create(tableName, true, TimeType.MILLIS, properties);

    // Add 10 splits to the table
    SortedSet<Text> partitions = new TreeSet<Text>();
    for (String s : "b,e,g,j,l,o,q,t,v,y".split(","))
      partitions.add(new Text(s));
    connector.tableOperations().addSplits(tableName, partitions);
    // Write some data to the table
    BatchWriter bw = connector.createBatchWriter(tableName, new BatchWriterConfig());
    String[] rows = "a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z".split(",");
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
      assertEquals(rows[i++], entry.getKey().getRow().toString());
    }

    // Verify the new files are written to the Volumes specified
    scanner = connector.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    scanner.setRange(new Range("1", "1<"));
    scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
    int fileCount = 0;
    for (Entry<Key,Value> entry : scanner) {
      boolean inV1 = entry.getKey().getColumnQualifier().toString().contains(v1.toString());
      boolean inV2 = entry.getKey().getColumnQualifier().toString().contains(v2.toString());
      boolean inV4 = entry.getKey().getColumnQualifier().toString().contains(v4.toString());
      assertTrue(inV1 || inV2 || inV4);
      fileCount++;
    }
    assertEquals(11, fileCount);
  }

  // Test that uses one table with 10 split points each. It uses the StaticVolumeChooser, but preferred volume is not an instance volume. This means that the
  // volume is chosen randomly from all instance volumes.
  @Test(timeout = 60 * 1000)
  public void notInstanceStaticVolumeChooser() throws Exception {
    log.info("Starting notInstanceStaticVolumeChooser");

    // Create and populate initial properties map for creating table 1
    Map<String,String> properties = new HashMap<String,String>();
    String propertyName = "table.custom.chooser";
    String volume = "org.apache.accumulo.server.fs.StaticVolumeChooser";
    properties.put(propertyName, volume);

    properties.remove("table.custom.preferredVolumes");
    propertyName = "table.custom.preferredVolumes";
    volume = v3.toString();
    properties.put(propertyName, volume);

    // Create a table with the initial properties
    Connector connector = getConnector();
    String tableName = getUniqueNames(2)[0];
    connector.tableOperations().create(tableName, true, TimeType.MILLIS, properties);

    // Add 10 splits to the table
    SortedSet<Text> partitions = new TreeSet<Text>();
    for (String s : "b,e,g,j,l,o,q,t,v,y".split(","))
      partitions.add(new Text(s));
    connector.tableOperations().addSplits(tableName, partitions);
    // Write some data to the table
    BatchWriter bw = connector.createBatchWriter(tableName, new BatchWriterConfig());
    String[] rows = "a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z".split(",");
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
      assertEquals(rows[i++], entry.getKey().getRow().toString());
    }

    // Verify the new files are written to the Volumes specified
    scanner = connector.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    scanner.setRange(new Range("1", "1<"));
    scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
    int fileCount = 0;
    for (Entry<Key,Value> entry : scanner) {
      boolean inV1 = entry.getKey().getColumnQualifier().toString().contains(v1.toString());
      boolean inV2 = entry.getKey().getColumnQualifier().toString().contains(v2.toString());
      boolean inV4 = entry.getKey().getColumnQualifier().toString().contains(v4.toString());
      assertTrue(inV1 || inV2 || inV4);
      fileCount++;
    }
    assertEquals(11, fileCount);
  }

  // Test that uses one table with 10 split points each. It uses the RandomVolumeChooser, but preferred volume is not an instance volume. This means that the
  // volume is chosen randomly from all instance volumes.
  @Test(timeout = 60 * 1000)
  public void notInstanceRandomVolumeChooser() throws Exception {
    log.info("Starting notInstanceRandomVolumeChooser");

    // Create and populate initial properties map for creating table 1
    Map<String,String> properties = new HashMap<String,String>();
    String propertyName = "table.custom.chooser";
    String volume = "org.apache.accumulo.server.fs.RandomVolumeChooser";
    properties.put(propertyName, volume);

    properties.remove("table.custom.preferredVolumes");
    propertyName = "table.custom.preferredVolumes";
    volume = v3.toString();
    properties.put(propertyName, volume);

    // Create a table with the initial properties
    Connector connector = getConnector();
    String tableName = getUniqueNames(2)[0];
    connector.tableOperations().create(tableName, true, TimeType.MILLIS, properties);

    // Add 10 splits to the table
    SortedSet<Text> partitions = new TreeSet<Text>();
    for (String s : "b,e,g,j,l,o,q,t,v,y".split(","))
      partitions.add(new Text(s));
    connector.tableOperations().addSplits(tableName, partitions);
    // Write some data to the table
    BatchWriter bw = connector.createBatchWriter(tableName, new BatchWriterConfig());
    String[] rows = "a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z".split(",");
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
      assertEquals(rows[i++], entry.getKey().getRow().toString());
    }

    // Verify the new files are written to the Volumes specified
    scanner = connector.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    scanner.setRange(new Range("1", "1<"));
    scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
    int fileCount = 0;
    for (Entry<Key,Value> entry : scanner) {
      boolean inV1 = entry.getKey().getColumnQualifier().toString().contains(v1.toString());
      boolean inV2 = entry.getKey().getColumnQualifier().toString().contains(v2.toString());
      boolean inV4 = entry.getKey().getColumnQualifier().toString().contains(v4.toString());
      assertTrue(inV1 || inV2 || inV4);
      fileCount++;
    }
    assertEquals(11, fileCount);
  }

  // Test that uses one table with 10 split points each. It does not specify a specific chooser, so the volume is chosen randomly from all instance volumes.
  @Test(timeout = 60 * 1000)
  public void chooserNotSpecified() throws Exception {
    log.info("Starting chooserNotSpecified");

    // Create a table with the initial properties
    Connector connector = getConnector();
    String tableName = getUniqueNames(2)[0];
    connector.tableOperations().create(tableName, true, TimeType.MILLIS);

    // Add 10 splits to the table
    SortedSet<Text> partitions = new TreeSet<Text>();
    for (String s : "b,e,g,j,l,o,q,t,v,y".split(","))
      partitions.add(new Text(s));
    connector.tableOperations().addSplits(tableName, partitions);
    // Write some data to the table
    BatchWriter bw = connector.createBatchWriter(tableName, new BatchWriterConfig());
    String[] rows = "a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z".split(",");
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
      assertEquals(rows[i++], entry.getKey().getRow().toString());
    }

    // Verify the new files are written to the Volumes specified
    scanner = connector.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    scanner.setRange(new Range("1", "1<"));
    scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
    int fileCount = 0;
    for (Entry<Key,Value> entry : scanner) {
      boolean inV1 = entry.getKey().getColumnQualifier().toString().contains(v1.toString());
      boolean inV2 = entry.getKey().getColumnQualifier().toString().contains(v2.toString());
      boolean inV4 = entry.getKey().getColumnQualifier().toString().contains(v4.toString());
      assertTrue(inV1 || inV2 || inV4);
      fileCount++;
    }
    assertEquals(11, fileCount);
  }

}
