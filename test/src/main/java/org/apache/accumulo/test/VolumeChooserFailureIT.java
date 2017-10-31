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

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.fs.PerTableVolumeChooser;
import org.apache.accumulo.server.fs.PreferredVolumeChooser;
import org.apache.accumulo.server.fs.VolumeChooserEnvironment.ChooserScope;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class VolumeChooserFailureIT extends ConfigurableMacBase {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private File volDirBase;
  private Path v1, v2, v3, v4;
  private static String[] rows = "a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z".split(",");
  private String namespace1;

  @Override
  protected int defaultTimeoutSeconds() {
    return 60;
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    // Get 2 tablet servers
    cfg.setNumTservers(2);
    namespace1 = "ns_" + getUniqueNames(1)[0];

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

    cfg.setSiteConfig(siteConfig);

    siteConfig.put(PerTableVolumeChooser.getPropertyNameForScope(ChooserScope.LOGGER), PreferredVolumeChooser.class.getName());
    // do not set preferred volumes
    cfg.setSiteConfig(siteConfig);

    // Only add volumes 1, 2, and 4 to the list of instance volumes to have one volume that isn't in the options list when they are choosing
    cfg.setProperty(Property.INSTANCE_VOLUMES, v1.toString() + "," + v2.toString() + "," + v4.toString());
    // no not set preferred volumes

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

  // Test that uses one table with 10 split points each. It uses the PreferredVolumeChooser, but no preferred volume is specified.
  // This means that the volume chooser will fail and no instance volumes will be assigned.
  @Test
  public void missingVolumePreferredVolumeChooser() throws Exception {
    log.info("Starting missingVolumePreferredVolumeChooser");

    // Create namespace
    Connector connector = getConnector();
    connector.namespaceOperations().create(namespace1);

    // Set properties on the namespace
    connector.namespaceOperations().setProperty(namespace1, PerTableVolumeChooser.TABLE_VOLUME_CHOOSER, PreferredVolumeChooser.class.getName());
    // deliberately do not set preferred volumes

    // Create table1 on namespace1 (will fail)
    String tableName = namespace1 + ".1";
    thrown.expect(AccumuloException.class);
    connector.tableOperations().create(tableName);
  }

  // Test that uses one table with 10 split points each. It uses the PreferredVolumeChooser, but preferred volume is not an instance volume.
  // This should fail.
  @Test
  public void notInstancePreferredVolumeChooser() throws Exception {
    log.info("Starting notInstancePreferredVolumeChooser");

    // Create namespace
    Connector connector = getConnector();
    connector.namespaceOperations().create(namespace1);

    // Set properties on the namespace
    String propertyName = PerTableVolumeChooser.TABLE_VOLUME_CHOOSER;
    String volume = PreferredVolumeChooser.class.getName();
    connector.namespaceOperations().setProperty(namespace1, propertyName, volume);

    // set to v3 which is not included in the list of instance volumes, so it should go to the
    // system default preferred volumes
    propertyName = PreferredVolumeChooser.TABLE_PREFERRED_VOLUMES;
    volume = v3.toString();
    connector.namespaceOperations().setProperty(namespace1, propertyName, volume);

    // Create table1 on namespace1 (will fail)
    String tableName = namespace1 + ".1";
    thrown.expect(AccumuloException.class);
    connector.tableOperations().create(tableName);
  }

}
