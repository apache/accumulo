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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

public class CleanTmpIT extends ConfigurableMacBase {
  private static final Logger log = LoggerFactory.getLogger(CleanTmpIT.class);

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
    cfg.setNumTservers(1);
    // use raw local file system so walogs sync and flush will work
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  @Test
  public void test() throws Exception {
    Connector c = getConnector();
    // make a table
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    // write to it
    BatchWriter bw = c.createBatchWriter(tableName, new BatchWriterConfig());
    Mutation m = new Mutation("row");
    m.put("cf", "cq", "value");
    bw.addMutation(m);
    bw.flush();

    // Compact memory to make a file
    c.tableOperations().compact(tableName, null, null, true, true);

    // Make sure that we'll have a WAL
    m = new Mutation("row2");
    m.put("cf", "cq", "value");
    bw.addMutation(m);
    bw.close();

    // create a fake _tmp file in its directory
    String id = c.tableOperations().tableIdMap().get(tableName);
    Scanner s = c.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    s.setRange(Range.prefix(id));
    s.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);
    Entry<Key,Value> entry = Iterables.getOnlyElement(s);
    Path file = new Path(entry.getKey().getColumnQualifier().toString());

    FileSystem fs = getCluster().getFileSystem();
    assertTrue("Could not find file: " + file, fs.exists(file));
    Path tabletDir = file.getParent();
    assertNotNull("Tablet dir should not be null", tabletDir);
    Path tmp = new Path(tabletDir, "junk.rf_tmp");
    // Make the file
    fs.create(tmp).close();
    log.info("Created tmp file {}", tmp.toString());
    getCluster().stop();
    getCluster().start();

    Scanner scanner = c.createScanner(tableName, Authorizations.EMPTY);
    assertEquals(2, Iterators.size(scanner.iterator()));
    // If we performed log recovery, we should have cleaned up any stray files
    assertFalse("File still exists: " + tmp, fs.exists(tmp));
  }
}
