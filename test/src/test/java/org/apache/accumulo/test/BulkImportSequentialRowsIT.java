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

import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterIT;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

// ACCUMULO-3967
public class BulkImportSequentialRowsIT extends AccumuloClusterIT {
  private static final Logger log = LoggerFactory.getLogger(BulkImportSequentialRowsIT.class);

  @Override
  public int defaultTimeoutSeconds() {
    return 60;
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    // Need more than one tserver
    cfg.setNumTservers(2);

    // use raw local file system so walogs sync and flush will work
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  @Test
  public void testBulkImportFailure() throws Exception {
    String tableName = getUniqueNames(1)[0];
    TableOperations to = getConnector().tableOperations();
    to.create(tableName);
    FileSystem fs = getFileSystem();
    String rootPath = getUsableDir();
    Path bulk = new Path(rootPath, "bulk");
    log.info("bulk: {}", bulk);
    if (fs.exists(bulk)) {
      fs.delete(bulk, true);
    }
    assertTrue(fs.mkdirs(bulk));
    Path err = new Path(rootPath, "err");
    log.info("err: {}", err);
    if (fs.exists(err)) {
      fs.delete(err, true);
    }
    assertTrue(fs.mkdirs(err));

    Path rfile = new Path(bulk, "file.rf");
    FileSKVWriter writer = FileOperations.getInstance().openWriter(rfile.toString(), fs, new Configuration(), DefaultConfiguration.getInstance());
    writer.startDefaultLocalityGroup();

    final Value emptyValue = new Value(new byte[0]);
    final SortedSet<Text> splits = new TreeSet<Text>();
    for (int i = 0; i < 100; i++) {
      String row = String.format("%03d", i);
      splits.add(new Text(row));
      writer.append(new Key(row, "", ""), emptyValue);
      for (int j = 0; j < 100; j++) {
        writer.append(new Key(row, "", String.format("%03d", j)), emptyValue);
      }
    }
    writer.close();

    assertTrue(fs.exists(rfile));

    // Add some splits
    to.addSplits(tableName, splits);

    // Then import a single rfile to all the tablets, hoping that we get a failure to import because of the balancer moving tablets around
    // and then we get to verify that the bug is actually fixed.
    to.importDirectory(tableName, bulk.toString(), err.toString(), false);

    // The bug is that some tablets don't get imported into.
    assertEquals(10100, Iterables.size(getConnector().createScanner(tableName, Authorizations.EMPTY)));
  }

}
