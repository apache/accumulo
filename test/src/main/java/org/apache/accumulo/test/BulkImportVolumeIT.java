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

import java.io.File;
import java.time.Duration;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// ACCUMULO-118/ACCUMULO-2504
public class BulkImportVolumeIT extends AccumuloClusterHarness {
  private static final Logger log = LoggerFactory.getLogger(BulkImportVolumeIT.class);

  File volDirBase = null;
  Path v1, v2;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(1);
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    File baseDir = cfg.getDir();
    volDirBase = new File(baseDir, "volumes");
    File v1f = new File(volDirBase, "v1");
    File v2f = new File(volDirBase, "v2");
    v1 = new Path("file://" + v1f.getAbsolutePath());
    v2 = new Path("file://" + v2f.getAbsolutePath());

    // Run MAC on two locations in the local file system
    cfg.setProperty(Property.INSTANCE_VOLUMES, v1 + "," + v2);

    // use raw local file system so walogs sync and flush will work
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  // suppress importDirectory deprecated since this tests legacy failure directory
  @SuppressWarnings("deprecation")
  @Test
  public void testBulkImportFailure() throws Exception {
    String tableName = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(tableName);
      FileSystem fs = getFileSystem();
      Path rootPath = new Path(cluster.getTemporaryPath(), getClass().getName());
      fs.deleteOnExit(rootPath);

      Path bulk = new Path(rootPath, "bulk");
      fs.deleteOnExit(bulk);
      log.info("bulk: {}", bulk);
      if (fs.exists(bulk)) {
        fs.delete(bulk, true);
      }
      assertTrue(fs.mkdirs(bulk));
      Path err = new Path(rootPath, "err");
      fs.deleteOnExit(err);
      log.info("err: {}", err);
      if (fs.exists(err)) {
        fs.delete(err, true);
      }
      assertTrue(fs.mkdirs(err));
      Path bogus = new Path(bulk, "bogus.rf");
      fs.deleteOnExit(bogus);
      fs.create(bogus).close();
      log.info("bogus: {}", bogus);
      assertTrue(fs.exists(bogus));
      log.info("Importing {} into {} with failures directory {}", bulk, tableName, err);
      client.tableOperations().importDirectory(tableName, bulk.toString(), err.toString(), false);
      assertEquals(1, fs.listStatus(err).length);
    }
  }

}
