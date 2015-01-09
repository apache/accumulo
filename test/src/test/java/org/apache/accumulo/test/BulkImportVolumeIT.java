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

import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.harness.AccumuloClusterIT;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// ACCUMULO-118/ACCUMULO-2504
public class BulkImportVolumeIT extends AccumuloClusterIT {
  private static final Logger log = LoggerFactory.getLogger(BulkImportVolumeIT.class);

  File volDirBase = null;
  Path v1, v2;

  @Override
  public int defaultTimeoutSeconds() {
    return 60;
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    File baseDir = cfg.getDir();
    volDirBase = new File(baseDir, "volumes");
    File v1f = new File(volDirBase, "v1");
    File v2f = new File(volDirBase, "v2");
    v1f.mkdir();
    v2f.mkdir();
    v1 = new Path("file://" + v1f.getAbsolutePath());
    v2 = new Path("file://" + v2f.getAbsolutePath());

    // Run MAC on two locations in the local file system
    cfg.setProperty(Property.INSTANCE_VOLUMES, v1.toString() + "," + v2.toString());

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
    Path bogus = new Path(bulk, "bogus.rf");
    fs.create(bogus).close();
    log.info("bogus: {}", bogus);
    assertTrue(fs.exists(bogus));
    to.importDirectory(tableName, bulk.toString(), err.toString(), false);
    assertEquals(1, fs.listStatus(err).length);
  }

}
