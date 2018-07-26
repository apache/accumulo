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

import static org.apache.accumulo.core.conf.Property.INSTANCE_CRYPTO_PREFIX;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteAheadLogEncryptedIT extends AccumuloClusterHarness {

  private static final Logger log = LoggerFactory.getLogger(WriteAheadLogEncryptedIT.class);

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    String keyPath = System.getProperty("user.dir")
        + "/target/mini-tests/WriteAheadLogEncryptedIT-testkeyfile";
    cfg.setProperty(Property.INSTANCE_CRYPTO_SERVICE,
        "org.apache.accumulo.core.security.crypto.impl.AESCryptoService");
    cfg.setProperty(INSTANCE_CRYPTO_PREFIX.getKey() + "kekId", keyPath);
    cfg.setProperty(INSTANCE_CRYPTO_PREFIX.getKey() + "keyManager", "uri");

    cfg.setProperty(Property.TSERV_WALOG_MAX_SIZE, "2M");
    cfg.setProperty(Property.GC_CYCLE_DELAY, "1");
    cfg.setProperty(Property.GC_CYCLE_START, "1");
    cfg.setProperty(Property.MASTER_RECOVERY_DELAY, "1s");
    cfg.setProperty(Property.TSERV_MAJC_DELAY, "1");
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());

    // setup key file
    try {
      Path keyFile = new Path(keyPath);
      FileSystem fs = FileSystem.getLocal(CachedConfiguration.getInstance());
      fs.delete(keyFile, true);
      if (fs.createNewFile(keyFile))
        log.info("Created keyfile at {}", keyPath);
      else
        log.error("Failed to create key file at {}", keyPath);

      try (FSDataOutputStream out = fs.create(keyFile)) {
        out.writeUTF("sixteenbytekey"); // 14 + 2 from writeUTF
      }
    } catch (Exception e) {
      log.error("Exception during configure", e);
    }
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 10 * 60;
  }

  @Test
  public void test() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    c.tableOperations().setProperty(tableName, Property.TABLE_SPLIT_THRESHOLD.getKey(), "750K");
    TestIngest.Opts opts = new TestIngest.Opts();
    VerifyIngest.Opts vopts = new VerifyIngest.Opts();
    opts.setTableName(tableName);
    opts.setClientInfo(getClientInfo());
    vopts.setClientInfo(getClientInfo());

    TestIngest.ingest(c, opts, new BatchWriterOpts());
    vopts.setTableName(tableName);
    VerifyIngest.verifyIngest(c, vopts, new ScannerOpts());
    getCluster().getClusterControl().stopAllServers(ServerType.TABLET_SERVER);
    getCluster().getClusterControl().startAllServers(ServerType.TABLET_SERVER);
    VerifyIngest.verifyIngest(c, vopts, new ScannerOpts());
  }

}
