/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test.functional;

import static org.apache.accumulo.core.conf.Property.TSERV_WALOG_CRYPTO_PREFIX;
import static org.apache.accumulo.test.functional.WriteAheadLogIT.testWAL;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteAheadLogEncryptedIT extends AccumuloClusterHarness {

  private static final Logger log = LoggerFactory.getLogger(WriteAheadLogEncryptedIT.class);

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    String keyPath =
        System.getProperty("user.dir") + "/target/mini-tests/WriteAheadLogEncryptedIT-testkeyfile";
    cfg.setProperty(Property.TSERV_WALOG_CRYPTO_ENCRYPT_SERVICE,
        "org.apache.accumulo.core.spi.crypto.AESCryptoService");
    cfg.setProperty(TSERV_WALOG_CRYPTO_PREFIX.getKey() + "key.uri", keyPath);

    WriteAheadLogIT.setupConfig(cfg, hadoopCoreSite);

    // setup key file
    try {
      Path keyFile = new Path(keyPath);
      FileSystem fs = FileSystem.getLocal(new Configuration());
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
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      testWAL(c, getUniqueNames(1)[0]);
    }
  }

}
