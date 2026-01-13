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
package org.apache.accumulo.test.functional;

import static org.apache.accumulo.core.conf.Property.INSTANCE_CRYPTO_FACTORY;
import static org.apache.accumulo.core.spi.crypto.PerTableCryptoServiceFactory.RECOVERY_NAME_PROP;
import static org.apache.accumulo.core.spi.crypto.PerTableCryptoServiceFactory.TABLE_SERVICE_NAME_PROP;
import static org.apache.accumulo.core.spi.crypto.PerTableCryptoServiceFactory.WAL_NAME_PROP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.cluster.standalone.StandaloneAccumuloCluster;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.CloneConfiguration;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.OfflineScanner;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.crypto.AESCryptoService;
import org.apache.accumulo.core.spi.crypto.GenericCryptoServiceFactory;
import org.apache.accumulo.core.spi.crypto.PerTableCryptoServiceFactory;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.log.WalStateManager;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.accumulo.tserver.logger.LogReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerTableCryptoIT extends AccumuloClusterHarness {
  private final static Logger log = LoggerFactory.getLogger(PerTableCryptoIT.class);
  private final String NO_ENCRYPT_STRING = "No on disk encryption detected";
  private final String ENCRYPTED_STRING = "Encrypted with Params:";

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    String keyPath =
        System.getProperty("user.dir") + "/target/mini-tests/PerTableCrypto-testkeyfile";
    cfg.setProperty(INSTANCE_CRYPTO_FACTORY, PerTableCryptoServiceFactory.class.getName());
    cfg.setProperty(WAL_NAME_PROP, AESCryptoService.class.getName());
    cfg.setProperty(GenericCryptoServiceFactory.GENERAL_SERVICE_NAME_PROP,
        AESCryptoService.class.getName());
    cfg.setProperty(RECOVERY_NAME_PROP, AESCryptoService.class.getName());
    cfg.setProperty(AESCryptoService.KEY_URI_PROPERTY, keyPath);

    // setup key file
    try {
      Path keyFile = new Path(keyPath);
      FileSystem fs = FileSystem.getLocal(new Configuration());
      fs.delete(keyFile, true);
      if (fs.createNewFile(keyFile)) {
        log.info("Created keyfile at {}", keyPath);
      } else {
        log.error("Failed to create key file at {}", keyPath);
      }

      try (FSDataOutputStream out = fs.create(keyFile)) {
        out.writeUTF("sixteenbytekey"); // 14 + 2 from writeUTF
      }
    } catch (Exception e) {
      log.error("Exception during configure", e);
    }
  }

  @Test
  public void testMultipleTablesAndWALs() throws Exception {
    var tables = getUniqueNames(3);
    Map<String,String> props = new HashMap<>();
    NewTableConfiguration tableConfig = new NewTableConfiguration();
    TableId tableId1;
    TableId tableId2;

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      // table 0 plain text but WAL should still be encrypted
      c.tableOperations().create(tables[0]);
      TableId tableId0 = TableId.of(c.tableOperations().tableIdMap().get(tables[0]));
      VerifyIngest.VerifyParams params = new VerifyIngest.VerifyParams(getClientProps(), tables[0]);
      TestIngest.ingest(c, params);

      checkWALEncryption();

      VerifyIngest.verifyIngest(c, params);

      // verify table data was not encrypted
      checkTableEncryption(tableId0, false);

      // encrypt table 1
      props.put(TABLE_SERVICE_NAME_PROP, AESCryptoService.class.getName());
      tableConfig.setProperties(props);
      c.tableOperations().create(tables[1], tableConfig);
      params = new VerifyIngest.VerifyParams(getClientProps(), tables[1]);
      TestIngest.ingest(c, params);
      VerifyIngest.verifyIngest(c, params);

      // clone encrypted table to table 2
      var cloneConfig = CloneConfiguration.builder().setFlush(true).build();
      c.tableOperations().clone(tables[1], tables[2], cloneConfig);

      c.tableOperations().compact(tables[2], new CompactionConfig());
      params = new VerifyIngest.VerifyParams(getClientProps(), tables[2]);
      VerifyIngest.verifyIngest(c, params);

      tableId1 = TableId.of(c.tableOperations().tableIdMap().get(tables[1]));
      tableId2 = TableId.of(c.tableOperations().tableIdMap().get(tables[2]));
    }

    // verify files were encrypted in tables 1 and 2
    checkTableEncryption(tableId1, true);
    checkTableEncryption(tableId2, true);
  }

  /**
   * Test that the OfflineIterator works with crypto
   */
  @Test
  public void testOfflineIterator() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      Map<String,String> props = new HashMap<>();
      NewTableConfiguration tableConfig = new NewTableConfiguration();
      props.put(TABLE_SERVICE_NAME_PROP, AESCryptoService.class.getName());

      tableConfig.setProperties(props);
      c.tableOperations().create(tableName, tableConfig);
      var params = new VerifyIngest.VerifyParams(getClientProps(), tableName);
      TestIngest.ingest(c, params);
      VerifyIngest.verifyIngest(c, params);
      TableId tableId = TableId.of(c.tableOperations().tableIdMap().get(tableName));
      c.tableOperations().offline(tableName, true);

      try (var oScanner = new OfflineScanner((ClientContext) c, tableId, Authorizations.EMPTY)) {
        long count = oScanner.stream().count();
        assertEquals(count, 100_000);
      }
    }
  }

  private void checkTableEncryption(TableId tableId, boolean expectEncrypt) throws Exception {
    try (var tm = getServerContext().getAmple().readTablets().forTable(tableId).build()) {
      for (var tabletMetadata : tm) {
        for (var f : tabletMetadata.getFiles()) {
          if (!getFileSystem().exists(f.getPath())) {
            continue;
          }
          ByteArrayOutputStream baos = new ByteArrayOutputStream();
          PrintStream oldOut = System.out;
          try (PrintStream newOut = new PrintStream(baos)) {
            System.setOut(newOut);
            List<String> args = new ArrayList<>();
            args.add(f.getPathStr());
            args.add("--props");
            args.add(getCluster().getAccumuloPropertiesPath());
            if (getClusterType() == ClusterType.STANDALONE && saslEnabled()) {
              args.add("--config");
              StandaloneAccumuloCluster sac = (StandaloneAccumuloCluster) cluster;
              String hadoopConfDir = sac.getHadoopConfDir();
              args.add(new Path(hadoopConfDir, "core-site.xml").toString());
              args.add(new Path(hadoopConfDir, "hdfs-site.xml").toString());
            }
            args.add("-o");
            args.add(TABLE_SERVICE_NAME_PROP + "=" + AESCryptoService.class.getName());
            args.add("-o");
            args.add(INSTANCE_CRYPTO_FACTORY + "=" + GenericCryptoServiceFactory.class.getName());
            log.info("Invoking PrintInfo with {}", args);
            org.apache.accumulo.core.file.rfile.PrintInfo.main(args.toArray(new String[0]));
            newOut.flush();
            String stdout = baos.toString();
            if (expectEncrypt) {
              assertTrue(stdout.contains(ENCRYPTED_STRING));
            } else {
              assertTrue(stdout.contains(NO_ENCRYPT_STRING));
            }
          } finally {
            System.setOut(oldOut);
          }
        }
      }
    }
  }

  private void checkWALEncryption() throws Exception {
    Set<String> walsSeen = new HashSet<>();
    int open = 0;
    int attempts = 0;
    boolean foundWal = false;
    while (open == 0) {
      attempts++;
      Map<String,WalStateManager.WalState> wals = WALSunnyDayIT._getWals(getServerContext());
      for (var entry : wals.entrySet()) {
        if (entry.getValue() == WalStateManager.WalState.OPEN) {
          open++;
          walsSeen.add(entry.getKey());
          foundWal = true;
        } else {
          // log CLOSED or UNREFERENCED to help debug this test
          log.debug("The WalState for {} is {}", entry.getKey(), entry.getValue());
        }
      }

      if (!foundWal) {
        Thread.sleep(50);
        if (attempts % 50 == 0) {
          log.debug("No open WALs found in {} attempts.", attempts);
        }
      }
    }

    assertFalse(walsSeen.isEmpty(), "Did not see any WALs");

    // verify that at least one of the WALs can be read by the crypto service
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream oldOut = System.out;
    try (PrintStream newOut = new PrintStream(baos)) {
      System.setOut(newOut);
      List<String> args = new ArrayList<>(walsSeen);
      args.add("-p");
      args.add(getCluster().getAccumuloPropertiesPath());
      args.add("-e");
      new LogReader().execute(args.toArray(new String[0]));
      newOut.flush();
      String stdout = baos.toString();
      assertTrue(stdout.contains(AESCryptoService.class.getName()));
    } finally {
      System.setOut(oldOut);
    }
  }
}
