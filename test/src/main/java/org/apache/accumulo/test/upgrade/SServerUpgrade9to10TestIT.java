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
package org.apache.accumulo.test.upgrade;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.metadata.ScanServerRefTabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.manager.upgrade.Upgrader9to10;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.miniclusterImpl.ProcessNotFoundException;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.Test;

public class SServerUpgrade9to10TestIT extends ConfigurableMacBase {

  private static final String OUR_SECRET = "itsreallysecret";
  private static final Upgrader9to10 upgrader = new Upgrader9to10();

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(5);
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
    cfg.setProperty(Property.INSTANCE_SECRET, OUR_SECRET);
    cfg.setProperty(Property.GC_CYCLE_START, "1000"); // gc will be killed before it is run

    // use raw local file system so walogs sync and flush will work
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  private void killMacGc() throws ProcessNotFoundException, InterruptedException, KeeperException {
    // kill gc started by MAC
    getCluster().killProcess(ServerType.GARBAGE_COLLECTOR,
        getCluster().getProcesses().get(ServerType.GARBAGE_COLLECTOR).iterator().next());
    // delete lock in zookeeper if there, this will allow next GC to start quickly
    var path = ServiceLock.path(getServerContext().getZooKeeperRoot() + Constants.ZGC_LOCK);
    ZooReaderWriter zk = getServerContext().getZooReaderWriter();
    try {
      ServiceLock.deleteLock(zk, path);
    } catch (IllegalStateException e) {
      log.error("Unable to delete ZooLock for mini accumulo-gc", e);
    }

    assertNull(getCluster().getProcesses().get(ServerType.GARBAGE_COLLECTOR));
  }

  @Test
  public void sserverRemoveMetadataTableRefsIT() throws Exception {
    deleteScanServerRange(Ample.DataLevel.METADATA);
  }

  @Test
  public void sserverRemoveUserTableRefsIT() throws Exception {
    deleteScanServerRange(Ample.DataLevel.USER);
  }

  public void deleteScanServerRange(Ample.DataLevel level) throws Exception {
    killMacGc();// we do not want anything deleted

    var table = level.metaTable();
    HostAndPort server = HostAndPort.fromParts("127.0.0.1", 1234);
    UUID serverLockUUID = UUID.randomUUID();

    log.info("Testing removal of scan server refs for upgrade {}", table);
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      client.securityOperations().grantTablePermission(client.whoami(), table,
          TablePermission.WRITE);

      try (BatchWriter writer = client.createBatchWriter(table)) {
        String prefix = MetadataSchema.ScanServerFileReferenceSection.getRowPrefix();
        Set<ScanServerRefTabletFile> scanRefs =
            Stream.of("F0001370.rf", "F0001371.rf", "F0001270.rf", "F0001271.rf")
                .map(f -> "hdfs://localhost:8020/accumulo/tables/2a/test_tablet/" + f)
                .map(f -> new ScanServerRefTabletFile(serverLockUUID, server.toString(), f))
                .collect(Collectors.toSet());

        for (ScanServerRefTabletFile scanRef : scanRefs) {
          Mutation m = new Mutation(prefix + scanRef.getRowSuffix());
          m.put(scanRef.getServerAddress().toString(), scanRef.getFilePath().toString(), "");
          writer.addMutation(m);
        }
        for (String filepath : Stream.of("F0001243.rf", "F0006512.rf", "F00452.rf", "F0002345.rf")
            .map(f -> "hdfs://localhost:8020/accumulo/tables/1a/test_tablet/" + f)
            .collect(Collectors.toSet())) {
          Mutation m = new Mutation(prefix + filepath);
          m.put(server.toString(), serverLockUUID.toString(), "");
          writer.addMutation(m);
        }

        writer.flush();
      } catch (MutationsRejectedException | TableNotFoundException e) {
        throw new IllegalStateException("Error inserting scan server file references into " + table,
            e);
      }

      try (Scanner scanner = client.createScanner(table, Authorizations.EMPTY)) {
        scanner.setRange(MetadataSchema.ScanServerFileReferenceSection.getRange());
        assertEquals(8, scanner.stream().count());
        upgrader.removeAllScanServerRefs(getServerContext(), level);
        assertEquals(0, scanner.stream().count());
      } catch (TableNotFoundException e) {
        throw new RuntimeException(e);
      }

    } catch (AccumuloException | AccumuloSecurityException e) {
      throw new RuntimeException(e);
    }
  }

}
