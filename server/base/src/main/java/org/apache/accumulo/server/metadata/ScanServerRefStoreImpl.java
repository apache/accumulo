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
package org.apache.accumulo.server.metadata;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.metadata.ScanServerRefStore;
import org.apache.accumulo.core.metadata.ScanServerRefTabletFile;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScanServerRefStoreImpl implements ScanServerRefStore {

  private static Logger log = LoggerFactory.getLogger(ScanServerRefStoreImpl.class);

  private final ClientContext context;
  private final String tableName;

  public ScanServerRefStoreImpl(ClientContext context, String tableName) {
    this.context = context;
    this.tableName = tableName;
  }

  @Override
  public void put(Collection<ScanServerRefTabletFile> scanRefs) {
    try (BatchWriter writer = context.createBatchWriter(tableName)) {
      for (ScanServerRefTabletFile ref : scanRefs) {
        Mutation m = new Mutation(ref.getRow());
        m.put(ref.getServerAddress(), ref.getServerLockUUID(), ref.getValue());
        writer.addMutation(m);
      }
    } catch (MutationsRejectedException | TableNotFoundException e) {
      throw new IllegalStateException(
          "Error inserting scan server file references into " + tableName, e);
    }
  }

  @Override
  public Stream<ScanServerRefTabletFile> list() {
    try {
      Scanner scanner = context.createScanner(tableName, Authorizations.EMPTY);
      return scanner.stream().onClose(scanner::close)
          .map(e -> new ScanServerRefTabletFile(e.getKey().getRowData().toString(),
              e.getKey().getColumnFamily(), e.getKey().getColumnQualifier()));
    } catch (TableNotFoundException e) {
      throw new IllegalStateException(tableName + " not found!", e);
    }
  }

  @Override
  public void delete(String serverAddress, UUID scanServerLockUUID) {
    Objects.requireNonNull(serverAddress, "Server address must be supplied");
    Objects.requireNonNull(scanServerLockUUID, "Server uuid must be supplied");
    try (Scanner scanner = context.createScanner(tableName, Authorizations.EMPTY)) {
      scanner.fetchColumn(new Text(serverAddress), new Text(scanServerLockUUID.toString()));

      Set<ScanServerRefTabletFile> refsToDelete = StreamSupport.stream(scanner.spliterator(), false)
          .map(e -> new ScanServerRefTabletFile(e.getKey().getRowData().toString(),
              e.getKey().getColumnFamily(), e.getKey().getColumnQualifier()))
          .collect(Collectors.toSet());

      if (!refsToDelete.isEmpty()) {
        this.delete(refsToDelete);
      }
    } catch (TableNotFoundException e) {
      throw new IllegalStateException(tableName + " not found!", e);
    }
  }

  @Override
  public void delete(Collection<ScanServerRefTabletFile> refsToDelete) {
    try (BatchWriter writer = context.createBatchWriter(tableName)) {
      for (ScanServerRefTabletFile ref : refsToDelete) {
        Mutation m = new Mutation(ref.getRow());
        m.putDelete(ref.getServerAddress(), ref.getServerLockUUID());
        writer.addMutation(m);
      }
      log.debug("Deleted scan server file reference entries for files: {}", refsToDelete);
    } catch (MutationsRejectedException | TableNotFoundException e) {
      throw new IllegalStateException(e);
    }
  }
}
