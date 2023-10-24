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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.metadata.RootTable.ZROOT_TABLET_GC_CANDIDATES;
import static org.apache.accumulo.server.util.MetadataTableUtil.EMPTY_TEXT;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.gc.GcCandidate;
import org.apache.accumulo.core.gc.ReferenceFile;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.ScanServerRefTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.ValidationUtil;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.AmpleImpl;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionFinalState;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.BlipSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.DeletesSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.DeletesSection.SkewedKeyValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.ExternalCompactionSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.ScanServerFileReferenceSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.BulkFileColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class ServerAmpleImpl extends AmpleImpl implements Ample {

  private static Logger log = LoggerFactory.getLogger(ServerAmpleImpl.class);

  private ServerContext context;

  public ServerAmpleImpl(ServerContext context) {
    super(context);
    this.context = context;
  }

  @Override
  public Ample.TabletMutator mutateTablet(KeyExtent extent) {
    TabletsMutator tmi = mutateTablets();
    Ample.TabletMutator tabletMutator = tmi.mutateTablet(extent);
    ((TabletMutatorBase) tabletMutator).setCloseAfterMutate(tmi);
    return tabletMutator;
  }

  @Override
  public TabletsMutator mutateTablets() {
    return new TabletsMutatorImpl(context);
  }

  private void mutateRootGcCandidates(Consumer<RootGcCandidates> mutator) {
    String zpath = context.getZooKeeperRoot() + ZROOT_TABLET_GC_CANDIDATES;
    try {
      context.getZooReaderWriter().mutateOrCreate(zpath, new byte[0], currVal -> {
        String currJson = new String(currVal, UTF_8);
        RootGcCandidates rgcc = new RootGcCandidates(currJson);
        log.debug("Root GC candidates before change : {}", currJson);
        mutator.accept(rgcc);
        String newJson = rgcc.toJson();
        log.debug("Root GC candidates after change  : {}", newJson);
        if (newJson.length() > 262_144) {
          log.warn(
              "Root tablet deletion candidates stored in ZK at {} are getting large ({} bytes), is"
                  + " Accumulo GC process running?  Large nodes may cause problems for Zookeeper!",
              zpath, newJson.length());
        }
        return newJson.getBytes(UTF_8);
      });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void putGcCandidates(TableId tableId, Collection<StoredTabletFile> candidates) {

    if (RootTable.ID.equals(tableId)) {
      mutateRootGcCandidates(rgcc -> rgcc.add(candidates.stream()));
      return;
    }

    try (BatchWriter writer = context.createBatchWriter(DataLevel.of(tableId).metaTable())) {
      for (StoredTabletFile file : candidates) {
        writer.addMutation(createDeleteMutation(file));
      }
    } catch (MutationsRejectedException | TableNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void putGcFileAndDirCandidates(TableId tableId, Collection<ReferenceFile> candidates) {

    if (DataLevel.of(tableId) == DataLevel.ROOT) {
      // Directories are unexpected for the root tablet, so convert to stored tablet file
      mutateRootGcCandidates(rgcc -> rgcc.add(candidates.stream()
          .map(reference -> new StoredTabletFile(reference.getMetadataEntry()))));
      return;
    }

    try (BatchWriter writer = context.createBatchWriter(DataLevel.of(tableId).metaTable())) {
      for (var fileOrDir : candidates) {
        writer.addMutation(createDeleteMutation(fileOrDir));
      }
    } catch (MutationsRejectedException | TableNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void addBulkLoadInProgressFlag(String path, long fateTxid) {

    // Bulk Import operations are not supported on the metadata table, so no entries will ever be
    // required on the root table.
    Mutation m = new Mutation(BlipSection.getRowPrefix() + path);
    m.put(EMPTY_TEXT, EMPTY_TEXT, new Value(FateTxId.formatTid(fateTxid)));

    try (BatchWriter bw = context.createBatchWriter(MetadataTable.NAME)) {
      bw.addMutation(m);
    } catch (MutationsRejectedException | TableNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void removeBulkLoadInProgressFlag(String path) {

    // Bulk Import operations are not supported on the metadata table, so no entries will ever be
    // required on the root table.
    Mutation m = new Mutation(BlipSection.getRowPrefix() + path);
    m.putDelete(EMPTY_TEXT, EMPTY_TEXT);

    try (BatchWriter bw = context.createBatchWriter(MetadataTable.NAME)) {
      bw.addMutation(m);
    } catch (MutationsRejectedException | TableNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void removeBulkLoadEntries(TableId tableId, long tid, Text firstSplit, Text lastSplit) {
    Preconditions.checkArgument(DataLevel.of(tableId) == DataLevel.USER);
    try (
        Scanner mscanner =
            new IsolatedScanner(context.createScanner(MetadataTable.NAME, Authorizations.EMPTY));
        BatchWriter bw = context.createBatchWriter(MetadataTable.NAME)) {
      mscanner.setRange(new KeyExtent(tableId, lastSplit, firstSplit).toMetaRange());
      mscanner.fetchColumnFamily(BulkFileColumnFamily.NAME);

      for (Map.Entry<Key,Value> entry : mscanner) {
        log.trace("Looking at entry {} with tid {}", entry, tid);
        long entryTid = BulkFileColumnFamily.getBulkLoadTid(entry.getValue());
        if (tid == entryTid) {
          log.trace("deleting entry {}", entry);
          Key key = entry.getKey();
          Mutation m = new Mutation(key.getRow());
          m.putDelete(key.getColumnFamily(), key.getColumnQualifier());
          bw.addMutation(m);
        }
      }
    } catch (MutationsRejectedException | TableNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void deleteGcCandidates(DataLevel level, Collection<GcCandidate> candidates,
      GcCandidateType type) {

    if (level == DataLevel.ROOT) {
      if (type == GcCandidateType.INUSE) {
        // Since there is only a single root tablet, supporting INUSE candidate deletions would add
        // additional code complexity without any substantial benefit.
        // Therefore, deletion of root INUSE candidates is not supported.
        return;
      }
      mutateRootGcCandidates(rgcc -> rgcc.remove(candidates.stream()));
      return;
    }

    try (BatchWriter writer = context.createBatchWriter(level.metaTable())) {
      for (GcCandidate candidate : candidates) {
        Mutation m = new Mutation(DeletesSection.encodeRow(candidate.getPath()));
        m.putDelete(EMPTY_TEXT, EMPTY_TEXT, candidate.getUid());
        writer.addMutation(m);
      }
    } catch (MutationsRejectedException | TableNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Iterator<GcCandidate> getGcCandidates(DataLevel level) {
    if (level == DataLevel.ROOT) {
      var zooReader = context.getZooReader();
      byte[] jsonBytes;
      try {
        jsonBytes =
            zooReader.getData(context.getZooKeeperRoot() + RootTable.ZROOT_TABLET_GC_CANDIDATES);
      } catch (KeeperException | InterruptedException e) {
        throw new RuntimeException(e);
      }
      return new RootGcCandidates(new String(jsonBytes, UTF_8)).sortedStream().iterator();
    } else if (level == DataLevel.METADATA || level == DataLevel.USER) {
      Range range = DeletesSection.getRange();

      Scanner scanner;
      try {
        scanner = context.createScanner(level.metaTable(), Authorizations.EMPTY);
      } catch (TableNotFoundException e) {
        throw new RuntimeException(e);
      }
      scanner.setRange(range);
      return scanner.stream().filter(entry -> entry.getValue().equals(SkewedKeyValue.NAME))
          .map(
              entry -> new GcCandidate(DeletesSection.decodeRow(entry.getKey().getRow().toString()),
                  entry.getKey().getTimestamp()))
          .iterator();
    } else {
      throw new IllegalArgumentException();
    }
  }

  @Override
  public Mutation createDeleteMutation(ReferenceFile tabletFilePathToRemove) {
    return createDelMutation(ValidationUtil.validate(tabletFilePathToRemove).getMetadataEntry());
  }

  public Mutation createDeleteMutation(StoredTabletFile pathToRemove) {
    return createDelMutation(pathToRemove.getMetaUpdateDelete());
  }

  private Mutation createDelMutation(String path) {
    Mutation delFlag = new Mutation(new Text(DeletesSection.encodeRow(path)));
    delFlag.put(EMPTY_TEXT, EMPTY_TEXT, DeletesSection.SkewedKeyValue.NAME);
    return delFlag;
  }

  @Override
  public void
      putExternalCompactionFinalStates(Collection<ExternalCompactionFinalState> finalStates) {
    try (BatchWriter writer = context.createBatchWriter(DataLevel.USER.metaTable())) {
      String prefix = ExternalCompactionSection.getRowPrefix();
      for (ExternalCompactionFinalState finalState : finalStates) {
        Mutation m = new Mutation(prefix + finalState.getExternalCompactionId().canonical());
        m.put("", "", finalState.toJson());
        writer.addMutation(m);
      }
    } catch (MutationsRejectedException | TableNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Stream<ExternalCompactionFinalState> getExternalCompactionFinalStates() {
    Scanner scanner;
    try {
      scanner = context.createScanner(DataLevel.USER.metaTable(), Authorizations.EMPTY);
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }

    scanner.setRange(ExternalCompactionSection.getRange());
    int pLen = ExternalCompactionSection.getRowPrefix().length();
    return scanner.stream()
        .map(e -> ExternalCompactionFinalState.fromJson(
            ExternalCompactionId.of(e.getKey().getRowData().toString().substring(pLen)),
            e.getValue().toString()));
  }

  @Override
  public void
      deleteExternalCompactionFinalStates(Collection<ExternalCompactionId> statusesToDelete) {
    try (BatchWriter writer = context.createBatchWriter(DataLevel.USER.metaTable())) {
      String prefix = ExternalCompactionSection.getRowPrefix();
      for (ExternalCompactionId ecid : statusesToDelete) {
        Mutation m = new Mutation(prefix + ecid.canonical());
        m.putDelete(EMPTY_TEXT, EMPTY_TEXT);
        writer.addMutation(m);
      }
      log.debug("Deleted external compaction final state entries for external compactions: {}",
          statusesToDelete);
    } catch (MutationsRejectedException | TableNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void putScanServerFileReferences(Collection<ScanServerRefTabletFile> scanRefs) {
    try (BatchWriter writer = context.createBatchWriter(DataLevel.USER.metaTable())) {
      String prefix = ScanServerFileReferenceSection.getRowPrefix();
      for (ScanServerRefTabletFile ref : scanRefs) {
        Mutation m = new Mutation(prefix + ref.getRowSuffix());
        m.put(ref.getServerAddress(), ref.getServerLockUUID(), ref.getValue());
        writer.addMutation(m);
      }
    } catch (MutationsRejectedException | TableNotFoundException e) {
      throw new IllegalStateException(
          "Error inserting scan server file references into " + DataLevel.USER.metaTable(), e);
    }
  }

  @Override
  public Stream<ScanServerRefTabletFile> getScanServerFileReferences() {
    try {
      Scanner scanner = context.createScanner(DataLevel.USER.metaTable(), Authorizations.EMPTY);
      scanner.setRange(ScanServerFileReferenceSection.getRange());
      int pLen = ScanServerFileReferenceSection.getRowPrefix().length();
      return StreamSupport.stream(scanner.spliterator(), false)
          .map(e -> new ScanServerRefTabletFile(e.getKey().getRowData().toString().substring(pLen),
              e.getKey().getColumnFamily(), e.getKey().getColumnQualifier()));
    } catch (TableNotFoundException e) {
      throw new IllegalStateException(DataLevel.USER.metaTable() + " not found!", e);
    }
  }

  @Override
  public void deleteScanServerFileReferences(String serverAddress, UUID scanServerLockUUID) {
    Objects.requireNonNull(serverAddress, "Server address must be supplied");
    Objects.requireNonNull(scanServerLockUUID, "Server uuid must be supplied");
    try (
        Scanner scanner = context.createScanner(DataLevel.USER.metaTable(), Authorizations.EMPTY)) {
      scanner.setRange(ScanServerFileReferenceSection.getRange());
      scanner.fetchColumn(new Text(serverAddress), new Text(scanServerLockUUID.toString()));

      int pLen = ScanServerFileReferenceSection.getRowPrefix().length();
      Set<ScanServerRefTabletFile> refsToDelete = StreamSupport.stream(scanner.spliterator(), false)
          .map(e -> new ScanServerRefTabletFile(e.getKey().getRowData().toString().substring(pLen),
              e.getKey().getColumnFamily(), e.getKey().getColumnQualifier()))
          .collect(Collectors.toSet());

      if (!refsToDelete.isEmpty()) {
        this.deleteScanServerFileReferences(refsToDelete);
      }
    } catch (TableNotFoundException e) {
      throw new IllegalStateException(DataLevel.USER.metaTable() + " not found!", e);
    }
  }

  @Override
  public void deleteScanServerFileReferences(Collection<ScanServerRefTabletFile> refsToDelete) {
    try (BatchWriter writer = context.createBatchWriter(DataLevel.USER.metaTable())) {
      String prefix = ScanServerFileReferenceSection.getRowPrefix();
      for (ScanServerRefTabletFile ref : refsToDelete) {
        Mutation m = new Mutation(prefix + ref.getRowSuffix());
        m.putDelete(ref.getServerAddress(), ref.getServerLockUUID());
        writer.addMutation(m);
      }
      log.debug("Deleted scan server file reference entries for files: {}", refsToDelete);
    } catch (MutationsRejectedException | TableNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

}
