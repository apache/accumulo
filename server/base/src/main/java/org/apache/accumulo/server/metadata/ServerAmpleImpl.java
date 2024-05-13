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

import java.net.URI;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.gc.GcCandidate;
import org.apache.accumulo.core.gc.ReferenceFile;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.ScanServerRefTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.ValidationUtil;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.AmpleImpl;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.BlipSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.DeletesSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.DeletesSection.SkewedKeyValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.ScanServerFileReferenceSection;
import org.apache.accumulo.core.metadata.schema.TabletMutatorBase;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

public class ServerAmpleImpl extends AmpleImpl implements Ample {

  private static Logger log = LoggerFactory.getLogger(ServerAmpleImpl.class);

  private final ServerContext context;

  public ServerAmpleImpl(ServerContext context) {
    super(context);
    this.context = context;
  }

  public ServerAmpleImpl(ServerContext context, Function<DataLevel,String> tableMapper) {
    super(context, tableMapper);
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
    return new TabletsMutatorImpl(context, getTableMapper());
  }

  @Override
  public ConditionalTabletsMutator conditionallyMutateTablets() {
    return new ConditionalTabletsMutatorImpl(context, getTableMapper());
  }

  @Override
  public AsyncConditionalTabletsMutator
      conditionallyMutateTablets(Consumer<ConditionalResult> resultsConsumer) {
    return new AsyncConditionalTabletsMutatorImpl(context, getTableMapper(), resultsConsumer);
  }

  private void mutateRootGcCandidates(Consumer<RootGcCandidates> mutator) {
    String zpath = context.getZooKeeperRoot() + ZROOT_TABLET_GC_CANDIDATES;
    try {
      // TODO calling create seems unnecessary and is possibly racy and inefficient
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
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void putGcCandidates(TableId tableId, Collection<StoredTabletFile> candidates) {

    if (AccumuloTable.ROOT.tableId().equals(tableId)) {
      mutateRootGcCandidates(rgcc -> rgcc.add(candidates.stream()));
      return;
    }

    try (BatchWriter writer = context.createBatchWriter(getMetaTable(DataLevel.of(tableId)))) {
      for (StoredTabletFile file : candidates) {
        writer.addMutation(createDeleteMutation(file));
      }
    } catch (MutationsRejectedException | TableNotFoundException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void putGcFileAndDirCandidates(TableId tableId, Collection<ReferenceFile> candidates) {

    if (DataLevel.of(tableId) == DataLevel.ROOT) {
      // Directories are unexpected for the root tablet, so convert to stored tablet file
      mutateRootGcCandidates(rgcc -> rgcc.add(candidates.stream()
          .map(reference -> StoredTabletFile.of(URI.create(reference.getMetadataPath())))));
      return;
    }

    try (BatchWriter writer = context.createBatchWriter(getMetaTable(DataLevel.of(tableId)))) {
      for (var fileOrDir : candidates) {
        writer.addMutation(createDeleteMutation(fileOrDir));
      }
    } catch (MutationsRejectedException | TableNotFoundException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void addBulkLoadInProgressFlag(String path, FateId fateId) {

    // Bulk Import operations are not supported on the metadata table, so no entries will ever be
    // required on the root table.
    Mutation m = new Mutation(BlipSection.getRowPrefix() + path);
    m.put(EMPTY_TEXT, EMPTY_TEXT, new Value(fateId.canonical()));

    try (BatchWriter bw = context.createBatchWriter(AccumuloTable.METADATA.tableName())) {
      bw.addMutation(m);
    } catch (MutationsRejectedException | TableNotFoundException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void removeBulkLoadInProgressFlag(String path) {

    // Bulk Import operations are not supported on the metadata table, so no entries will ever be
    // required on the root table.
    Mutation m = new Mutation(BlipSection.getRowPrefix() + path);
    m.putDelete(EMPTY_TEXT, EMPTY_TEXT);

    try (BatchWriter bw = context.createBatchWriter(AccumuloTable.METADATA.tableName())) {
      bw.addMutation(m);
    } catch (MutationsRejectedException | TableNotFoundException e) {
      throw new IllegalStateException(e);
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

    try (BatchWriter writer = context.createBatchWriter(getMetaTable(level))) {
      if (type == GcCandidateType.VALID) {
        for (GcCandidate candidate : candidates) {
          Mutation m = new Mutation(DeletesSection.encodeRow(candidate.getPath()));
          // Removes all versions of the candidate to avoid reprocessing deleted file entries
          m.putDelete(EMPTY_TEXT, EMPTY_TEXT);
          writer.addMutation(m);
        }
      } else {
        for (GcCandidate candidate : candidates) {
          Mutation m = new Mutation(DeletesSection.encodeRow(candidate.getPath()));
          // Removes this and older versions while allowing newer candidate versions to persist
          m.putDelete(EMPTY_TEXT, EMPTY_TEXT, candidate.getUid());
          writer.addMutation(m);
        }
      }
    } catch (MutationsRejectedException | TableNotFoundException e) {
      throw new IllegalStateException(e);
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
        throw new IllegalStateException(e);
      }
      return new RootGcCandidates(new String(jsonBytes, UTF_8)).sortedStream().iterator();
    } else if (level == DataLevel.METADATA || level == DataLevel.USER) {
      Range range = DeletesSection.getRange();

      Scanner scanner;
      try {
        scanner = context.createScanner(getMetaTable(level), Authorizations.EMPTY);
      } catch (TableNotFoundException e) {
        throw new IllegalStateException(e);
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
    return createDelMutation(ValidationUtil.validate(tabletFilePathToRemove).getMetadataPath());
  }

  public Mutation createDeleteMutation(StoredTabletFile pathToRemove) {
    return createDelMutation(pathToRemove.getMetadataPath());
  }

  private Mutation createDelMutation(String path) {
    Mutation delFlag = new Mutation(new Text(DeletesSection.encodeRow(path)));
    delFlag.put(EMPTY_TEXT, EMPTY_TEXT, DeletesSection.SkewedKeyValue.NAME);
    return delFlag;
  }

  @Override
  public void putScanServerFileReferences(Collection<ScanServerRefTabletFile> scanRefs) {
    var metaTable = getMetaTable(DataLevel.USER);
    try (BatchWriter writer = context.createBatchWriter(metaTable)) {
      String prefix = ScanServerFileReferenceSection.getRowPrefix();
      for (ScanServerRefTabletFile ref : scanRefs) {
        Mutation m = new Mutation(prefix + ref.getRowSuffix());
        m.put(ref.getServerAddress(), ref.getServerLockUUID(), ref.getValue());
        writer.addMutation(m);
      }
    } catch (MutationsRejectedException | TableNotFoundException e) {
      throw new IllegalStateException(
          "Error inserting scan server file references into " + metaTable, e);
    }
  }

  @Override
  public Stream<ScanServerRefTabletFile> getScanServerFileReferences() {
    var metaTable = getMetaTable(DataLevel.USER);
    try {
      Scanner scanner = context.createScanner(metaTable, Authorizations.EMPTY);
      scanner.setRange(ScanServerFileReferenceSection.getRange());
      int pLen = ScanServerFileReferenceSection.getRowPrefix().length();
      return scanner.stream().onClose(scanner::close)
          .map(e -> new ScanServerRefTabletFile(e.getKey().getRowData().toString().substring(pLen),
              e.getKey().getColumnFamily(), e.getKey().getColumnQualifier()));
    } catch (TableNotFoundException e) {
      throw new IllegalStateException(metaTable + " not found!", e);
    }
  }

  @Override
  public void deleteScanServerFileReferences(String serverAddress, UUID scanServerLockUUID) {
    Objects.requireNonNull(serverAddress, "Server address must be supplied");
    Objects.requireNonNull(scanServerLockUUID, "Server uuid must be supplied");
    var metaTable = getMetaTable(DataLevel.USER);
    try (Scanner scanner = context.createScanner(metaTable, Authorizations.EMPTY)) {
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
      throw new IllegalStateException(metaTable + " not found!", e);
    }
  }

  @Override
  public void deleteScanServerFileReferences(Collection<ScanServerRefTabletFile> refsToDelete) {
    try (BatchWriter writer = context.createBatchWriter(getMetaTable(DataLevel.USER))) {
      String prefix = ScanServerFileReferenceSection.getRowPrefix();
      for (ScanServerRefTabletFile ref : refsToDelete) {
        Mutation m = new Mutation(prefix + ref.getRowSuffix());
        m.putDelete(ref.getServerAddress(), ref.getServerLockUUID());
        writer.addMutation(m);
      }
      log.debug("Deleted scan server file reference entries for files: {}", refsToDelete);
    } catch (MutationsRejectedException | TableNotFoundException e) {
      throw new IllegalStateException(e);
    }
  }

  @VisibleForTesting
  protected ServerContext getContext() {
    return context;
  }

  private String getMetaTable(DataLevel dataLevel) {
    return getTableMapper().apply(dataLevel);
  }
}
