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
package org.apache.accumulo.server.metadata;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.metadata.RootTable.ZROOT_TABLET_GC_CANDIDATES;
import static org.apache.accumulo.server.util.MetadataTableUtil.EMPTY_TEXT;

import java.util.Collection;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFileUtil;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.AmpleImpl;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.DeletesSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.DeletesSection.SkewedKeyValue;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.fate.zookeeper.ZooReader;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class ServerAmpleImpl extends AmpleImpl implements Ample {

  private static Logger log = LoggerFactory.getLogger(ServerAmpleImpl.class);

  private ServerContext context;

  public ServerAmpleImpl(ServerContext ctx) {
    super(ctx);
    this.context = ctx;
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
        RootGcCandidates rgcc = RootGcCandidates.fromJson(currJson);
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
      mutateRootGcCandidates(rgcc -> rgcc.add(candidates.iterator()));
      return;
    }

    try (BatchWriter writer = createWriter(tableId)) {
      for (StoredTabletFile file : candidates) {
        writer.addMutation(createDeleteMutation(file));
      }
    } catch (MutationsRejectedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void putGcFileAndDirCandidates(TableId tableId, Collection<String> candidates) {

    if (RootTable.ID.equals(tableId)) {

      // Directories are unexpected for the root tablet, so convert to stored tablet file
      mutateRootGcCandidates(
          rgcc -> rgcc.add(candidates.stream().map(StoredTabletFile::new).iterator()));
      return;
    }

    try (BatchWriter writer = createWriter(tableId)) {
      for (String fileOrDir : candidates) {
        writer.addMutation(createDeleteMutation(fileOrDir));
      }
    } catch (MutationsRejectedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void deleteGcCandidates(DataLevel level, Collection<String> paths) {

    if (level == DataLevel.ROOT) {
      mutateRootGcCandidates(rgcc -> rgcc.remove(paths));
      return;
    }

    try (BatchWriter writer = context.createBatchWriter(level.metaTable())) {
      for (String path : paths) {
        Mutation m = new Mutation(DeletesSection.encodeRow(path));
        m.putDelete(EMPTY_TEXT, EMPTY_TEXT);
        writer.addMutation(m);
      }
    } catch (MutationsRejectedException | TableNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Iterator<String> getGcCandidates(DataLevel level, String continuePoint) {
    if (level == DataLevel.ROOT) {
      var zooReader = new ZooReader(context.getZooKeepers(), context.getZooKeepersSessionTimeOut());
      byte[] json;
      try {
        json = zooReader.getData(context.getZooKeeperRoot() + RootTable.ZROOT_TABLET_GC_CANDIDATES);
      } catch (KeeperException | InterruptedException e) {
        throw new RuntimeException(e);
      }
      Stream<String> candidates = RootGcCandidates.fromJson(json).stream().sorted();

      if (continuePoint != null && !continuePoint.isEmpty()) {
        candidates = candidates.dropWhile(candidate -> candidate.compareTo(continuePoint) <= 0);
      }

      return candidates.iterator();
    } else if (level == DataLevel.METADATA || level == DataLevel.USER) {
      Range range = DeletesSection.getRange();
      if (continuePoint != null && !continuePoint.isEmpty()) {
        String continueRow = DeletesSection.encodeRow(continuePoint);
        range = new Range(new Key(continueRow).followingKey(PartialKey.ROW), true,
            range.getEndKey(), range.isEndKeyInclusive());
      }

      Scanner scanner;
      try {
        scanner = context.createScanner(level.metaTable(), Authorizations.EMPTY);
      } catch (TableNotFoundException e) {
        throw new RuntimeException(e);
      }
      scanner.setRange(range);
      return StreamSupport.stream(scanner.spliterator(), false)
          .filter(entry -> entry.getValue().equals(SkewedKeyValue.NAME))
          .map(entry -> DeletesSection.decodeRow(entry.getKey().getRow().toString())).iterator();
    } else {
      throw new IllegalArgumentException();
    }
  }

  private BatchWriter createWriter(TableId tableId) {

    Preconditions.checkArgument(!RootTable.ID.equals(tableId));

    try {
      if (MetadataTable.ID.equals(tableId)) {
        return context.createBatchWriter(RootTable.NAME);
      } else {
        return context.createBatchWriter(MetadataTable.NAME);
      }
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Mutation createDeleteMutation(String tabletFilePathToRemove) {
    String path = TabletFileUtil.validate(tabletFilePathToRemove);
    return createDelMutation(path);
  }

  public Mutation createDeleteMutation(StoredTabletFile pathToRemove) {
    return createDelMutation(pathToRemove.getMetaUpdateDelete());
  }

  private Mutation createDelMutation(String path) {
    Mutation delFlag = new Mutation(new Text(DeletesSection.encodeRow(path)));
    delFlag.put(EMPTY_TEXT, EMPTY_TEXT, DeletesSection.SkewedKeyValue.NAME);
    return delFlag;
  }
}
