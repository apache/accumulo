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
package org.apache.accumulo.manager.upgrade;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.metadata.RootTable.ZROOT_TABLET;
import static org.apache.accumulo.server.AccumuloDataVersion.METADATA_FILE_JSON_ENCODING;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ChoppedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ExternalCompactionColumnFamily;
import org.apache.accumulo.core.metadata.schema.RootTabletMetadata;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

public class Upgrader11to12 implements Upgrader {

  private static final Logger log = LoggerFactory.getLogger(Upgrader11to12.class);

  @SuppressWarnings("deprecation")
  private static final Text CHOPPED = ChoppedColumnFamily.NAME;

  @VisibleForTesting
  static final Set<Text> UPGRADE_FAMILIES =
      Set.of(DataFileColumnFamily.NAME, CHOPPED, ExternalCompactionColumnFamily.NAME);

  @Override
  public void upgradeZookeeper(@NonNull ServerContext context) {
    log.debug("Upgrade ZooKeeper: upgrading to data version {}", METADATA_FILE_JSON_ENCODING);
    var rootBase = ZooUtil.getRoot(context.getInstanceID()) + ZROOT_TABLET;

    try {
      var zrw = context.getZooReaderWriter();
      Stat stat = new Stat();
      byte[] rootData = zrw.getData(rootBase, stat);

      String json = new String(rootData, UTF_8);

      var rtm = new RootTabletMetadata(json);

      TreeMap<Key,Value> entries = new TreeMap<>();
      rtm.toKeyValues().filter(e -> UPGRADE_FAMILIES.contains(e.getKey().getColumnFamily()))
          .forEach(entry -> entries.put(entry.getKey(), entry.getValue()));
      ArrayList<Mutation> mutations = new ArrayList<>();

      processReferences(mutations::add, entries.entrySet(), "root_table_metadata");

      Preconditions.checkState(mutations.size() <= 1);

      if (!mutations.isEmpty()) {
        log.info("Root metadata in ZooKeeper before upgrade: {}", json);
        rtm.update(mutations.get(0));
        zrw.overwritePersistentData(rootBase, rtm.toJson().getBytes(UTF_8), stat.getVersion());
        log.info("Root metadata in ZooKeeper after upgrade: {}", rtm.toJson());
      }
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(
          "Could not read root metadata from ZooKeeper due to interrupt", ex);
    } catch (KeeperException ex) {
      throw new IllegalStateException(
          "Could not read or write root metadata in ZooKeeper because of ZooKeeper exception", ex);
    }
  }

  interface MutationWriter {
    void addMutation(Mutation m) throws MutationsRejectedException;
  }

  @Override
  public void upgradeRoot(@NonNull ServerContext context) {
    log.debug("Upgrade root: upgrading to data version {}", METADATA_FILE_JSON_ENCODING);
    var rootName = Ample.DataLevel.METADATA.metaTable();
    upgradeTabletsMetadata(context, rootName);
  }

  @Override
  public void upgradeMetadata(@NonNull ServerContext context) {
    log.debug("Upgrade metadata: upgrading to data version {}", METADATA_FILE_JSON_ENCODING);
    var metaName = Ample.DataLevel.USER.metaTable();
    upgradeTabletsMetadata(context, metaName);
  }

  private void upgradeTabletsMetadata(@NonNull ServerContext context, String metaName) {
    // not using ample to avoid StoredTabletFile because old file ref is incompatible
    try (BatchWriter batchWriter = context.createBatchWriter(metaName); Scanner scanner =
        new IsolatedScanner(context.createScanner(metaName, Authorizations.EMPTY))) {
      UPGRADE_FAMILIES.forEach(scanner::fetchColumnFamily);
      scanner.setRange(MetadataSchema.TabletsSection.getRange());
      processReferences(batchWriter::addMutation, scanner, metaName);
    } catch (TableNotFoundException ex) {
      throw new IllegalStateException("Failed to find table " + metaName, ex);
    } catch (MutationsRejectedException mex) {
      log.warn("Failed to update reference for table: " + metaName);
      log.warn("Constraint violations: {}", mex.getConstraintViolationSummaries());
      throw new IllegalStateException("Failed to process table: " + metaName, mex);
    }
  }

  void processReferences(MutationWriter batchWriter, Iterable<Map.Entry<Key,Value>> scanner,
      String tableName) {
    try {
      Mutation update = null;
      for (Map.Entry<Key,Value> entry : scanner) {
        Key key = entry.getKey();
        Value value = entry.getValue();
        Preconditions.checkState(key.getColumnVisibilityData().length() == 0,
            "Expected empty visibility, saw %s ", key.getColumnVisibilityData());
        // on new row, write current mutation and prepare a new one.
        Text r = key.getRow();
        if (update == null) {
          update = new Mutation(r);
        } else if (!Arrays.equals(update.getRow(), TextUtil.getBytes(r))) {
          if (log.isTraceEnabled()) {
            log.trace("table: {}, update: {}", tableName, update.prettyPrint());
          }
          if (!update.getUpdates().isEmpty()) {
            batchWriter.addMutation(update);
          }
          update = new Mutation(r);
        }

        var family = key.getColumnFamily();
        if (family.equals(DataFileColumnFamily.NAME)) {
          upgradeDataFileCF(key, value, update);
        } else if (family.equals(CHOPPED)) {
          log.warn(
              "Deleting chopped reference from:{}. Previous split or delete may not have completed cleanly. Ref: {}",
              tableName, key.getRow());
          update.at().family(CHOPPED).qualifier(CHOPPED).delete();
        } else if (family.equals(ExternalCompactionColumnFamily.NAME)) {
          log.debug(
              "Deleting external compaction reference from:{}. Previous compaction may not have completed. Ref: {}",
              tableName, key.getRow());
          update.at().family(ExternalCompactionColumnFamily.NAME)
              .qualifier(key.getColumnQualifier()).delete();
        } else {
          throw new IllegalStateException("Processing: " + tableName
              + " Received unexpected column family processing references: " + family);
        }
      }
      // send last mutation
      if (update != null && !update.getUpdates().isEmpty()) {
        log.trace("table: {}, update: {}", tableName, update.prettyPrint());
        batchWriter.addMutation(update);
      }
    } catch (MutationsRejectedException mex) {
      log.warn("Failed to update reference for table: " + tableName);
      log.warn("Constraint violations: {}", mex.getConstraintViolationSummaries());
      throw new IllegalStateException("Failed to process table: " + tableName, mex);
    }
  }

  @VisibleForTesting
  static void upgradeDataFileCF(final Key key, final Value value, final Mutation m) {
    String file = key.getColumnQualifier().toString();
    // filter out references if they are in the correct format already.
    boolean needsConversion = StoredTabletFile.fileNeedsConversion(file);
    log.trace("file: {} needs conversion: {}", file, needsConversion);
    if (needsConversion) {
      var fileJson = StoredTabletFile.of(new Path(file)).getMetadataText();
      m.at().family(DataFileColumnFamily.STR_NAME).qualifier(fileJson).put(value);
      m.at().family(DataFileColumnFamily.STR_NAME).qualifier(file).delete();
    }
  }

}
