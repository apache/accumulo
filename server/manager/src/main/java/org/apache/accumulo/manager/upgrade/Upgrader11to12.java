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

import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.RootTabletMetadata;
import org.apache.accumulo.core.metadata.schema.UpgraderDeprecatedConstants;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.ComparablePair;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonSyntaxException;

public class Upgrader11to12 implements Upgrader {

  private static final Logger log = LoggerFactory.getLogger(Upgrader11to12.class);

  @Override
  public void upgradeZookeeper(@NonNull ServerContext context) {
    log.debug("Upgrade ZooKeeper: upgrading to data version {}", METADATA_FILE_JSON_ENCODING);
    var rootBase = ZooUtil.getRoot(context.getInstanceID()) + ZROOT_TABLET;

    try {
      var zrw = context.getZooReaderWriter();
      Stat stat = new Stat();
      byte[] rootData = zrw.getData(rootBase, stat);

      String json = new String(rootData, UTF_8);
      if (RootTabletMetadata.Data.needsConversion(json)) {
        RootTabletMetadata rtm = RootTabletMetadata.upgrade(json);
        zrw.overwritePersistentData(rootBase, rtm.toJson().getBytes(UTF_8), stat.getVersion());
      }
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(
          "Could not read root metadata from ZooKeeper due to interrupt", ex);
    } catch (KeeperException ex) {
      throw new IllegalStateException(
          "Could not read root metadata from ZooKeeper because of ZooKeeper exception", ex);
    }
  }

  @Override
  public void upgradeRoot(@NonNull ServerContext context) {
    log.debug("Upgrade root: upgrading to data version {}", METADATA_FILE_JSON_ENCODING);

    var rootName = Ample.DataLevel.METADATA.metaTable();
    var filesToConvert = getFileReferences(context, rootName);
    convertFileReferences(context, rootName, filesToConvert);
    deleteObsoleteReferences(context, rootName);
  }

  @Override
  public void upgradeMetadata(@NonNull ServerContext context) {
    log.debug("Upgrade metadata: upgrading to data version {}", METADATA_FILE_JSON_ENCODING);

    var metaName = Ample.DataLevel.USER.metaTable();
    var filesToConvert = getFileReferences(context, metaName);
    convertFileReferences(context, metaName, filesToConvert);
    deleteObsoleteReferences(context, metaName);
  }

  private Map<ComparablePair<KeyExtent,String>,Value>
      getFileReferences(@NonNull final ServerContext context, @NonNull final String tableName) {

    log.trace("gather file references for table: {}", tableName);

    Map<ComparablePair<KeyExtent,String>,Value> filesToConvert = new TreeMap<>();

    try (Scanner scanner = context.createScanner(tableName, Authorizations.EMPTY)) {
      scanner.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.STR_NAME);
      scanner.forEach((k, v) -> {
        KeyExtent ke = KeyExtent.fromMetaRow(k.getRow());
        String file = k.getColumnQualifier().toString();
        // filter out references that are in the correct format already.
        if (fileNeedsConversion(file)) {
          var prev = filesToConvert.put(new ComparablePair<>(ke, file), v);
          if (prev != null) {
            throw new IllegalStateException(
                "upgrade for table: " + tableName + " aborted, would have missed: " + prev);
          }
        }
      });
    } catch (TableNotFoundException ex) {
      throw new IllegalStateException("failed to read metadata table for upgrade", ex);
    }
    log.debug("Number of files to convert for table: {}, number of files: {}", tableName,
        filesToConvert.size());
    return filesToConvert;
  }

  private void convertFileReferences(final ServerContext context, final String tableName,
      Map<ComparablePair<KeyExtent,String>,Value> filesToConvert) {

    // not using ample to avoid StoredTabletFile because old file ref is incompatible
    try (AccumuloClient c = Accumulo.newClient().from(context.getProperties()).build();
        BatchWriter batchWriter = c.createBatchWriter(tableName)) {
      filesToConvert.forEach((refPair, value) -> {
        try {
          log.trace("update file reference for table: {}. row: {} to: {} value: {}", tableName,
              refPair.getFirst().toMetaRow(), refPair.getSecond(), value);

          var row = refPair.getFirst().toMetaRow();
          var fileJson = new Text(StoredTabletFile.serialize(refPair.getSecond(), new Range()));

          Mutation m = new Mutation(row);
          m.at().family(MetadataSchema.TabletsSection.DataFileColumnFamily.STR_NAME)
              .qualifier(fileJson).put(value);

          log.trace("table: {}, adding: {}", tableName, m.prettyPrint());
          batchWriter.addMutation(m);

          Mutation delete = new Mutation(row);
          delete.at().family(MetadataSchema.TabletsSection.DataFileColumnFamily.STR_NAME)
              .qualifier(refPair.getSecond()).delete();

          log.trace("table {}: deleting: {}", tableName, delete.prettyPrint());
          batchWriter.addMutation(delete);
        } catch (MutationsRejectedException ex) {
          throw new IllegalStateException("Failed to update file entries for table: " + tableName,
              ex);
        }
      });
      batchWriter.flush();
    } catch (Exception ex) {
      throw new IllegalStateException(ex);
    }
  }

  /**
   * Removes chopped and external compaction references that have obsolete encoding that is
   * incompatible with StoredTabletFile json encoding. These references should be rare. If they are
   * present, some operations likely terminated abnormally under the old version before shutdown.
   * The deletions are logged so those operations may be re-run if desired.
   */
  private void deleteObsoleteReferences(ServerContext context, String tableName) {
    log.debug("processing obsolete references for table: {}", tableName);
    try (AccumuloClient c = Accumulo.newClient().from(context.getProperties()).build();
        BatchWriter batchWriter = c.createBatchWriter(tableName)) {

      try (Scanner scanner = context.createScanner(tableName, Authorizations.EMPTY)) {
        scanner.fetchColumnFamily(UpgraderDeprecatedConstants.ChoppedColumnFamily.STR_NAME);
        scanner
            .fetchColumnFamily(MetadataSchema.TabletsSection.ExternalCompactionColumnFamily.NAME);
        scanner.forEach((k, v) -> {
          Mutation delete;
          var family = k.getColumnFamily();
          if (family.equals(UpgraderDeprecatedConstants.ChoppedColumnFamily.NAME)) {
            delete = buildChoppedDeleteMutation(k, tableName);
          } else if (family
              .equals(MetadataSchema.TabletsSection.ExternalCompactionColumnFamily.NAME)) {
            delete = buildExternalCompactionDelete(k, tableName);
          } else {
            throw new IllegalStateException(
                "unexpected column Family: '{}' seen processing obsolete references");
          }
          try {
            batchWriter.addMutation(delete);
          } catch (MutationsRejectedException ex) {
            log.warn("Failed to delete obsolete reference for table: " + tableName + ". Ref: "
                + delete.prettyPrint()
                + ". Will try to continue. Ref may need to be manually removed");
            log.warn("Constraint violations: {}", ex.getConstraintViolationSummaries());
          }
        });
      }
    } catch (MutationsRejectedException ex) {
      log.warn("Failed to delete obsolete reference for table: " + tableName + " on close");
      log.warn("Constraint violations: {}", ex.getConstraintViolationSummaries());
      throw new IllegalStateException(ex);
    } catch (Exception ex) {
      throw new IllegalStateException(
          "Processing obsolete referenced for table: " + tableName + " failed. Upgrade aborting",
          ex);
    }
  }

  private Mutation buildChoppedDeleteMutation(final Key k, final String tableName) {
    Mutation delete = new Mutation(k.getRow()).at()
        .family(UpgraderDeprecatedConstants.ChoppedColumnFamily.STR_NAME)
        .qualifier(UpgraderDeprecatedConstants.ChoppedColumnFamily.STR_NAME).delete();
    log.warn(
        "Deleting chopped reference from:{}. Previous split or delete may not have completed cleanly. Ref: {}",
        tableName, delete.prettyPrint());
    return delete;
  }

  private Mutation buildExternalCompactionDelete(Key k, String tableName) {
    Mutation delete = new Mutation(k.getRow()).at()
        .family(MetadataSchema.TabletsSection.ExternalCompactionColumnFamily.NAME)
        .qualifier(k.getColumnQualifier()).delete();
    log.warn(
        "Deleting external compaction reference from:{}. Previous compaction may not have completed. Ref: {}",
        tableName, delete.prettyPrint());
    return delete;
  }

  /**
   * Quick sanity check to see if value has been converted by checking the candidate can be parsed
   * into a StoredTabletFile. If parsing does not throw a parsing exception. then assume it has been
   * converted. If validate cannot parse the candidate a JSON, then it is assumed that it has not
   * been converted. Other parsing errors will propagate as IllegalArgumentExceptions.
   *
   * @param candidate a possible file: reference.
   * @return false if a valid StoredTabletFile, true if it cannot be parsed as JSON. Otherwise,
   *         propagate an IllegalArgumentException
   */
  private boolean fileNeedsConversion(@NonNull final String candidate) {
    try {
      StoredTabletFile.validate(candidate);
      return false;
    } catch (JsonSyntaxException ex) {
      return true;
    }
  }
}
