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
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ExternalCompactionColumnFamily;
import static org.apache.accumulo.core.metadata.schema.UpgraderDeprecatedConstants.ChoppedColumnFamily;
import static org.apache.accumulo.server.AccumuloDataVersion.METADATA_FILE_JSON_ENCODING;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.RootTabletMetadata;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
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
    processReferences(context, rootName);
  }

  @Override
  public void upgradeMetadata(@NonNull ServerContext context) {
    log.debug("Upgrade metadata: upgrading to data version {}", METADATA_FILE_JSON_ENCODING);
    var metaName = Ample.DataLevel.USER.metaTable();
    processReferences(context, metaName);
  }

  private void processReferences(ServerContext context, String tableName) {
    // not using ample to avoid StoredTabletFile because old file ref is incompatible
    try (AccumuloClient c = Accumulo.newClient().from(context.getProperties()).build();
        BatchWriter batchWriter = c.createBatchWriter(tableName);
        Scanner scanner = context.createScanner(tableName, Authorizations.EMPTY)) {

      scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
      scanner.fetchColumnFamily(ChoppedColumnFamily.NAME);
      scanner.fetchColumnFamily(ExternalCompactionColumnFamily.NAME);
      scanner.forEach((k, v) -> {
        var family = k.getColumnFamily();
        if (family.equals(DataFileColumnFamily.NAME)) {
          upgradeDataFileCF(k, v, batchWriter, tableName);
        } else if (family.equals(ChoppedColumnFamily.NAME)) {
          removeChoppedCF(k, batchWriter, tableName);
        } else if (family.equals(ExternalCompactionColumnFamily.NAME)) {
          removeExternalCompactionCF(k, batchWriter, tableName);
        } else {
          log.warn("Received unexpected column family processing references: " + family);
        }
      });
    } catch (MutationsRejectedException mex) {
      log.warn("Failed to update reference for table: " + tableName);
      log.warn("Constraint violations: {}", mex.getConstraintViolationSummaries());
      throw new IllegalStateException("Failed to process table: " + tableName, mex);
    } catch (Exception ex) {
      throw new IllegalStateException("Failed to process table: " + tableName, ex);
    }
  }

  @VisibleForTesting
  void upgradeDataFileCF(final Key key, final Value value, final BatchWriter batchWriter,
      final String tableName) {
    String file = key.getColumnQualifier().toString();
    // filter out references if they are in the correct format already.
    if (fileNeedsConversion(file)) {
      var fileJson = StoredTabletFile.of(new Path(file)).getMetadataText();
      try {
        Mutation update = new Mutation(key.getRow());
        update.at().family(DataFileColumnFamily.STR_NAME).qualifier(fileJson).put(value);
        log.trace("table: {}, adding: {}", tableName, update.prettyPrint());
        batchWriter.addMutation(update);

        Mutation delete = new Mutation(key.getRow());
        delete.at().family(DataFileColumnFamily.STR_NAME).qualifier(file).delete();
        log.trace("table {}: deleting: {}", tableName, delete.prettyPrint());
        batchWriter.addMutation(delete);
      } catch (MutationsRejectedException ex) {
        // include constraint violation info in log - but stop upgrade
        log.warn(
            "Failed to update file reference for table: " + tableName + ". row: " + key.getRow());
        log.warn("Constraint violations: {}", ex.getConstraintViolationSummaries());
        throw new IllegalStateException("File conversion failed. Aborting upgrade", ex);
      }
    }
  }

  @VisibleForTesting
  void removeChoppedCF(final Key key, final BatchWriter batchWriter, final String tableName) {
    Mutation delete = null;
    try {
      delete = new Mutation(key.getRow()).at().family(ChoppedColumnFamily.STR_NAME)
          .qualifier(ChoppedColumnFamily.STR_NAME).delete();
      log.warn(
          "Deleting chopped reference from:{}. Previous split or delete may not have completed cleanly. Ref: {}",
          tableName, delete.prettyPrint());
      batchWriter.addMutation(delete);
    } catch (MutationsRejectedException ex) {
      log.warn("Failed to delete obsolete chopped CF reference for table: " + tableName + ". Ref: "
          + delete.prettyPrint() + ". Will try to continue. Ref may need to be manually removed");
      log.warn("Constraint violations: {}", ex.getConstraintViolationSummaries());
    }
  }

  @VisibleForTesting
  void removeExternalCompactionCF(final Key key, final BatchWriter batchWriter,
      final String tableName) {
    Mutation delete = null;
    try {
      delete = new Mutation(key.getRow()).at().family(ExternalCompactionColumnFamily.NAME)
          .qualifier(key.getColumnQualifier()).delete();
      log.warn(
          "Deleting external compaction reference from:{}. Previous compaction may not have completed. Ref: {}",
          tableName, delete.prettyPrint());
      batchWriter.addMutation(delete);
    } catch (MutationsRejectedException ex) {
      log.warn("Failed to delete obsolete external compaction CF reference for table: " + tableName
          + ". Ref: " + delete.prettyPrint()
          + ". Will try to continue. Ref may need to be manually removed");
      log.warn("Constraint violations: {}", ex.getConstraintViolationSummaries());
    }
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
      log.trace("file reference does not need conversion: {}", candidate);
      return false;
    } catch (JsonSyntaxException ex) {
      log.trace("file reference needs conversion: {}", candidate);
      return true;
    }
  }
}
