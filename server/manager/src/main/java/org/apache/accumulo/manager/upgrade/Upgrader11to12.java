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
import static java.util.Objects.requireNonNull;
import static org.apache.accumulo.core.metadata.RootTable.ZROOT_TABLET;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.RESERVED_PREFIX;
import static org.apache.accumulo.server.AccumuloDataVersion.METADATA_FILE_JSON_ENCODING;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.clientImpl.NamespaceMapping;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ChoppedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ExternalCompactionColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ScanFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.RootTabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMergeabilityMetadata;
import org.apache.accumulo.core.schema.Section;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Encoding;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.core.util.tables.TableNameUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.accumulo.server.init.FileSystemInitializer;
import org.apache.accumulo.server.init.InitialConfiguration;
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

  public static final Collection<Range> OLD_SCAN_SERVERS_RANGES =
      List.of(new Range("~sserv", "~sserx"), new Range("~scanref", "~scanreg"));

  @VisibleForTesting
  static final Set<Text> UPGRADE_FAMILIES = Set.of(DataFileColumnFamily.NAME, CHOPPED,
      ExternalCompactionColumnFamily.NAME, ScanFileColumnFamily.NAME);

  private static final String ZTRACERS = "/tracers";

  @VisibleForTesting
  static final String ZNAMESPACE_NAME = "/name";

  @Override
  public void upgradeZookeeper(@NonNull ServerContext context) {
    log.debug("Upgrade ZooKeeper: upgrading to data version {}", METADATA_FILE_JSON_ENCODING);

    try {
      var zrw = context.getZooSession().asReaderWriter();

      // clean up nodes no longer in use
      zrw.recursiveDelete(ZTRACERS, ZooUtil.NodeMissingPolicy.SKIP);

      Stat stat = new Stat();
      byte[] rootData = zrw.getData(ZROOT_TABLET, stat);

      String json = new String(rootData, UTF_8);

      var rtm = new RootTabletMetadata(json);

      TreeMap<Key,Value> entries = new TreeMap<>();
      rtm.getKeyValues().filter(e -> UPGRADE_FAMILIES.contains(e.getKey().getColumnFamily()))
          .forEach(entry -> entries.put(entry.getKey(), entry.getValue()));
      ArrayList<Mutation> mutations = new ArrayList<>();

      processReferences(mutations::add, entries.entrySet(), "root_table_metadata");

      Preconditions.checkState(mutations.size() <= 1);

      if (!mutations.isEmpty()) {
        log.info("Root metadata in ZooKeeper before upgrade: {}", json);
        rtm.update(mutations.get(0));
        zrw.overwritePersistentData(ZROOT_TABLET, rtm.toJson().getBytes(UTF_8), stat.getVersion());
        log.info("Root metadata in ZooKeeper after upgrade: {}", rtm.toJson());
      }

      byte[] namespacesData = zrw.getData(Constants.ZNAMESPACES);
      if (namespacesData.length != 0) {
        throw new IllegalStateException(
            "Unexpected data found under namespaces node: " + new String(namespacesData, UTF_8));
      }
      List<String> namespaceIdList = zrw.getChildren(Constants.ZNAMESPACES);
      Map<String,String> namespaceMap = new HashMap<>();
      for (String namespaceId : namespaceIdList) {
        String namespaceNamePath = Constants.ZNAMESPACES + "/" + namespaceId + ZNAMESPACE_NAME;
        namespaceMap.put(namespaceId, new String(zrw.getData(namespaceNamePath), UTF_8));
      }
      byte[] mapping = NamespaceMapping.serializeMap(namespaceMap);
      zrw.putPersistentData(Constants.ZNAMESPACES, mapping, ZooUtil.NodeExistsPolicy.OVERWRITE);

      for (String namespaceId : namespaceIdList) {
        String namespaceNamePath = Constants.ZNAMESPACES + "/" + namespaceId + ZNAMESPACE_NAME;
        zrw.delete(namespaceNamePath);
      }

      log.info("Removing problems reports from zookeeper");
      removeZKProblemReports(context);

      log.info("Creating ZooKeeper entries for ScanServerRefTable");
      preparePre4_0NewTableState(context, SystemTables.SCAN_REF.tableId(), Namespace.ACCUMULO.id(),
          SystemTables.SCAN_REF.tableName(), TableState.ONLINE, ZooUtil.NodeExistsPolicy.FAIL);

    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(
          "Could not read root metadata from ZooKeeper due to interrupt", ex);
    } catch (KeeperException ex) {
      throw new IllegalStateException(
          "Could not read or write root metadata in ZooKeeper because of ZooKeeper exception", ex);
    }
  }

  private static final String ZTABLE_NAME = "/name";
  private static final String ZTABLE_COMPACT_ID = "/compact-id";
  private static final String ZTABLE_COMPACT_CANCEL_ID = "/compact-cancel-id";
  private static final String ZTABLE_STATE = "/state";
  private static final byte[] ZERO_BYTE = {'0'};

  public static void preparePre4_0NewTableState(ServerContext context, TableId tableId,
      NamespaceId namespaceId, String tableName, TableState state, NodeExistsPolicy existsPolicy)
      throws KeeperException, InterruptedException {
    // state gets created last
    log.debug("Creating ZooKeeper entries for new table {} (ID: {}) in namespace (ID: {})",
        tableName, tableId, namespaceId);
    Pair<String,String> qualifiedTableName = TableNameUtil.qualify(tableName);
    tableName = qualifiedTableName.getSecond();
    String zTablePath = Constants.ZTABLES + "/" + tableId;
    final ZooReaderWriter zoo = context.getZooSession().asReaderWriter();
    zoo.putPersistentData(zTablePath, new byte[0], existsPolicy);
    zoo.putPersistentData(zTablePath + Constants.ZTABLE_NAMESPACE,
        namespaceId.canonical().getBytes(UTF_8), existsPolicy);
    zoo.putPersistentData(zTablePath + ZTABLE_NAME, tableName.getBytes(UTF_8), existsPolicy);
    zoo.putPersistentData(zTablePath + Constants.ZTABLE_FLUSH_ID, ZERO_BYTE, existsPolicy);
    zoo.putPersistentData(zTablePath + ZTABLE_COMPACT_ID, ZERO_BYTE, existsPolicy);
    zoo.putPersistentData(zTablePath + ZTABLE_COMPACT_CANCEL_ID, ZERO_BYTE, existsPolicy);
    zoo.putPersistentData(zTablePath + ZTABLE_STATE, state.name().getBytes(UTF_8), existsPolicy);
    var propKey = TablePropKey.of(tableId);
    if (!context.getPropStore().exists(propKey)) {
      context.getPropStore().create(propKey, Map.of());
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
    removeScanServerRange(context, metaName);
    createScanServerRefTableMetadataEntries(context);
    log.info("Removing problems reports from metadata table");
    removeMetadataProblemReports(context);
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
        } else if (family.equals(ScanFileColumnFamily.NAME)) {
          log.debug("Deleting scan reference from:{}. Ref: {}", tableName, key.getRow());
          update.at().family(ScanFileColumnFamily.NAME).qualifier(key.getColumnQualifier())
              .delete();
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

  public void removeScanServerRange(ServerContext context, String tableName) {
    log.info("Removing Scan Server Range from table {}", tableName);
    try (BatchDeleter batchDeleter =
        context.createBatchDeleter(tableName, Authorizations.EMPTY, 4)) {
      batchDeleter.setRanges(OLD_SCAN_SERVERS_RANGES);
      batchDeleter.delete();
    } catch (TableNotFoundException | MutationsRejectedException e) {
      throw new RuntimeException(e);
    }
    log.info("Scan Server Ranges {} removed from table {}", OLD_SCAN_SERVERS_RANGES, tableName);
  }

  public void createScanServerRefTableMetadataEntries(ServerContext context) {
    try {
      FileSystemInitializer initializer = new FileSystemInitializer(
          new InitialConfiguration(context.getHadoopConf(), context.getSiteConfiguration()));
      // For upgrading an existing system set to never merge. If the mergeability is changed
      // then we would look to use the thrift client to look up the current Manager time to
      // set as part of the mergeability metadata
      FileSystemInitializer.InitialTablet scanRefTablet =
          initializer.createScanRefTablet(context, TabletMergeabilityMetadata.never());
      // Add references to the Metadata Table
      try (BatchWriter writer = context.createBatchWriter(SystemTables.METADATA.tableName())) {
        writer.addMutation(scanRefTablet.createMutation());
      } catch (MutationsRejectedException | TableNotFoundException e) {
        log.error("Failed to write tablet refs to metadata table");
        throw new RuntimeException(e);
      }
    } catch (IOException e) {
      log.error("Problem attempting to create ScanServerRef table", e);
    }
    log.info("Created ScanServerRef table");
  }

  private static final String ZPROBLEMS = "/problems";

  private void removeZKProblemReports(ServerContext context) {
    try {
      if (!context.getZooSession().asReaderWriter().exists(ZPROBLEMS)) {
        // could be running a second time and the node was already deleted
        return;
      }
      var children = context.getZooSession().asReaderWriter().getChildren(ZPROBLEMS);
      for (var child : children) {
        var pr = ProblemReport.decodeZooKeeperEntry(context, child);
        logProblemDeletion(pr);
      }
      context.getZooSession().asReaderWriter().recursiveDelete(ZPROBLEMS,
          ZooUtil.NodeMissingPolicy.SKIP);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Holds error message processing flags
   */
  private static class ProblemSection {
    private static final Section section =
        new Section(RESERVED_PREFIX + "err_", true, RESERVED_PREFIX + "err`", false);

    public static Range getRange() {
      return section.getRange();
    }

    public static String getRowPrefix() {
      return section.getRowPrefix();
    }
  }

  private void removeMetadataProblemReports(ServerContext context) {
    try (
        var scanner =
            context.createScanner(SystemTables.METADATA.tableName(), Authorizations.EMPTY);
        var writer = context.createBatchWriter(SystemTables.METADATA.tableName())) {
      scanner.setRange(ProblemSection.getRange());
      for (Map.Entry<Key,Value> entry : scanner) {
        var pr = ProblemReport.decodeMetadataEntry(entry.getKey(), entry.getValue());
        logProblemDeletion(pr);
        Mutation m = new Mutation(entry.getKey().getRow());
        m.putDelete(entry.getKey().getColumnFamily(), entry.getKey().getColumnQualifier());
        writer.addMutation(m);
      }
    } catch (TableNotFoundException | MutationsRejectedException e) {
      throw new IllegalStateException(e);
    }
  }

  private void logProblemDeletion(ProblemReport pr) {
    log.info(
        "Deleting problem report tableId:{} type:{} resource:{} server:{} time:{} exception:{}",
        pr.tableId, pr.problemType, pr.resource, pr.server, pr.creationTime, pr.exception);
  }

  public enum ProblemType {
    FILE_READ, FILE_WRITE, TABLET_LOAD
  }

  private static class ProblemReport {
    private final TableId tableId;
    private final ProblemType problemType;
    private final String resource;
    private String exception;
    private String server;
    private long creationTime;

    private ProblemReport(TableId table, ProblemType problemType, String resource, byte[] enc) {
      requireNonNull(table, "table is null");
      requireNonNull(problemType, "problemType is null");
      requireNonNull(resource, "resource is null");
      this.tableId = table;
      this.problemType = problemType;
      this.resource = resource;

      decode(enc);
    }

    private void decode(byte[] enc) {
      try {
        ByteArrayInputStream bais = new ByteArrayInputStream(enc);
        DataInputStream dis = new DataInputStream(bais);

        creationTime = dis.readLong();

        if (dis.readBoolean()) {
          server = dis.readUTF();
        } else {
          server = null;
        }

        if (dis.readBoolean()) {
          exception = dis.readUTF();
        } else {
          exception = null;
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    static ProblemReport decodeZooKeeperEntry(ServerContext context, String node)
        throws IOException, KeeperException, InterruptedException {
      byte[] bytes = Encoding.decodeBase64FileName(node);

      ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
      DataInputStream dis = new DataInputStream(bais);

      TableId tableId = TableId.of(dis.readUTF());
      String problemType = dis.readUTF();
      String resource = dis.readUTF();

      String zpath = ZPROBLEMS + "/" + node;
      byte[] enc = context.getZooSession().asReaderWriter().getData(zpath);

      return new ProblemReport(tableId, ProblemType.valueOf(problemType), resource, enc);

    }

    public static ProblemReport decodeMetadataEntry(Key key, Value value) {
      TableId tableId =
          TableId.of(key.getRow().toString().substring(ProblemSection.getRowPrefix().length()));
      String problemType = key.getColumnFamily().toString();
      String resource = key.getColumnQualifier().toString();

      return new ProblemReport(tableId, ProblemType.valueOf(problemType), resource, value.get());
    }
  }
}
