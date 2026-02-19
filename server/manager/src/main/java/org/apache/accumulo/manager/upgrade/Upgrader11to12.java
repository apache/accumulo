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
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.Upgrade11to12.COMPACT_COL;

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
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.clientImpl.NamespaceMapping;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.Ample.TabletsMutator;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ChoppedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ExternalCompactionColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ScanFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.RootTabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMergeabilityMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.schema.Section;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Encoding;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.core.util.compaction.CompactionServicesConfig;
import org.apache.accumulo.core.util.tables.TableNameUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.NamespacePropKey;
import org.apache.accumulo.server.conf.store.ResourceGroupPropKey;
import org.apache.accumulo.server.conf.store.SystemPropKey;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.accumulo.server.init.FileSystemInitializer;
import org.apache.accumulo.server.init.InitialConfiguration;
import org.apache.accumulo.server.util.PropUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

//TODO when removing this class, also remove MetadataSchema.Upgrader11to12
public class Upgrader11to12 implements Upgrader {

  interface MutationWriter {
    void addMutation(Mutation m) throws MutationsRejectedException;
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

  private static final Logger LOG = LoggerFactory.getLogger(Upgrader11to12.class);
  private static final String ZPROBLEMS = "/problems";
  private static final String ZTRACERS = "/tracers";
  private static final String ZTABLE_COMPACT_ID = "/compact-id";
  private static final String ZTABLE_COMPACT_CANCEL_ID = "/compact-cancel-id";
  private static final String ZTABLE_STATE = "/state";
  private static final byte[] ZERO_BYTE = {'0'};

  @SuppressWarnings("deprecation")
  private static final Text CHOPPED = ChoppedColumnFamily.NAME;

  @VisibleForTesting
  static final String ZTABLE_NAME = "/name";

  @VisibleForTesting
  static final Set<Text> UPGRADE_FAMILIES = Set.of(DataFileColumnFamily.NAME, CHOPPED,
      ExternalCompactionColumnFamily.NAME, ScanFileColumnFamily.NAME);

  @VisibleForTesting
  static final String ZNAMESPACE_NAME = "/name";

  public static final Collection<Range> OLD_SCAN_SERVERS_RANGES =
      List.of(new Range("~sserv", "~sserx"), new Range("~scanref", "~scanreg"));

  @Override
  public void upgradeZookeeper(ServerContext context) {
    LOG.info("Ensuring all worker server processes are down.");
    validateEmptyZKWorkerServerPaths(context);
    LOG.info("Removing ZTracer node");
    removeZTracersNode(context);
    LOG.info("Updating file references in root tablet");
    updateRootTabletFileReferences(context);
    LOG.info("Removing problems reports from zookeeper");
    removeZKProblemReports(context);
    LOG.info("Creating NamespaceMappings");
    createNamespaceMappings(context);
    LOG.info("Validating root and metadata compaction services");
    validateCompactionServiceConfiguration(context);
    LOG.info("Setting root table stored hosting availability");
    addHostingGoals(context, TabletAvailability.HOSTED, DataLevel.ROOT);
    LOG.info("Removing nodes no longer used from ZooKeeper");
    removeUnusedZKNodes(context);
    LOG.info("Removing compact columns from root tablet");
    removeCompactColumnsFromRootTabletMetadata(context);
    LOG.info("Adding compactions node to zookeeper");
    addCompactionsNode(context);
    LOG.info("Creating ZooKeeper entries for ScanServerRefTable");
    initializeScanRefTable(context);
    LOG.info("Creating ZooKeeper entries for accumulo.fate table");
    initializeFateTable(context);
    LOG.info("Adding table mappings to zookeeper");
    addTableMappingsToZooKeeper(context);
    LOG.info("Adding default resource group node to zookeeper");
    addDefaultResourceGroupConfigNode(context);
    LOG.info("Moving table properties from system to namespaces");
    moveTableProperties(context);
    LOG.info("Add assistant manager node");
    addAssistantManager(context);
  }

  @Override
  public void upgradeRoot(ServerContext context) {
    LOG.info("Updating file references in metadata tablets");
    upgradeTabletsMetadata(context, Ample.DataLevel.METADATA.metaTable());
    LOG.info("Looking for partial splits");
    handlePartialSplits(context, SystemTables.ROOT.tableName());
    LOG.info("setting metadata table hosting availability");
    addHostingGoals(context, TabletAvailability.HOSTED, DataLevel.METADATA);
    LOG.info("Removing MetadataBulkLoadFilter iterator from root table");
    removeMetaDataBulkLoadFilter(context, SystemTables.ROOT.tableId());
    LOG.info("Removing compact columns from metadata tablets");
    removeCompactColumnsFromTable(context, SystemTables.ROOT.tableName());
  }

  @Override
  public void upgradeMetadata(ServerContext context) {
    LOG.info("Updating file references in user tablets");
    upgradeTabletsMetadata(context, Ample.DataLevel.USER.metaTable());
    LOG.info("Removing Scan Server Range from metadata table");
    removeScanServerRanges(context);
    LOG.info("Removing problems reports from metadata table");
    removeMetadataProblemReports(context);
    LOG.info("Creating table {}", SystemTables.SCAN_REF.tableName());
    createScanRefTable(context);
    LOG.info("Creating table {}", SystemTables.FATE.tableName());
    createFateTable(context);
    LOG.info("Looking for partial splits");
    handlePartialSplits(context, SystemTables.METADATA.tableName());
    LOG.info("setting hosting availability on user tables");
    addHostingGoals(context, TabletAvailability.ONDEMAND, DataLevel.USER);
    LOG.info("Deleting external compaction final states from user tables");
    deleteExternalCompactionFinalStates(context);
    LOG.info("Deleting external compaction from user tables");
    deleteExternalCompactions(context);
    LOG.info("Removing MetadataBulkLoadFilter iterator from metadata table");
    removeMetaDataBulkLoadFilter(context, SystemTables.METADATA.tableId());
    LOG.info("Removing compact columns from user tables");
    removeCompactColumnsFromTable(context, SystemTables.METADATA.tableName());
    LOG.info("Removing bulk file columns from metadata table");
    removeBulkFileColumnsFromTable(context, SystemTables.METADATA.tableName());
  }

  private static void addAssistantManager(ServerContext context) {
    try {
      context.getZooSession().asReaderWriter().putPersistentData(Constants.ZMANAGER_ASSISTANT_LOCK,
          new byte[0], ZooUtil.NodeExistsPolicy.SKIP);
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }

  private static void addCompactionsNode(ServerContext context) {
    try {
      context.getZooSession().asReaderWriter().putPersistentData(Constants.ZCOMPACTIONS,
          new byte[0], ZooUtil.NodeExistsPolicy.SKIP);
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }

  private void createScanRefTable(ServerContext context) {
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
        LOG.error("Failed to write tablet refs to metadata table");
        throw new RuntimeException(e);
      }
    } catch (IOException e) {
      LOG.error("Problem attempting to create ScanServerRef table", e);
    }
    LOG.info("Created ScanServerRef table");
  }

  private void createFateTable(ServerContext context) {
    try {
      FileSystemInitializer initializer = new FileSystemInitializer(
          new InitialConfiguration(context.getHadoopConf(), context.getSiteConfiguration()));
      // For upgrading an existing system set to never merge. If the mergeability is changed
      // then we would look to use the thrift client to look up the current Manager time to
      // set as part of the mergeability metadata
      FileSystemInitializer.InitialTablet fateTableTableTablet =
          initializer.createFateRefTablet(context, TabletMergeabilityMetadata.never());
      // Add references to the Metadata Table
      try (BatchWriter writer = context.createBatchWriter(SystemTables.METADATA.tableName())) {
        writer.addMutation(fateTableTableTablet.createMutation());
      } catch (MutationsRejectedException | TableNotFoundException e) {
        LOG.error("Failed to write tablet refs to metadata table");
        throw new RuntimeException(e);
      }
    } catch (IOException e) {
      LOG.error("Problem attempting to create Fate table", e);
    }
    LOG.info("Created Fate table");
  }

  private void removeCompactColumnsFromRootTabletMetadata(ServerContext context) {

    try {
      var zrw = context.getZooSession().asReaderWriter();
      Stat stat = new Stat();
      byte[] rootData = zrw.getData(ZROOT_TABLET, stat);

      String json = new String(rootData, UTF_8);

      var rtm = new RootTabletMetadata(json);

      ArrayList<Mutation> mutations = new ArrayList<>();
      for (Map.Entry<Key,Value> entry : rtm.toKeyValues().entrySet()) {
        var key = entry.getKey();

        if (COMPACT_COL.hasColumns(key)) {
          var row = key.getRow();
          Preconditions.checkState(key.getColumnVisibilityData().length() == 0,
              "Expected empty visibility, saw %s ", key.getColumnVisibilityData());
          Mutation m = new Mutation(row);
          // TODO will metadata contraint fail when this is written?
          COMPACT_COL.putDelete(m);
          mutations.add(m);
        }
      }

      Preconditions.checkState(mutations.size() <= 1);

      if (!mutations.isEmpty()) {
        LOG.info("Root metadata in ZooKeeper before upgrade: {}", json);
        rtm.update(mutations.get(0));
        zrw.overwritePersistentData(ZROOT_TABLET, rtm.toJson().getBytes(UTF_8), stat.getVersion());
        LOG.info("Root metadata in ZooKeeper after upgrade: {}", rtm.toJson());
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

  private void removeCompactColumnsFromTable(ServerContext context, String tableName) {

    try (var scanner = context.createScanner(tableName, Authorizations.EMPTY);
        var writer = context.createBatchWriter(tableName)) {
      scanner.setRange(MetadataSchema.TabletsSection.getRange());
      COMPACT_COL.fetch(scanner);

      for (Map.Entry<Key,Value> entry : scanner) {
        var key = entry.getKey();
        if (COMPACT_COL.hasColumns(key)) {
          var row = key.getRow();
          Preconditions.checkState(key.getColumnVisibilityData().length() == 0,
              "Expected empty visibility, saw %s ", key.getColumnVisibilityData());
          Mutation m = new Mutation(row);
          COMPACT_COL.putDelete(m);
          writer.addMutation(m);
        }
      }
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private void removeBulkFileColumnsFromTable(ServerContext context, String tableName) {
    // FATE transaction ids have changed from 3.x to 4.x which are used as the value for the bulk
    // file column. FATE ops won't persist through upgrade, so these columns can be safely deleted
    // if they exist.
    try (var scanner = context.createScanner(tableName, Authorizations.EMPTY);
        var writer = context.createBatchWriter(tableName)) {
      scanner.setRange(MetadataSchema.TabletsSection.getRange());
      scanner.fetchColumnFamily(TabletsSection.BulkFileColumnFamily.NAME);
      for (Map.Entry<Key,Value> entry : scanner) {
        var key = entry.getKey();
        Mutation m = new Mutation(key.getRow());
        Preconditions.checkState(
            key.getColumnFamily().equals(TabletsSection.BulkFileColumnFamily.NAME),
            "Expected family %s, saw %s ", TabletsSection.BulkFileColumnFamily.NAME,
            key.getColumnFamily());
        Preconditions.checkState(key.getColumnVisibilityData().length() == 0,
            "Expected empty visibility, saw %s ", key.getColumnVisibilityData());
        m.putDelete(key.getColumnFamily(), key.getColumnQualifier());
        writer.addMutation(m);
      }
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private void removeUnusedZKNodes(ServerContext context) {
    try {
      final var zrw = context.getZooSession().asReaderWriter();

      final String ZCOORDINATOR = "/coordinators";
      final String BULK_ARBITRATOR_TYPE = "bulkTx";

      zrw.recursiveDelete(ZCOORDINATOR, ZooUtil.NodeMissingPolicy.SKIP);
      zrw.recursiveDelete("/" + BULK_ARBITRATOR_TYPE, ZooUtil.NodeMissingPolicy.SKIP);

      final String ZTABLE_COMPACT_ID = "/compact-id";
      final String ZTABLE_COMPACT_CANCEL_ID = "/compact-cancel-id";

      for (String tId : zrw.getChildren(Constants.ZTABLES)) {
        final String zTablePath = Constants.ZTABLES + "/" + tId;
        zrw.delete(zTablePath + ZTABLE_COMPACT_ID);
        zrw.delete(zTablePath + ZTABLE_COMPACT_CANCEL_ID);
      }
    } catch (KeeperException | InterruptedException e1) {
      throw new IllegalStateException(e1);
    }
  }

  private void removeMetaDataBulkLoadFilter(ServerContext context, TableId tableId) {
    final String propName = Property.TABLE_ITERATOR_PREFIX.getKey() + "majc.bulkLoadFilter";
    PropUtil.removeProperties(context, TablePropKey.of(tableId), List.of(propName));
  }

  private void deleteExternalCompactionFinalStates(ServerContext context) {
    // This metadata was only written for user tablets as part of the compaction commit process.
    // Compactions are committed in a completely different way now, so delete these entries. Its
    // possible some completed compactions may need to be redone, but processing these entries would
    // not be easy to test so its better for correctness to delete them and redo the work.
    try (
        var scanner =
            context.createScanner(SystemTables.METADATA.tableName(), Authorizations.EMPTY);
        var writer = context.createBatchWriter(SystemTables.METADATA.tableName())) {
      var section = new Section(RESERVED_PREFIX + "ecomp", true, RESERVED_PREFIX + "ecomq", false);
      scanner.setRange(section.getRange());

      for (Map.Entry<Key,Value> entry : scanner) {
        var key = entry.getKey();
        var row = key.getRow();
        Preconditions.checkState(row.toString().startsWith(section.getRowPrefix()));
        Mutation m = new Mutation(row);
        Preconditions.checkState(key.getColumnVisibilityData().length() == 0,
            "Expected empty visibility, saw %s ", key.getColumnVisibilityData());
        m.putDelete(key.getColumnFamily(), key.getColumnQualifier());
        writer.addMutation(m);
      }
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private void addHostingGoals(ServerContext context, TabletAvailability availability,
      DataLevel level) {
    try (
        TabletsMetadata tm =
            context.getAmple().readTablets().forLevel(level).fetch(ColumnType.PREV_ROW).build();
        TabletsMutator mut = context.getAmple().mutateTablets()) {
      tm.forEach(t -> mut.mutateTablet(t.getExtent()).putTabletAvailability(availability).mutate());
    }
  }

  private void deleteExternalCompactions(ServerContext context) {
    // External compactions were only written for user tablets in 3.x and earlier, so only need to
    // process the metadata table. The metadata related to an external compaction has changed so
    // delete any that exists. Not using Ample in case there are problems deserializing the old
    // external compaction metadata.
    try (
        var scanner =
            context.createScanner(SystemTables.METADATA.tableName(), Authorizations.EMPTY);
        var writer = context.createBatchWriter(SystemTables.METADATA.tableName())) {
      scanner.setRange(TabletsSection.getRange());
      scanner.fetchColumnFamily(ExternalCompactionColumnFamily.NAME);

      for (Map.Entry<Key,Value> entry : scanner) {
        var key = entry.getKey();
        Mutation m = new Mutation(key.getRow());
        Preconditions.checkState(key.getColumnFamily().equals(ExternalCompactionColumnFamily.NAME),
            "Expected family %s, saw %s ", ExternalCompactionColumnFamily.NAME,
            key.getColumnFamily());
        Preconditions.checkState(key.getColumnVisibilityData().length() == 0,
            "Expected empty visibility, saw %s ", key.getColumnVisibilityData());
        m.putDelete(key.getColumnFamily(), key.getColumnQualifier());
        writer.addMutation(m);
      }
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private void handlePartialSplits(ServerContext context, String table) {
    try (var scanner = context.createScanner(table, Authorizations.EMPTY)) {
      scanner.setRange(TabletsSection.getRange());
      TabletsSection.Upgrade11to12.SPLIT_RATIO_COLUMN.fetch(scanner);

      for (var entry : scanner) {
        SplitRecovery11to12.fixSplit(context, entry.getKey().getRow());
      }
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private void validateEmptyZKWorkerServerPaths(ServerContext context) {
    // #4861 added the resource group to the compactor, sserver, tserver
    // and dead tserver zookeeper paths. Make sure that the these paths
    // are empty. This means that for the Accumulo 4.0 upgrade, the Manager
    // should be started first before any other process.
    final ZooReader zr = context.getZooSession().asReader();
    for (String serverPath : new String[] {Constants.ZCOMPACTORS, Constants.ZSSERVERS,
        Constants.ZTSERVERS, Constants.ZDEADTSERVERS}) {
      try {
        List<String> children = zr.getChildren(serverPath);
        for (String child : children) {
          if (child.contains(":")) {
            String childPath = serverPath + "/" + child;
            if (zr.getChildren(childPath).isEmpty()) {
              // child is likely host:port and is an empty directory. Since there
              // is no lock here, then the server is likely down (or should be).
              // Remove the entry and move on.
              context.getZooSession().asReaderWriter().recursiveDelete(childPath,
                  NodeMissingPolicy.SKIP);
            } else {
              throw new IllegalStateException("Found server address at " + serverPath + "/" + child
                  + " with content in the directory. Was expecting either a nothing, a resource group name or an empty directory."
                  + " Stop any referenced servers.");
            }
          }
        }
      } catch (InterruptedException | KeeperException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  public static void preparePre4_0NewTableState(ServerContext context, TableId tableId,
      NamespaceId namespaceId, String tableName, TableState state, NodeExistsPolicy existsPolicy)
      throws KeeperException, InterruptedException {
    // state gets created last
    LOG.debug("Creating ZooKeeper entries for new table {} (ID: {}) in namespace (ID: {})",
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

  @VisibleForTesting
  void initializeScanRefTable(ServerContext context) {
    try {
      preparePre4_0NewTableState(context, SystemTables.SCAN_REF.tableId(), Namespace.ACCUMULO.id(),
          SystemTables.SCAN_REF.tableName(), TableState.ONLINE, ZooUtil.NodeExistsPolicy.FAIL);
    } catch (InterruptedException | KeeperException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Error creating scanref table", ex);
    }
  }

  private void initializeFateTable(ServerContext context) {
    try {
      preparePre4_0NewTableState(context, SystemTables.FATE.tableId(), Namespace.ACCUMULO.id(),
          SystemTables.FATE.tableName(), TableState.ONLINE, ZooUtil.NodeExistsPolicy.FAIL);
    } catch (InterruptedException | KeeperException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Error creating fate table", ex);
    }
  }

  void addTableMappingsToZooKeeper(ServerContext context) {
    var zrw = context.getZooSession().asReaderWriter();
    try {
      List<String> tableIds = zrw.getChildren(Constants.ZTABLES);
      Map<String,Map<String,String>> mapOfTableMaps = new HashMap<>();

      for (String tableId : tableIds) {
        var tableName =
            new String(zrw.getData(Constants.ZTABLES + "/" + tableId + ZTABLE_NAME), UTF_8);
        var namespaceId = new String(
            zrw.getData(Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_NAMESPACE), UTF_8);
        mapOfTableMaps.computeIfAbsent(namespaceId, k -> new HashMap<>()).compute(tableId,
            (tid, existingName) -> {
              if (existingName != null) {
                throw new IllegalStateException(
                    "Table id " + tid + " already present in map for namespace id " + namespaceId);
              }
              return tableName;
            });
      }
      // Ensure the default namespace table mapping node gets created
      // in case there are not tables in the default namespace
      mapOfTableMaps.putIfAbsent(Namespace.DEFAULT.id().canonical(), new HashMap<>());
      for (Map.Entry<String,Map<String,String>> entry : mapOfTableMaps.entrySet()) {
        zrw.putPersistentData(Constants.ZNAMESPACES + "/" + entry.getKey() + Constants.ZTABLES,
            NamespaceMapping.serializeMap(entry.getValue()), ZooUtil.NodeExistsPolicy.FAIL);
      }
      for (String tableId : tableIds) {
        String tableNamePath = Constants.ZTABLES + "/" + tableId + ZTABLE_NAME;
        zrw.delete(tableNamePath);
      }
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Could not read metadata from ZooKeeper due to interrupt",
          ex);
    } catch (KeeperException ex) {
      throw new IllegalStateException(
          "Could not read or write metadata in ZooKeeper because of ZooKeeper exception", ex);
    }
  }

  private void validateCompactionServiceConfiguration(ServerContext ctx) {

    final String compactionSvcKey = Property.TABLE_COMPACTION_DISPATCHER_OPTS.getKey() + "service";
    final Map<String,String> compactionPlanners =
        new CompactionServicesConfig(ctx.getConfiguration()).getPlanners();

    for (TableId tid : new TableId[] {SystemTables.ROOT.tableId(),
        SystemTables.METADATA.tableId()}) {

      final TablePropKey tpk = TablePropKey.of(tid);
      final VersionedProperties tableProps = ctx.getPropStore().get(tpk);
      final String value = tableProps.asMap().get(compactionSvcKey);

      if (value != null) {
        if (tid.equals(SystemTables.ROOT.tableId()) && value.equals("root")
            && !compactionPlanners.containsKey(value)) {
          LOG.warn(
              "Compaction service \"root\" in configuration for root table, but is not defined. "
                  + "Modifying root table configuration to use the default compaction service configuration");
          ctx.getPropStore().removeProperties(tpk, Set.of(compactionSvcKey));
        } else if (tid.equals(SystemTables.METADATA.tableId()) && value.equals("meta")
            && !compactionPlanners.containsKey(value)) {
          LOG.warn(
              "Compaction service \"meta\" in configuration for metadata table, but is not defined. "
                  + "Modifying metadata table configuration to use the default compaction service configuration");
          ctx.getPropStore().removeProperties(tpk, Set.of(compactionSvcKey));
        }
      }

    }
  }

  @VisibleForTesting
  void removeZKProblemReports(ServerContext context) {
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
    LOG.info(
        "Deleting problem report tableId:{} type:{} resource:{} server:{} time:{} exception:{}",
        pr.tableId, pr.problemType, pr.resource, pr.server, pr.creationTime, pr.exception);
  }

  @VisibleForTesting
  void removeZTracersNode(ServerContext context) {
    try {
      context.getZooSession().asReaderWriter().recursiveDelete(ZTRACERS,
          ZooUtil.NodeMissingPolicy.SKIP);
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException("Error removing ZTRACERS node", e);
    }
  }

  @VisibleForTesting
  static void upgradeDataFileCF(final Key key, final Value value, final Mutation m) {
    String file = key.getColumnQualifier().toString();
    // filter out references if they are in the correct format already.
    boolean needsConversion = StoredTabletFile.fileNeedsConversion(file);
    LOG.trace("file: {} needs conversion: {}", file, needsConversion);
    if (needsConversion) {
      var fileJson = StoredTabletFile.of(new Path(file)).getMetadataText();
      m.at().family(DataFileColumnFamily.STR_NAME).qualifier(fileJson).put(value);
      m.at().family(DataFileColumnFamily.STR_NAME).qualifier(file).delete();
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
          if (LOG.isTraceEnabled()) {
            LOG.trace("table: {}, update: {}", tableName, update.prettyPrint());
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
          LOG.debug("Deleting scan reference from:{}. Ref: {}", tableName, key.getRow());
          update.at().family(ScanFileColumnFamily.NAME).qualifier(key.getColumnQualifier())
              .delete();
        } else if (family.equals(CHOPPED)) {
          LOG.warn(
              "Deleting chopped reference from:{}. Previous split or delete may not have completed cleanly. Ref: {}",
              tableName, key.getRow());
          update.at().family(CHOPPED).qualifier(CHOPPED).delete();
        } else if (family.equals(ExternalCompactionColumnFamily.NAME)) {
          LOG.debug(
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
        LOG.trace("table: {}, update: {}", tableName, update.prettyPrint());
        batchWriter.addMutation(update);
      }
    } catch (MutationsRejectedException mex) {
      LOG.warn("Failed to update reference for table: " + tableName);
      LOG.warn("Constraint violations: {}", mex.getConstraintViolationSummaries());
      throw new IllegalStateException("Failed to process table: " + tableName, mex);
    }
  }

  @VisibleForTesting
  void updateRootTabletFileReferences(ServerContext context) {
    try {
      var zrw = context.getZooSession().asReaderWriter();
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
        LOG.info("Root metadata in ZooKeeper before upgrade: {}", json);
        rtm.update(mutations.get(0));
        zrw.overwritePersistentData(ZROOT_TABLET, rtm.toJson().getBytes(UTF_8), stat.getVersion());
        LOG.info("Root metadata in ZooKeeper after upgrade: {}", rtm.toJson());
      }
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException("Error upgrading file references in root tablet", e);
    }
  }

  @VisibleForTesting
  void createNamespaceMappings(ServerContext context) {
    try {
      var zrw = context.getZooSession().asReaderWriter();
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
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException("Error creating namespace mappings", e);
    }
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
      LOG.warn("Failed to update reference for table: " + metaName);
      LOG.warn("Constraint violations: {}", mex.getConstraintViolationSummaries());
      throw new IllegalStateException("Failed to process table: " + metaName, mex);
    }
  }

  public void removeScanServerRanges(ServerContext context) {
    try (BatchDeleter batchDeleter =
        context.createBatchDeleter(Ample.DataLevel.USER.metaTable(), Authorizations.EMPTY, 4)) {
      batchDeleter.setRanges(OLD_SCAN_SERVERS_RANGES);
      batchDeleter.delete();
    } catch (TableNotFoundException | MutationsRejectedException e) {
      throw new RuntimeException(e);
    }
  }

  private void addDefaultResourceGroupConfigNode(ServerContext context) {
    try {
      ResourceGroupPropKey.DEFAULT.createZNode(context.getZooSession().asReaderWriter());
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException("Error creating default resource group config node", e);
    }
  }

  void moveTableProperties(ServerContext context) {
    final SystemPropKey spk = SystemPropKey.of();
    final VersionedProperties sysProps = context.getPropStore().get(spk);
    final Map<String,String> sysTableProps = new HashMap<>();
    sysProps.asMap().entrySet().stream()
        .filter(e -> e.getKey().startsWith(Property.TABLE_PREFIX.getKey()))
        .forEach(e -> sysTableProps.put(e.getKey(), e.getValue()));
    LOG.info("Adding the following table properties to namespaces unless overridden:");
    sysTableProps.forEach((k, v) -> LOG.info("{} -> {}", k, v));

    context.getNamespaceMapping().getIdToNameMap().forEach((nsid, ns) -> {
      final NamespacePropKey nsk = NamespacePropKey.of(nsid);
      final Map<String,String> nsProps = context.getPropStore().get(nsk).asMap();
      final Map<String,String> nsPropAdditions = new HashMap<>();

      for (Entry<String,String> e : sysTableProps.entrySet()) {

        // Don't move iterators or constraints from the system configuration
        // to the system namespace. This will affect the root and metadata
        // tables.
        if (ns.equals(Namespace.ACCUMULO.name())
            && (e.getKey().startsWith(Property.TABLE_ITERATOR_PREFIX.getKey())
                || e.getKey().startsWith(Property.TABLE_CONSTRAINT_PREFIX.getKey()))) {
          LOG.debug(
              "Not moving property {} to 'accumulo' namespace, iterator and constraint properties are ignored on purpose.",
              e.getKey());
          continue;
        }

        final String nsVal = nsProps.get(e.getKey());
        // If it's not set, then add the system table property
        // to the namespace. If it is set, then it doesnt matter
        // what the value is, we can ignore it.
        if (nsVal == null) {
          nsPropAdditions.put(e.getKey(), e.getValue());
        }
      }
      context.getPropStore().putAll(nsk, nsPropAdditions);
      LOG.debug("Added table properties to namespace '{}' id:{}:", ns, nsid);
      nsPropAdditions.forEach((k, v) -> LOG.debug("{} -> {}", k, v));
      LOG.info("Namespace '{}' id:{} completed.", ns, nsid);
    });

    LOG.info("Removing table properties from system configuration.");
    context.getPropStore().removeProperties(spk, sysTableProps.keySet());

    LOG.info(
        "Moving table properties from system configuration to namespace configurations complete.");
  }
}
