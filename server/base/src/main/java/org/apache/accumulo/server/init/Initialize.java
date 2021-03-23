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
package org.apache.accumulo.server.init;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.TIME_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.IteratorSetting.Column;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.master.thrift.ManagerGoalState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.ReplicationSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.FutureLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataTime;
import org.apache.accumulo.core.metadata.schema.RootTabletMetadata;
import org.apache.accumulo.core.replication.ReplicationConstants;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.ReplicationSchema.WorkSection;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.singletons.SingletonManager;
import org.apache.accumulo.core.singletons.SingletonManager.Mode;
import org.apache.accumulo.core.spi.compaction.SimpleCompactionDispatcher;
import org.apache.accumulo.core.spi.fs.VolumeChooserEnvironment;
import org.apache.accumulo.core.spi.fs.VolumeChooserEnvironment.Scope;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.volume.VolumeConfiguration;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServerUtil;
import org.apache.accumulo.server.constraints.MetadataConstraints;
import org.apache.accumulo.server.fs.VolumeChooserEnvironmentImpl;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.iterators.MetadataBulkLoadFilter;
import org.apache.accumulo.server.log.WalStateManager;
import org.apache.accumulo.server.metadata.RootGcCandidates;
import org.apache.accumulo.server.replication.ReplicationUtil;
import org.apache.accumulo.server.replication.StatusCombiner;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.accumulo.server.util.ReplicationTableUtil;
import org.apache.accumulo.server.util.SystemPropUtil;
import org.apache.accumulo.server.util.TablePropUtil;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.utils.InfoCmp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;
import com.google.common.base.Joiner;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * This class is used to setup the directory structure and the root tablet to get an instance
 * started
 */
@SuppressFBWarnings(value = "DM_EXIT", justification = "CLI utility can exit")
@AutoService(KeywordExecutable.class)
public class Initialize implements KeywordExecutable {
  private static final Logger log = LoggerFactory.getLogger(Initialize.class);
  private static final String DEFAULT_ROOT_USER = "root";
  private static final String TABLE_TABLETS_TABLET_DIR = "table_info";

  private static LineReader reader = null;
  private static ZooReaderWriter zoo = null;

  private static LineReader getLineReader() throws IOException {
    if (reader == null) {
      reader = LineReaderBuilder.builder().build();
    }
    return reader;
  }

  /**
   * Sets this class's ZooKeeper reader/writer.
   *
   * @param zooReaderWriter
   *          reader/writer
   */
  static void setZooReaderWriter(ZooReaderWriter zooReaderWriter) {
    zoo = zooReaderWriter;
  }

  /**
   * Gets this class's ZooKeeper reader/writer.
   *
   * @return reader/writer
   */
  static ZooReaderWriter getZooReaderWriter() {
    return zoo;
  }

  // config only for root table
  private static HashMap<String,String> initialRootConf = new HashMap<>();
  // config for root and metadata table
  private static HashMap<String,String> initialRootMetaConf = new HashMap<>();
  // config for only metadata table
  private static HashMap<String,String> initialMetaConf = new HashMap<>();
  private static HashMap<String,String> initialReplicationTableConf = new HashMap<>();

  static {
    initialRootConf.put(Property.TABLE_COMPACTION_DISPATCHER.getKey(),
        SimpleCompactionDispatcher.class.getName());
    initialRootConf.put(Property.TABLE_COMPACTION_DISPATCHER_OPTS.getKey() + "service", "root");

    initialRootMetaConf.put(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), "32K");
    initialRootMetaConf.put(Property.TABLE_FILE_REPLICATION.getKey(), "5");
    initialRootMetaConf.put(Property.TABLE_DURABILITY.getKey(), "sync");
    initialRootMetaConf.put(Property.TABLE_MAJC_RATIO.getKey(), "1");
    initialRootMetaConf.put(Property.TABLE_SPLIT_THRESHOLD.getKey(), "64M");
    initialRootMetaConf.put(Property.TABLE_CONSTRAINT_PREFIX.getKey() + "1",
        MetadataConstraints.class.getName());
    initialRootMetaConf.put(Property.TABLE_ITERATOR_PREFIX.getKey() + "scan.vers",
        "10," + VersioningIterator.class.getName());
    initialRootMetaConf.put(Property.TABLE_ITERATOR_PREFIX.getKey() + "scan.vers.opt.maxVersions",
        "1");
    initialRootMetaConf.put(Property.TABLE_ITERATOR_PREFIX.getKey() + "minc.vers",
        "10," + VersioningIterator.class.getName());
    initialRootMetaConf.put(Property.TABLE_ITERATOR_PREFIX.getKey() + "minc.vers.opt.maxVersions",
        "1");
    initialRootMetaConf.put(Property.TABLE_ITERATOR_PREFIX.getKey() + "majc.vers",
        "10," + VersioningIterator.class.getName());
    initialRootMetaConf.put(Property.TABLE_ITERATOR_PREFIX.getKey() + "majc.vers.opt.maxVersions",
        "1");
    initialRootMetaConf.put(Property.TABLE_ITERATOR_PREFIX.getKey() + "majc.bulkLoadFilter",
        "20," + MetadataBulkLoadFilter.class.getName());
    initialRootMetaConf.put(Property.TABLE_FAILURES_IGNORE.getKey(), "false");
    initialRootMetaConf.put(Property.TABLE_LOCALITY_GROUP_PREFIX.getKey() + "tablet",
        String.format("%s,%s", TabletColumnFamily.NAME, CurrentLocationColumnFamily.NAME));
    initialRootMetaConf.put(Property.TABLE_LOCALITY_GROUP_PREFIX.getKey() + "server",
        String.format("%s,%s,%s,%s", DataFileColumnFamily.NAME, LogColumnFamily.NAME,
            ServerColumnFamily.NAME, FutureLocationColumnFamily.NAME));
    initialRootMetaConf.put(Property.TABLE_LOCALITY_GROUPS.getKey(), "tablet,server");
    initialRootMetaConf.put(Property.TABLE_DEFAULT_SCANTIME_VISIBILITY.getKey(), "");
    initialRootMetaConf.put(Property.TABLE_INDEXCACHE_ENABLED.getKey(), "true");
    initialRootMetaConf.put(Property.TABLE_BLOCKCACHE_ENABLED.getKey(), "true");

    initialMetaConf.put(Property.TABLE_COMPACTION_DISPATCHER.getKey(),
        SimpleCompactionDispatcher.class.getName());
    initialMetaConf.put(Property.TABLE_COMPACTION_DISPATCHER_OPTS.getKey() + "service", "meta");

    // ACCUMULO-3077 Set the combiner on accumulo.metadata during init to reduce the likelihood of a
    // race condition where a tserver compacts away Status updates because it didn't see the
    // Combiner
    // configured
    IteratorSetting setting =
        new IteratorSetting(9, ReplicationTableUtil.COMBINER_NAME, StatusCombiner.class);
    Combiner.setColumns(setting, Collections.singletonList(new Column(ReplicationSection.COLF)));
    for (IteratorScope scope : IteratorScope.values()) {
      String root = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX,
          scope.name().toLowerCase(), setting.getName());
      for (Entry<String,String> prop : setting.getOptions().entrySet()) {
        initialMetaConf.put(root + ".opt." + prop.getKey(), prop.getValue());
      }
      initialMetaConf.put(root, setting.getPriority() + "," + setting.getIteratorClass());
    }

    // add combiners to replication table
    setting = new IteratorSetting(30, ReplicationTable.COMBINER_NAME, StatusCombiner.class);
    setting.setPriority(30);
    Combiner.setColumns(setting,
        Arrays.asList(new Column(StatusSection.NAME), new Column(WorkSection.NAME)));
    for (IteratorScope scope : EnumSet.allOf(IteratorScope.class)) {
      String root = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX,
          scope.name().toLowerCase(), setting.getName());
      for (Entry<String,String> prop : setting.getOptions().entrySet()) {
        initialReplicationTableConf.put(root + ".opt." + prop.getKey(), prop.getValue());
      }
      initialReplicationTableConf.put(root,
          setting.getPriority() + "," + setting.getIteratorClass());
    }
    // add locality groups to replication table
    for (Entry<String,Set<Text>> g : ReplicationTable.LOCALITY_GROUPS.entrySet()) {
      initialReplicationTableConf.put(Property.TABLE_LOCALITY_GROUP_PREFIX + g.getKey(),
          LocalityGroupUtil.encodeColumnFamilies(g.getValue()));
    }
    initialReplicationTableConf.put(Property.TABLE_LOCALITY_GROUPS.getKey(),
        Joiner.on(",").join(ReplicationTable.LOCALITY_GROUPS.keySet()));
    // add formatter to replication table
    initialReplicationTableConf.put(Property.TABLE_FORMATTER_CLASS.getKey(),
        ReplicationUtil.STATUS_FORMATTER_CLASS_NAME);
  }

  static boolean checkInit(VolumeManager fs, SiteConfiguration sconf, Configuration hadoopConf)
      throws IOException {
    log.info("Hadoop Filesystem is {}", FileSystem.getDefaultUri(hadoopConf));
    log.info("Accumulo data dirs are {}", Arrays.asList(VolumeConfiguration.getVolumeUris(sconf)));
    log.info("Zookeeper server is {}", sconf.get(Property.INSTANCE_ZK_HOST));
    log.info("Checking if Zookeeper is available. If this hangs, then you need"
        + " to make sure zookeeper is running");
    if (!zookeeperAvailable()) {
      // ACCUMULO-3651 Changed level to error and added FATAL to message for slf4j compatibility
      log.error("FATAL Zookeeper needs to be up and running in order to init. Exiting ...");
      return false;
    }
    if (sconf.get(Property.INSTANCE_SECRET).equals(Property.INSTANCE_SECRET.getDefaultValue())) {
      LineReader c = getLineReader();
      var w = c.getTerminal().writer();
      c.getTerminal().puts(InfoCmp.Capability.bell);
      w.println();
      w.println();
      w.println("Warning!!! Your instance secret is still set to the default,"
          + " this is not secure. We highly recommend you change it.");
      w.println();
      w.println();
      w.println("You can change the instance secret in accumulo by using:");
      w.println("   bin/accumulo " + org.apache.accumulo.server.util.ChangeSecret.class.getName());
      w.println("You will also need to edit your secret in your configuration"
          + " file by adding the property instance.secret to your"
          + " accumulo.properties. Without this accumulo will not operate" + " correctly");
    }
    try {
      if (isInitialized(fs, sconf)) {
        printInitializeFailureMessages(sconf);
        return false;
      }
    } catch (IOException e) {
      throw new IOException("Failed to check if filesystem already initialized", e);
    }

    return true;
  }

  static void printInitializeFailureMessages(SiteConfiguration sconf) {
    log.error("It appears the directories {}",
        VolumeConfiguration.getVolumeUris(sconf) + " were previously initialized.");
    log.error("Change the property {} to use different volumes.",
        Property.INSTANCE_VOLUMES.getKey());
    log.error("The current value of {} is |{}|", Property.INSTANCE_VOLUMES.getKey(),
        sconf.get(Property.INSTANCE_VOLUMES));
  }

  public boolean doInit(SiteConfiguration siteConfig, Opts opts, Configuration hadoopConf,
      VolumeManager fs) throws IOException {
    if (!checkInit(fs, siteConfig, hadoopConf)) {
      return false;
    }

    // prompt user for instance name and root password early, in case they
    // abort, we don't leave an inconsistent HDFS/ZooKeeper structure
    String instanceNamePath;
    try {
      instanceNamePath = getInstanceNamePath(opts);
    } catch (Exception e) {
      log.error("FATAL: Failed to talk to zookeeper", e);
      return false;
    }

    String rootUser;
    try {
      rootUser = getRootUserName(siteConfig, opts);
    } catch (Exception e) {
      log.error("FATAL: Failed to obtain user for administrative privileges");
      return false;
    }

    // Don't prompt for a password when we're running SASL(Kerberos)
    if (siteConfig.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
      opts.rootpass = UUID.randomUUID().toString().getBytes(UTF_8);
    } else {
      opts.rootpass = getRootPassword(siteConfig, opts, rootUser);
    }

    UUID uuid = UUID.randomUUID();
    // the actual disk locations of the root table and tablets
    Set<String> configuredVolumes = VolumeConfiguration.getVolumeUris(siteConfig);
    String instanceName = instanceNamePath.substring(getInstanceNamePrefix().length());

    try (ServerContext context =
        ServerContext.initialize(siteConfig, instanceName, uuid.toString())) {
      VolumeChooserEnvironment chooserEnv =
          new VolumeChooserEnvironmentImpl(Scope.INIT, RootTable.ID, null, context);
      String rootTabletDirName = RootTable.ROOT_TABLET_DIR_NAME;
      String ext = FileOperations.getNewFileExtension(DefaultConfiguration.getInstance());
      String rootTabletFileUri = new Path(fs.choose(chooserEnv, configuredVolumes) + Path.SEPARATOR
          + ServerConstants.TABLE_DIR + Path.SEPARATOR + RootTable.ID + Path.SEPARATOR
          + rootTabletDirName + Path.SEPARATOR + "00000_00000." + ext).toString();

      try {
        initZooKeeper(opts, uuid.toString(), instanceNamePath, rootTabletDirName,
            rootTabletFileUri);
      } catch (Exception e) {
        log.error("FATAL: Failed to initialize zookeeper", e);
        return false;
      }

      try {
        initFileSystem(siteConfig, hadoopConf, fs, uuid,
            new Path(fs.choose(chooserEnv, configuredVolumes) + Path.SEPARATOR
                + ServerConstants.TABLE_DIR + Path.SEPARATOR + RootTable.ID + rootTabletDirName)
                    .toString(),
            rootTabletFileUri, context);
      } catch (Exception e) {
        log.error("FATAL Failed to initialize filesystem", e);
        return false;
      }

      // When we're using Kerberos authentication, we need valid credentials to perform
      // initialization. If the user provided some, use them.
      // If they did not, fall back to the credentials present in accumulo.properties that the
      // servers will use themselves.
      try {
        if (siteConfig.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
          final UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
          // We don't have any valid creds to talk to HDFS
          if (!ugi.hasKerberosCredentials()) {
            final String accumuloKeytab = siteConfig.get(Property.GENERAL_KERBEROS_KEYTAB),
                accumuloPrincipal = siteConfig.get(Property.GENERAL_KERBEROS_PRINCIPAL);

            // Fail if the site configuration doesn't contain appropriate credentials to login as
            // servers
            if (StringUtils.isBlank(accumuloKeytab) || StringUtils.isBlank(accumuloPrincipal)) {
              log.error("FATAL: No Kerberos credentials provided, and Accumulo is"
                  + " not properly configured for server login");
              return false;
            }

            log.info("Logging in as {} with {}", accumuloPrincipal, accumuloKeytab);

            // Login using the keytab as the 'accumulo' user
            UserGroupInformation.loginUserFromKeytab(accumuloPrincipal, accumuloKeytab);
          }
        }
      } catch (IOException e) {
        log.error("FATAL: Failed to get the Kerberos user", e);
        return false;
      }

      try {
        initSecurity(context, opts, rootUser);
      } catch (Exception e) {
        log.error("FATAL: Failed to initialize security", e);
        return false;
      }

      if (opts.uploadAccumuloProps) {
        try {
          log.info("Uploading properties in accumulo.properties to Zookeeper."
              + " Properties that cannot be set in Zookeeper will be skipped:");
          Map<String,String> entries = new TreeMap<>();
          siteConfig.getProperties(entries, x -> true, false);
          for (Map.Entry<String,String> entry : entries.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (Property.isValidZooPropertyKey(key)) {
              SystemPropUtil.setSystemProperty(context, key, value);
              log.info("Uploaded - {} = {}", key, Property.isSensitive(key) ? "<hidden>" : value);
            } else {
              log.info("Skipped - {} = {}", key, Property.isSensitive(key) ? "<hidden>" : value);
            }
          }
        } catch (Exception e) {
          log.error("FATAL: Failed to upload accumulo.properties to Zookeeper", e);
          return false;
        }
      }

      return true;
    }
  }

  private static boolean zookeeperAvailable() {
    try {
      return zoo.exists("/");
    } catch (KeeperException | InterruptedException e) {
      return false;
    }
  }

  private static void initDirs(VolumeManager fs, UUID uuid, Set<String> baseDirs, boolean print)
      throws IOException {
    for (String baseDir : baseDirs) {
      fs.mkdirs(new Path(new Path(baseDir, ServerConstants.VERSION_DIR),
          "" + ServerConstants.DATA_VERSION), new FsPermission("700"));

      // create an instance id
      Path iidLocation = new Path(baseDir, ServerConstants.INSTANCE_ID_DIR);
      fs.mkdirs(iidLocation);
      fs.createNewFile(new Path(iidLocation, uuid.toString()));
      if (print) {
        log.info("Initialized volume {}", baseDir);
      }
    }
  }

  private void initFileSystem(SiteConfiguration siteConfig, Configuration hadoopConf,
      VolumeManager fs, UUID uuid, String rootTabletDirUri, String rootTabletFileUri,
      ServerContext serverContext) throws IOException {
    initDirs(fs, uuid, VolumeConfiguration.getVolumeUris(siteConfig), false);

    // initialize initial system tables config in zookeeper
    initSystemTablesConfig(zoo, Constants.ZROOT + "/" + uuid, hadoopConf);

    Text splitPoint = TabletsSection.getRange().getEndKey().getRow();

    VolumeChooserEnvironment chooserEnv =
        new VolumeChooserEnvironmentImpl(Scope.INIT, MetadataTable.ID, splitPoint, serverContext);
    String tableMetadataTabletDirName = TABLE_TABLETS_TABLET_DIR;
    String tableMetadataTabletDirUri =
        fs.choose(chooserEnv, ServerConstants.getBaseUris(siteConfig, hadoopConf))
            + Constants.HDFS_TABLES_DIR + Path.SEPARATOR + MetadataTable.ID + Path.SEPARATOR
            + tableMetadataTabletDirName;
    chooserEnv =
        new VolumeChooserEnvironmentImpl(Scope.INIT, ReplicationTable.ID, null, serverContext);
    String replicationTableDefaultTabletDirName = ServerColumnFamily.DEFAULT_TABLET_DIR_NAME;
    String replicationTableDefaultTabletDirUri =
        fs.choose(chooserEnv, ServerConstants.getBaseUris(siteConfig, hadoopConf))
            + Constants.HDFS_TABLES_DIR + Path.SEPARATOR + ReplicationTable.ID + Path.SEPARATOR
            + replicationTableDefaultTabletDirName;
    chooserEnv =
        new VolumeChooserEnvironmentImpl(Scope.INIT, MetadataTable.ID, null, serverContext);
    String defaultMetadataTabletDirName = ServerColumnFamily.DEFAULT_TABLET_DIR_NAME;
    String defaultMetadataTabletDirUri =
        fs.choose(chooserEnv, ServerConstants.getBaseUris(siteConfig, hadoopConf))
            + Constants.HDFS_TABLES_DIR + Path.SEPARATOR + MetadataTable.ID + Path.SEPARATOR
            + defaultMetadataTabletDirName;

    // create table and default tablets directories
    createDirectories(fs, rootTabletDirUri, tableMetadataTabletDirUri, defaultMetadataTabletDirUri,
        replicationTableDefaultTabletDirUri);

    String ext = FileOperations.getNewFileExtension(DefaultConfiguration.getInstance());

    // populate the metadata tables tablet with info about the replication table's one initial
    // tablet
    String metadataFileName = tableMetadataTabletDirUri + Path.SEPARATOR + "0_1." + ext;
    Tablet replicationTablet =
        new Tablet(ReplicationTable.ID, replicationTableDefaultTabletDirName, null, null);
    createMetadataFile(fs, metadataFileName, siteConfig, replicationTablet);

    // populate the root tablet with info about the metadata table's two initial tablets
    Tablet tablesTablet = new Tablet(MetadataTable.ID, tableMetadataTabletDirName, null, splitPoint,
        metadataFileName);
    Tablet defaultTablet =
        new Tablet(MetadataTable.ID, defaultMetadataTabletDirName, splitPoint, null);
    createMetadataFile(fs, rootTabletFileUri, siteConfig, tablesTablet, defaultTablet);
  }

  private static class Tablet {
    TableId tableId;
    String dirName;
    Text prevEndRow, endRow;
    String[] files;

    Tablet(TableId tableId, String dirName, Text prevEndRow, Text endRow, String... files) {
      this.tableId = tableId;
      this.dirName = dirName;
      this.prevEndRow = prevEndRow;
      this.endRow = endRow;
      this.files = files;
    }
  }

  private static void createMetadataFile(VolumeManager volmanager, String fileName,
      AccumuloConfiguration conf, Tablet... tablets) throws IOException {
    // sort file contents in memory, then play back to the file
    TreeMap<Key,Value> sorted = new TreeMap<>();
    for (Tablet tablet : tablets) {
      createEntriesForTablet(sorted, tablet);
    }
    FileSystem fs = volmanager.getFileSystemByPath(new Path(fileName));

    FileSKVWriter tabletWriter = FileOperations.getInstance().newWriterBuilder()
        .forFile(fileName, fs, fs.getConf()).withTableConfiguration(conf).build();
    tabletWriter.startDefaultLocalityGroup();

    for (Entry<Key,Value> entry : sorted.entrySet()) {
      tabletWriter.append(entry.getKey(), entry.getValue());
    }

    tabletWriter.close();
  }

  private static void createEntriesForTablet(TreeMap<Key,Value> map, Tablet tablet) {
    Value EMPTY_SIZE = new DataFileValue(0, 0).encodeAsValue();
    Text extent = new Text(TabletsSection.encodeRow(tablet.tableId, tablet.endRow));
    addEntry(map, extent, DIRECTORY_COLUMN, new Value(tablet.dirName));
    addEntry(map, extent, TIME_COLUMN, new Value(new MetadataTime(0, TimeType.LOGICAL).encode()));
    addEntry(map, extent, PREV_ROW_COLUMN, TabletColumnFamily.encodePrevEndRow(tablet.prevEndRow));
    for (String file : tablet.files) {
      addEntry(map, extent, new ColumnFQ(DataFileColumnFamily.NAME, new Text(file)), EMPTY_SIZE);
    }
  }

  private static void addEntry(TreeMap<Key,Value> map, Text row, ColumnFQ col, Value value) {
    map.put(new Key(row, col.getColumnFamily(), col.getColumnQualifier(), 0), value);
  }

  private static void createDirectories(VolumeManager fs, String... dirs) throws IOException {
    for (String s : dirs) {
      Path dir = new Path(s);
      try {
        FileStatus fstat = fs.getFileStatus(dir);
        if (!fstat.isDirectory()) {
          log.error("FATAL: location {} exists but is not a directory", dir);
          return;
        }
      } catch (FileNotFoundException fnfe) {
        // attempt to create directory, since it doesn't exist
        if (!fs.mkdirs(dir)) {
          log.error("FATAL: unable to create directory {}", dir);
          return;
        }
      }
    }
  }

  private static void initZooKeeper(Opts opts, String uuid, String instanceNamePath,
      String rootTabletDirName, String rootTabletFileUri)
      throws KeeperException, InterruptedException {
    // setup basic data in zookeeper
    zoo.putPersistentData(Constants.ZROOT, new byte[0], NodeExistsPolicy.SKIP, Ids.OPEN_ACL_UNSAFE);
    zoo.putPersistentData(Constants.ZROOT + Constants.ZINSTANCES, new byte[0],
        NodeExistsPolicy.SKIP, Ids.OPEN_ACL_UNSAFE);

    // setup instance name
    if (opts.clearInstanceName) {
      zoo.recursiveDelete(instanceNamePath, NodeMissingPolicy.SKIP);
    }
    zoo.putPersistentData(instanceNamePath, uuid.getBytes(UTF_8), NodeExistsPolicy.FAIL);

    final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    final byte[] ZERO_CHAR_ARRAY = {'0'};

    // setup the instance
    String zkInstanceRoot = Constants.ZROOT + "/" + uuid;
    zoo.putPersistentData(zkInstanceRoot, EMPTY_BYTE_ARRAY, NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZTABLES, Constants.ZTABLES_INITIAL_ID,
        NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZNAMESPACES, new byte[0],
        NodeExistsPolicy.FAIL);
    TableManager.prepareNewNamespaceState(zoo, uuid, Namespace.DEFAULT.id(),
        Namespace.DEFAULT.name(), NodeExistsPolicy.FAIL);
    TableManager.prepareNewNamespaceState(zoo, uuid, Namespace.ACCUMULO.id(),
        Namespace.ACCUMULO.name(), NodeExistsPolicy.FAIL);
    TableManager.prepareNewTableState(zoo, uuid, RootTable.ID, Namespace.ACCUMULO.id(),
        RootTable.NAME, TableState.ONLINE, NodeExistsPolicy.FAIL);
    TableManager.prepareNewTableState(zoo, uuid, MetadataTable.ID, Namespace.ACCUMULO.id(),
        MetadataTable.NAME, TableState.ONLINE, NodeExistsPolicy.FAIL);
    TableManager.prepareNewTableState(zoo, uuid, ReplicationTable.ID, Namespace.ACCUMULO.id(),
        ReplicationTable.NAME, TableState.OFFLINE, NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZTSERVERS, EMPTY_BYTE_ARRAY,
        NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZPROBLEMS, EMPTY_BYTE_ARRAY,
        NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + RootTable.ZROOT_TABLET,
        RootTabletMetadata.getInitialJson(rootTabletDirName, rootTabletFileUri),
        NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + RootTable.ZROOT_TABLET_GC_CANDIDATES,
        new RootGcCandidates().toJson().getBytes(UTF_8), NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZMANAGERS, EMPTY_BYTE_ARRAY,
        NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZMANAGER_LOCK, EMPTY_BYTE_ARRAY,
        NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZMANAGER_GOAL_STATE,
        ManagerGoalState.NORMAL.toString().getBytes(UTF_8), NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZGC, EMPTY_BYTE_ARRAY, NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZGC_LOCK, EMPTY_BYTE_ARRAY,
        NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZCONFIG, EMPTY_BYTE_ARRAY,
        NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZTABLE_LOCKS, EMPTY_BYTE_ARRAY,
        NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZHDFS_RESERVATIONS, EMPTY_BYTE_ARRAY,
        NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZNEXT_FILE, ZERO_CHAR_ARRAY,
        NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZRECOVERY, ZERO_CHAR_ARRAY,
        NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZMONITOR, EMPTY_BYTE_ARRAY,
        NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZMONITOR_LOCK, EMPTY_BYTE_ARRAY,
        NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + ReplicationConstants.ZOO_BASE, EMPTY_BYTE_ARRAY,
        NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + ReplicationConstants.ZOO_TSERVERS, EMPTY_BYTE_ARRAY,
        NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + WalStateManager.ZWALS, EMPTY_BYTE_ARRAY,
        NodeExistsPolicy.FAIL);
  }

  private String getInstanceNamePrefix() {
    return Constants.ZROOT + Constants.ZINSTANCES + "/";
  }

  private String getInstanceNamePath(Opts opts)
      throws IOException, KeeperException, InterruptedException {
    // setup the instance name
    String instanceName, instanceNamePath = null;
    boolean exists = true;
    do {
      if (opts.cliInstanceName == null) {
        instanceName = getLineReader().readLine("Instance name : ");
      } else {
        instanceName = opts.cliInstanceName;
      }
      if (instanceName == null) {
        System.exit(0);
      }
      instanceName = instanceName.trim();
      if (instanceName.isEmpty()) {
        continue;
      }
      instanceNamePath = getInstanceNamePrefix() + instanceName;
      if (opts.clearInstanceName) {
        exists = false;
      } else {
        // ACCUMULO-4401 setting exists=false is just as important as setting it to true
        exists = zoo.exists(instanceNamePath);
        if (exists) {
          String decision = getLineReader().readLine("Instance name \"" + instanceName
              + "\" exists. Delete existing entry from zookeeper? [Y/N] : ");
          if (decision == null) {
            System.exit(0);
          }
          if (decision.length() == 1 && decision.toLowerCase(Locale.ENGLISH).charAt(0) == 'y') {
            opts.clearInstanceName = true;
            exists = false;
          }
        }
      }
    } while (exists);
    return instanceNamePath;
  }

  private String getRootUserName(SiteConfiguration siteConfig, Opts opts) throws IOException {
    final String keytab = siteConfig.get(Property.GENERAL_KERBEROS_KEYTAB);
    if (keytab.equals(Property.GENERAL_KERBEROS_KEYTAB.getDefaultValue())
        || !siteConfig.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
      return DEFAULT_ROOT_USER;
    }

    LineReader c = getLineReader();
    c.getTerminal().writer().println("Running against secured HDFS");

    if (opts.rootUser != null) {
      return opts.rootUser;
    }

    do {
      String user = c.readLine("Principal (user) to grant administrative privileges to : ");
      if (user == null) {
        // should not happen
        System.exit(1);
      }
      if (!user.isEmpty()) {
        return user;
      }
    } while (true);
  }

  private byte[] getRootPassword(SiteConfiguration siteConfig, Opts opts, String rootUser)
      throws IOException {
    if (opts.cliPassword != null) {
      return opts.cliPassword.getBytes(UTF_8);
    }
    String rootpass;
    String confirmpass;
    do {
      rootpass = getLineReader().readLine(
          "Enter initial password for " + rootUser + getInitialPasswordWarning(siteConfig), '*');
      if (rootpass == null) {
        System.exit(0);
      }
      confirmpass =
          getLineReader().readLine("Confirm initial password for " + rootUser + ": ", '*');
      if (confirmpass == null) {
        System.exit(0);
      }
      if (!rootpass.equals(confirmpass)) {
        log.error("Passwords do not match");
      }
    } while (!rootpass.equals(confirmpass));
    return rootpass.getBytes(UTF_8);
  }

  /**
   * Create warning message related to initial password, if appropriate.
   *
   * ACCUMULO-2907 Remove unnecessary security warning from console message unless its actually
   * appropriate. The warning message should only be displayed when the value of
   * <code>instance.security.authenticator</code> differs between the SiteConfiguration and the
   * DefaultConfiguration values.
   *
   * @return String containing warning portion of console message.
   */
  private String getInitialPasswordWarning(SiteConfiguration siteConfig) {
    String optionalWarning;
    Property authenticatorProperty = Property.INSTANCE_SECURITY_AUTHENTICATOR;
    if (siteConfig.get(authenticatorProperty).equals(authenticatorProperty.getDefaultValue())) {
      optionalWarning = ": ";
    } else {
      optionalWarning = " (this may not be applicable for your security setup): ";
    }
    return optionalWarning;
  }

  private static void initSecurity(ServerContext context, Opts opts, String rootUser)
      throws AccumuloSecurityException {
    AuditedSecurityOperation.getInstance(context).initializeSecurity(context.rpcCreds(), rootUser,
        opts.rootpass);
  }

  public static void initSystemTablesConfig(ZooReaderWriter zoo, String zooKeeperRoot,
      Configuration hadoopConf) throws IOException {
    try {
      int max = hadoopConf.getInt("dfs.replication.max", 512);
      // Hadoop 0.23 switched the min value configuration name
      int min = Math.max(hadoopConf.getInt("dfs.replication.min", 1),
          hadoopConf.getInt("dfs.namenode.replication.min", 1));
      if (max < 5) {
        setMetadataReplication(max, "max");
      }
      if (min > 5) {
        setMetadataReplication(min, "min");
      }

      for (Entry<String,String> entry : initialRootConf.entrySet()) {
        if (!TablePropUtil.setTableProperty(zoo, zooKeeperRoot, RootTable.ID, entry.getKey(),
            entry.getValue())) {
          throw new IOException("Cannot create per-table property " + entry.getKey());
        }
      }

      for (Entry<String,String> entry : initialRootMetaConf.entrySet()) {
        if (!TablePropUtil.setTableProperty(zoo, zooKeeperRoot, RootTable.ID, entry.getKey(),
            entry.getValue())) {
          throw new IOException("Cannot create per-table property " + entry.getKey());
        }
        if (!TablePropUtil.setTableProperty(zoo, zooKeeperRoot, MetadataTable.ID, entry.getKey(),
            entry.getValue())) {
          throw new IOException("Cannot create per-table property " + entry.getKey());
        }
      }

      for (Entry<String,String> entry : initialMetaConf.entrySet()) {
        if (!TablePropUtil.setTableProperty(zoo, zooKeeperRoot, MetadataTable.ID, entry.getKey(),
            entry.getValue())) {
          throw new IOException("Cannot create per-table property " + entry.getKey());
        }
      }

      // add configuration to the replication table
      for (Entry<String,String> entry : initialReplicationTableConf.entrySet()) {
        if (!TablePropUtil.setTableProperty(zoo, zooKeeperRoot, ReplicationTable.ID, entry.getKey(),
            entry.getValue())) {
          throw new IOException("Cannot create per-table property " + entry.getKey());
        }
      }
    } catch (Exception e) {
      log.error("FATAL: Error talking to ZooKeeper", e);
      throw new IOException(e);
    }
  }

  private static void setMetadataReplication(int replication, String reason) throws IOException {
    String rep = getLineReader()
        .readLine("Your HDFS replication " + reason + " is not compatible with our default "
            + MetadataTable.NAME + " replication of 5. What do you want to set your "
            + MetadataTable.NAME + " replication to? (" + replication + ") ");
    if (rep == null || rep.isEmpty()) {
      rep = Integer.toString(replication);
    } else {
      // Lets make sure it's a number
      Integer.parseInt(rep);
    }
    initialRootMetaConf.put(Property.TABLE_FILE_REPLICATION.getKey(), rep);
  }

  public static boolean isInitialized(VolumeManager fs, SiteConfiguration siteConfig)
      throws IOException {
    for (String baseDir : VolumeConfiguration.getVolumeUris(siteConfig)) {
      if (fs.exists(new Path(baseDir, ServerConstants.INSTANCE_ID_DIR))
          || fs.exists(new Path(baseDir, ServerConstants.VERSION_DIR))) {
        return true;
      }
    }

    return false;
  }

  private static void addVolumes(VolumeManager fs, SiteConfiguration siteConfig,
      Configuration hadoopConf) throws IOException {

    Set<String> volumeURIs = VolumeConfiguration.getVolumeUris(siteConfig);

    Set<String> initializedDirs =
        ServerConstants.checkBaseUris(siteConfig, hadoopConf, volumeURIs, true);

    HashSet<String> uinitializedDirs = new HashSet<>();
    uinitializedDirs.addAll(volumeURIs);
    uinitializedDirs.removeAll(initializedDirs);

    Path aBasePath = new Path(initializedDirs.iterator().next());
    Path iidPath = new Path(aBasePath, ServerConstants.INSTANCE_ID_DIR);
    Path versionPath = new Path(aBasePath, ServerConstants.VERSION_DIR);

    UUID uuid = UUID.fromString(VolumeManager.getInstanceIDFromHdfs(iidPath, hadoopConf));
    for (Pair<Path,Path> replacementVolume : ServerConstants.getVolumeReplacements(siteConfig,
        hadoopConf)) {
      if (aBasePath.equals(replacementVolume.getFirst())) {
        log.error(
            "{} is set to be replaced in {} and should not appear in {}."
                + " It is highly recommended that this property be removed as data"
                + " could still be written to this volume.",
            aBasePath, Property.INSTANCE_VOLUMES_REPLACEMENTS, Property.INSTANCE_VOLUMES);
      }
    }

    if (ServerUtil.getAccumuloPersistentVersion(versionPath.getFileSystem(hadoopConf), versionPath)
        != ServerConstants.DATA_VERSION) {
      throw new IOException("Accumulo " + Constants.VERSION + " cannot initialize data version "
          + ServerUtil.getAccumuloPersistentVersion(fs));
    }

    initDirs(fs, uuid, uinitializedDirs, true);
  }

  static class Opts extends Help {
    @Parameter(names = "--add-volumes",
        description = "Initialize any uninitialized volumes listed in instance.volumes")
    boolean addVolumes = false;
    @Parameter(names = "--reset-security",
        description = "just update the security information, will prompt")
    boolean resetSecurity = false;
    @Parameter(names = {"-f", "--force"},
        description = "force reset of the security information without prompting")
    boolean forceResetSecurity = false;
    @Parameter(names = "--clear-instance-name",
        description = "delete any existing instance name without prompting")
    boolean clearInstanceName = false;
    @Parameter(names = "--upload-accumulo-props",
        description = "Uploads properties in accumulo.properties to Zookeeper")
    boolean uploadAccumuloProps = false;
    @Parameter(names = "--instance-name",
        description = "the instance name, if not provided, will prompt")
    String cliInstanceName = null;
    @Parameter(names = "--password", description = "set the password on the command line")
    String cliPassword = null;
    @Parameter(names = {"-u", "--user"},
        description = "the name of the user to grant system permissions to")
    String rootUser = null;

    byte[] rootpass = null;
  }

  @Override
  public String keyword() {
    return "init";
  }

  @Override
  public UsageGroup usageGroup() {
    return UsageGroup.CORE;
  }

  @Override
  public String description() {
    return "Initializes Accumulo";
  }

  @Override
  public void execute(final String[] args) {
    Opts opts = new Opts();
    opts.parseArgs("accumulo init", args);
    var siteConfig = SiteConfiguration.auto();

    try {
      setZooReaderWriter(new ZooReaderWriter(siteConfig));
      SecurityUtil.serverLogin(siteConfig);
      Configuration hadoopConfig = new Configuration();

      try (var fs = VolumeManagerImpl.get(siteConfig, hadoopConfig)) {

        if (opts.resetSecurity) {
          log.info("Resetting security on accumulo.");
          try (ServerContext context = new ServerContext(siteConfig)) {
            if (isInitialized(fs, siteConfig)) {
              if (!opts.forceResetSecurity) {
                LineReader c = getLineReader();
                String userEnteredName = c.readLine("WARNING: This will remove all"
                    + " users from Accumulo! If you wish to proceed enter the instance"
                    + " name: ");
                if (userEnteredName != null && !context.getInstanceName().equals(userEnteredName)) {
                  log.error(
                      "Aborted reset security: Instance name did not match current instance.");
                  return;
                }
              }

              final String rootUser = getRootUserName(siteConfig, opts);
              opts.rootpass = getRootPassword(siteConfig, opts, rootUser);
              initSecurity(context, opts, rootUser);
            } else {
              log.error("FATAL: Attempted to reset security on accumulo before it was initialized");
            }
          }
        }

        if (opts.addVolumes) {
          addVolumes(fs, siteConfig, hadoopConfig);
        }

        if (!opts.resetSecurity && !opts.addVolumes) {
          if (!doInit(siteConfig, opts, hadoopConfig, fs)) {
            System.exit(-1);
          }
        }
      }
    } catch (Exception e) {
      log.error("Fatal exception", e);
      throw new RuntimeException(e);
    } finally {
      SingletonManager.setMode(Mode.CLOSED);
    }
  }

  public static void main(String[] args) {
    new Initialize().execute(args);
  }
}
