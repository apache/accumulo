/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.IteratorSetting.Column;
import org.apache.accumulo.core.client.impl.Namespace;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.master.thrift.MasterGoalState;
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
import org.apache.accumulo.core.replication.ReplicationConstants;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.ReplicationSchema.WorkSection;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.volume.VolumeConfiguration;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.Accumulo;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.constraints.MetadataConstraints;
import org.apache.accumulo.server.fs.VolumeChooserEnvironment;
import org.apache.accumulo.server.fs.VolumeChooserEnvironment.ChooserScope;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.iterators.MetadataBulkLoadFilter;
import org.apache.accumulo.server.log.WalStateManager;
import org.apache.accumulo.server.replication.ReplicationUtil;
import org.apache.accumulo.server.replication.StatusCombiner;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.accumulo.server.tablets.TabletTime;
import org.apache.accumulo.server.util.ReplicationTableUtil;
import org.apache.accumulo.server.util.SystemPropUtil;
import org.apache.accumulo.server.util.TablePropUtil;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;
import com.google.common.base.Joiner;

import jline.console.ConsoleReader;

/**
 * This class is used to setup the directory structure and the root tablet to get an instance started
 */
@AutoService(KeywordExecutable.class)
public class Initialize implements KeywordExecutable {
  private static final Logger log = LoggerFactory.getLogger(Initialize.class);
  private static final String DEFAULT_ROOT_USER = "root";
  private static final String TABLE_TABLETS_TABLET_DIR = "/table_info";

  private static ConsoleReader reader = null;
  private static IZooReaderWriter zoo = null;

  private static ConsoleReader getConsoleReader() throws IOException {
    if (reader == null)
      reader = new ConsoleReader();
    return reader;
  }

  /**
   * Sets this class's ZooKeeper reader/writer.
   *
   * @param izoo
   *          reader/writer
   */
  static void setZooReaderWriter(IZooReaderWriter izoo) {
    zoo = izoo;
  }

  /**
   * Gets this class's ZooKeeper reader/writer.
   *
   * @return reader/writer
   */
  static IZooReaderWriter getZooReaderWriter() {
    return zoo;
  }

  private static HashMap<String,String> initialMetadataConf = new HashMap<>();
  private static HashMap<String,String> initialMetadataCombinerConf = new HashMap<>();
  private static HashMap<String,String> initialReplicationTableConf = new HashMap<>();

  static {
    initialMetadataConf.put(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), "32K");
    initialMetadataConf.put(Property.TABLE_FILE_REPLICATION.getKey(), "5");
    initialMetadataConf.put(Property.TABLE_DURABILITY.getKey(), "sync");
    initialMetadataConf.put(Property.TABLE_MAJC_RATIO.getKey(), "1");
    initialMetadataConf.put(Property.TABLE_SPLIT_THRESHOLD.getKey(), "64M");
    initialMetadataConf.put(Property.TABLE_CONSTRAINT_PREFIX.getKey() + "1", MetadataConstraints.class.getName());
    initialMetadataConf.put(Property.TABLE_ITERATOR_PREFIX.getKey() + "scan.vers", "10," + VersioningIterator.class.getName());
    initialMetadataConf.put(Property.TABLE_ITERATOR_PREFIX.getKey() + "scan.vers.opt.maxVersions", "1");
    initialMetadataConf.put(Property.TABLE_ITERATOR_PREFIX.getKey() + "minc.vers", "10," + VersioningIterator.class.getName());
    initialMetadataConf.put(Property.TABLE_ITERATOR_PREFIX.getKey() + "minc.vers.opt.maxVersions", "1");
    initialMetadataConf.put(Property.TABLE_ITERATOR_PREFIX.getKey() + "majc.vers", "10," + VersioningIterator.class.getName());
    initialMetadataConf.put(Property.TABLE_ITERATOR_PREFIX.getKey() + "majc.vers.opt.maxVersions", "1");
    initialMetadataConf.put(Property.TABLE_ITERATOR_PREFIX.getKey() + "majc.bulkLoadFilter", "20," + MetadataBulkLoadFilter.class.getName());
    initialMetadataConf.put(Property.TABLE_FAILURES_IGNORE.getKey(), "false");
    initialMetadataConf.put(Property.TABLE_LOCALITY_GROUP_PREFIX.getKey() + "tablet",
        String.format("%s,%s", TabletColumnFamily.NAME, CurrentLocationColumnFamily.NAME));
    initialMetadataConf.put(Property.TABLE_LOCALITY_GROUP_PREFIX.getKey() + "server",
        String.format("%s,%s,%s,%s", DataFileColumnFamily.NAME, LogColumnFamily.NAME, ServerColumnFamily.NAME, FutureLocationColumnFamily.NAME));
    initialMetadataConf.put(Property.TABLE_LOCALITY_GROUPS.getKey(), "tablet,server");
    initialMetadataConf.put(Property.TABLE_DEFAULT_SCANTIME_VISIBILITY.getKey(), "");
    initialMetadataConf.put(Property.TABLE_INDEXCACHE_ENABLED.getKey(), "true");
    initialMetadataConf.put(Property.TABLE_BLOCKCACHE_ENABLED.getKey(), "true");

    // ACCUMULO-3077 Set the combiner on accumulo.metadata during init to reduce the likelihood of a race
    // condition where a tserver compacts away Status updates because it didn't see the Combiner configured
    IteratorSetting setting = new IteratorSetting(9, ReplicationTableUtil.COMBINER_NAME, StatusCombiner.class);
    Combiner.setColumns(setting, Collections.singletonList(new Column(ReplicationSection.COLF)));
    for (IteratorScope scope : IteratorScope.values()) {
      String root = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX, scope.name().toLowerCase(), setting.getName());
      for (Entry<String,String> prop : setting.getOptions().entrySet()) {
        initialMetadataCombinerConf.put(root + ".opt." + prop.getKey(), prop.getValue());
      }
      initialMetadataCombinerConf.put(root, setting.getPriority() + "," + setting.getIteratorClass());
    }

    // add combiners to replication table
    setting = new IteratorSetting(30, ReplicationTable.COMBINER_NAME, StatusCombiner.class);
    setting.setPriority(30);
    Combiner.setColumns(setting, Arrays.asList(new Column(StatusSection.NAME), new Column(WorkSection.NAME)));
    for (IteratorScope scope : EnumSet.allOf(IteratorScope.class)) {
      String root = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX, scope.name().toLowerCase(), setting.getName());
      for (Entry<String,String> prop : setting.getOptions().entrySet()) {
        initialReplicationTableConf.put(root + ".opt." + prop.getKey(), prop.getValue());
      }
      initialReplicationTableConf.put(root, setting.getPriority() + "," + setting.getIteratorClass());
    }
    // add locality groups to replication table
    for (Entry<String,Set<Text>> g : ReplicationTable.LOCALITY_GROUPS.entrySet()) {
      initialReplicationTableConf.put(Property.TABLE_LOCALITY_GROUP_PREFIX + g.getKey(), LocalityGroupUtil.encodeColumnFamilies(g.getValue()));
    }
    initialReplicationTableConf.put(Property.TABLE_LOCALITY_GROUPS.getKey(), Joiner.on(",").join(ReplicationTable.LOCALITY_GROUPS.keySet()));
    // add formatter to replication table
    initialReplicationTableConf.put(Property.TABLE_FORMATTER_CLASS.getKey(), ReplicationUtil.STATUS_FORMATTER_CLASS_NAME);
  }

  static boolean checkInit(Configuration conf, VolumeManager fs, SiteConfiguration sconf) throws IOException {
    @SuppressWarnings("deprecation")
    String fsUri = sconf.get(Property.INSTANCE_DFS_URI);
    if (fsUri.equals(""))
      fsUri = FileSystem.getDefaultUri(conf).toString();
    log.info("Hadoop Filesystem is {}", fsUri);
    log.info("Accumulo data dirs are {}", Arrays.asList(VolumeConfiguration.getVolumeUris(SiteConfiguration.getInstance())));
    log.info("Zookeeper server is {}", sconf.get(Property.INSTANCE_ZK_HOST));
    log.info("Checking if Zookeeper is available. If this hangs, then you need to make sure zookeeper is running");
    if (!zookeeperAvailable()) {
      // ACCUMULO-3651 Changed level to error and added FATAL to message for slf4j compatibility
      log.error("FATAL Zookeeper needs to be up and running in order to init. Exiting ...");
      return false;
    }
    if (sconf.get(Property.INSTANCE_SECRET).equals(Property.INSTANCE_SECRET.getDefaultValue())) {
      ConsoleReader c = getConsoleReader();
      c.beep();
      c.println();
      c.println();
      c.println("Warning!!! Your instance secret is still set to the default, this is not secure. We highly recommend you change it.");
      c.println();
      c.println();
      c.println("You can change the instance secret in accumulo by using:");
      c.println("   bin/accumulo " + org.apache.accumulo.server.util.ChangeSecret.class.getName());
      c.println("You will also need to edit your secret in your configuration file by adding the property instance.secret to your accumulo-site.xml. "
          + "Without this accumulo will not operate correctly");
    }
    try {
      if (isInitialized(fs)) {
        printInitializeFailureMessages(sconf);
        return false;
      }
    } catch (IOException e) {
      throw new IOException("Failed to check if filesystem already initialized", e);
    }

    return true;
  }

  static void printInitializeFailureMessages(SiteConfiguration sconf) {
    @SuppressWarnings("deprecation")
    Property INSTANCE_DFS_DIR = Property.INSTANCE_DFS_DIR;
    @SuppressWarnings("deprecation")
    Property INSTANCE_DFS_URI = Property.INSTANCE_DFS_URI;
    String instanceDfsDir = sconf.get(INSTANCE_DFS_DIR);
    // ACCUMULO-3651 Changed level to error and added FATAL to message for slf4j compatibility
    log.error("FATAL It appears the directories {}", Arrays.asList(VolumeConfiguration.getVolumeUris(SiteConfiguration.getInstance()))
        + " were previously initialized.");
    String instanceVolumes = sconf.get(Property.INSTANCE_VOLUMES);
    String instanceDfsUri = sconf.get(INSTANCE_DFS_URI);

    // ACCUMULO-3651 Changed level to error and added FATAL to message for slf4j compatibility

    if (!instanceVolumes.isEmpty()) {
      log.error("FATAL: Change the property {} to use different filesystems,", Property.INSTANCE_VOLUMES);
    } else if (!instanceDfsDir.isEmpty()) {
      log.error("FATAL: Change the property {} to use a different filesystem,", INSTANCE_DFS_URI);
    } else {
      log.error("FATAL: You are using the default URI for the filesystem. Set the property {} to use a different filesystem,", Property.INSTANCE_VOLUMES);
    }
    log.error("FATAL: or change the property {} to use a different directory.", INSTANCE_DFS_DIR);
    log.error("FATAL: The current value of {} is |{}|", INSTANCE_DFS_URI, instanceDfsUri);
    log.error("FATAL: The current value of {} is |{}|", INSTANCE_DFS_DIR, instanceDfsDir);
    log.error("FATAL: The current value of {} is |{}|", Property.INSTANCE_VOLUMES, instanceVolumes);
  }

  public boolean doInit(Opts opts, Configuration conf, VolumeManager fs) throws IOException {
    if (!checkInit(conf, fs, SiteConfiguration.getInstance())) {
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
      rootUser = getRootUserName(opts);
    } catch (Exception e) {
      log.error("FATAL: Failed to obtain user for administrative privileges");
      return false;
    }

    // Don't prompt for a password when we're running SASL(Kerberos)
    final AccumuloConfiguration siteConf = SiteConfiguration.getInstance();
    if (siteConf.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
      opts.rootpass = UUID.randomUUID().toString().getBytes(UTF_8);
    } else {
      opts.rootpass = getRootPassword(opts, rootUser);
    }

    return initialize(opts, instanceNamePath, fs, rootUser);
  }

  private boolean initialize(Opts opts, String instanceNamePath, VolumeManager fs, String rootUser) {

    UUID uuid = UUID.randomUUID();
    // the actual disk locations of the root table and tablets
    String[] configuredVolumes = VolumeConfiguration.getVolumeUris(SiteConfiguration.getInstance());
    VolumeChooserEnvironment chooserEnv = new VolumeChooserEnvironment(ChooserScope.INIT);
    final String rootTabletDir = new Path(fs.choose(chooserEnv, configuredVolumes) + Path.SEPARATOR + ServerConstants.TABLE_DIR + Path.SEPARATOR + RootTable.ID
        + RootTable.ROOT_TABLET_LOCATION).toString();

    try {
      initZooKeeper(opts, uuid.toString(), instanceNamePath, rootTabletDir);
    } catch (Exception e) {
      log.error("FATAL: Failed to initialize zookeeper", e);
      return false;
    }

    try {
      initFileSystem(opts, fs, uuid, rootTabletDir);
    } catch (Exception e) {
      log.error("FATAL Failed to initialize filesystem", e);

      if (SiteConfiguration.getInstance().get(Property.INSTANCE_VOLUMES).trim().equals("")) {
        Configuration fsConf = CachedConfiguration.getInstance();

        final String defaultFsUri = "file:///";
        String fsDefaultName = fsConf.get("fs.default.name", defaultFsUri), fsDefaultFS = fsConf.get("fs.defaultFS", defaultFsUri);

        // Try to determine when we couldn't find an appropriate core-site.xml on the classpath
        if (defaultFsUri.equals(fsDefaultName) && defaultFsUri.equals(fsDefaultFS)) {
          log.error("FATAL: Default filesystem value ('fs.defaultFS' or 'fs.default.name') of '{}' was found in the Hadoop configuration", defaultFsUri);
          log.error("FATAL: Please ensure that the Hadoop core-site.xml is on the classpath using 'general.classpaths' in accumulo-site.xml");
        }
      }

      return false;
    }

    final Instance instance = HdfsZooInstance.getInstance();
    final ServerConfigurationFactory confFactory = new ServerConfigurationFactory(instance);

    // When we're using Kerberos authentication, we need valid credentials to perform initialization. If the user provided some, use them.
    // If they did not, fall back to the credentials present in accumulo-site.xml that the servers will use themselves.
    try {
      final SiteConfiguration siteConf = confFactory.getSiteConfiguration();
      if (siteConf.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
        final UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        // We don't have any valid creds to talk to HDFS
        if (!ugi.hasKerberosCredentials()) {
          final String accumuloKeytab = siteConf.get(Property.GENERAL_KERBEROS_KEYTAB), accumuloPrincipal = siteConf.get(Property.GENERAL_KERBEROS_PRINCIPAL);

          // Fail if the site configuration doesn't contain appropriate credentials to login as servers
          if (StringUtils.isBlank(accumuloKeytab) || StringUtils.isBlank(accumuloPrincipal)) {
            log.error("FATAL: No Kerberos credentials provided, and Accumulo is not properly configured for server login");
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
      AccumuloServerContext context = new AccumuloServerContext(instance, confFactory);
      initSecurity(context, opts, uuid.toString(), rootUser);
    } catch (Exception e) {
      log.error("FATAL: Failed to initialize security", e);
      return false;
    }

    if (opts.uploadAccumuloSite) {
      try {
        log.info("Uploading properties in accumulo-site.xml to Zookeeper. Properties that cannot be set in Zookeeper will be skipped:");
        Map<String,String> entries = new TreeMap<>();
        SiteConfiguration.getInstance().getProperties(entries, x -> true, false);
        for (Map.Entry<String,String> entry : entries.entrySet()) {
          String key = entry.getKey();
          String value = entry.getValue();
          if (Property.isValidZooPropertyKey(key)) {
            SystemPropUtil.setSystemProperty(key, value);
            log.info("Uploaded - {} = {}", key, Property.isSensitive(key) ? "<hidden>" : value);
          } else {
            log.info("Skipped - {} = {}", key, Property.isSensitive(key) ? "<hidden>" : value);
          }
        }
      } catch (Exception e) {
        log.error("FATAL: Failed to upload accumulo-site.xml to Zookeeper", e);
        return false;
      }
    }

    return true;
  }

  private static boolean zookeeperAvailable() {
    try {
      return zoo.exists("/");
    } catch (KeeperException e) {
      return false;
    } catch (InterruptedException e) {
      return false;
    }
  }

  private static void initDirs(VolumeManager fs, UUID uuid, String[] baseDirs, boolean print) throws IOException {
    for (String baseDir : baseDirs) {
      fs.mkdirs(new Path(new Path(baseDir, ServerConstants.VERSION_DIR), "" + ServerConstants.DATA_VERSION), new FsPermission("700"));

      // create an instance id
      Path iidLocation = new Path(baseDir, ServerConstants.INSTANCE_ID_DIR);
      fs.mkdirs(iidLocation);
      fs.createNewFile(new Path(iidLocation, uuid.toString()));
      if (print)
        log.info("Initialized volume {}", baseDir);
    }
  }

  private void initFileSystem(Opts opts, VolumeManager fs, UUID uuid, String rootTabletDir) throws IOException {
    initDirs(fs, uuid, VolumeConfiguration.getVolumeUris(SiteConfiguration.getInstance()), false);

    // initialize initial system tables config in zookeeper
    initSystemTablesConfig();

    VolumeChooserEnvironment chooserEnv = new VolumeChooserEnvironment(ChooserScope.INIT);
    String tableMetadataTabletDir = fs.choose(chooserEnv, ServerConstants.getBaseUris()) + Constants.HDFS_TABLES_DIR + Path.SEPARATOR + MetadataTable.ID
        + TABLE_TABLETS_TABLET_DIR;
    String replicationTableDefaultTabletDir = fs.choose(chooserEnv, ServerConstants.getBaseUris()) + Constants.HDFS_TABLES_DIR + Path.SEPARATOR
        + ReplicationTable.ID + Constants.DEFAULT_TABLET_LOCATION;
    String defaultMetadataTabletDir = fs.choose(chooserEnv, ServerConstants.getBaseUris()) + Constants.HDFS_TABLES_DIR + Path.SEPARATOR + MetadataTable.ID
        + Constants.DEFAULT_TABLET_LOCATION;

    // create table and default tablets directories
    createDirectories(fs, rootTabletDir, tableMetadataTabletDir, defaultMetadataTabletDir, replicationTableDefaultTabletDir);

    String ext = FileOperations.getNewFileExtension(DefaultConfiguration.getInstance());

    // populate the metadata tables tablet with info about the replication table's one initial tablet
    String metadataFileName = tableMetadataTabletDir + Path.SEPARATOR + "0_1." + ext;
    Tablet replicationTablet = new Tablet(ReplicationTable.ID, replicationTableDefaultTabletDir, null, null);
    createMetadataFile(fs, metadataFileName, replicationTablet);

    // populate the root tablet with info about the metadata table's two initial tablets
    String rootTabletFileName = rootTabletDir + Path.SEPARATOR + "00000_00000." + ext;
    Text splitPoint = TabletsSection.getRange().getEndKey().getRow();
    Tablet tablesTablet = new Tablet(MetadataTable.ID, tableMetadataTabletDir, null, splitPoint, metadataFileName);
    Tablet defaultTablet = new Tablet(MetadataTable.ID, defaultMetadataTabletDir, splitPoint, null);
    createMetadataFile(fs, rootTabletFileName, tablesTablet, defaultTablet);
  }

  private static class Tablet {
    Table.ID tableId;
    String dir;
    Text prevEndRow, endRow;
    String[] files;

    Tablet(Table.ID tableId, String dir, Text prevEndRow, Text endRow, String... files) {
      this.tableId = tableId;
      this.dir = dir;
      this.prevEndRow = prevEndRow;
      this.endRow = endRow;
      this.files = files;
    }
  }

  private static void createMetadataFile(VolumeManager volmanager, String fileName, Tablet... tablets) throws IOException {
    // sort file contents in memory, then play back to the file
    TreeMap<Key,Value> sorted = new TreeMap<>();
    for (Tablet tablet : tablets) {
      createEntriesForTablet(sorted, tablet);
    }
    FileSystem fs = volmanager.getVolumeByPath(new Path(fileName)).getFileSystem();
    FileSKVWriter tabletWriter = FileOperations.getInstance().newWriterBuilder().forFile(fileName, fs, fs.getConf())
        .withTableConfiguration(DefaultConfiguration.getInstance()).build();
    tabletWriter.startDefaultLocalityGroup();

    for (Entry<Key,Value> entry : sorted.entrySet()) {
      tabletWriter.append(entry.getKey(), entry.getValue());
    }

    tabletWriter.close();
  }

  private static void createEntriesForTablet(TreeMap<Key,Value> map, Tablet tablet) {
    Value EMPTY_SIZE = new DataFileValue(0, 0).encodeAsValue();
    Text extent = new Text(KeyExtent.getMetadataEntry(tablet.tableId, tablet.endRow));
    addEntry(map, extent, DIRECTORY_COLUMN, new Value(tablet.dir.getBytes(UTF_8)));
    addEntry(map, extent, TIME_COLUMN, new Value((TabletTime.LOGICAL_TIME_ID + "0").getBytes(UTF_8)));
    addEntry(map, extent, PREV_ROW_COLUMN, KeyExtent.encodePrevEndRow(tablet.prevEndRow));
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

  private static void initZooKeeper(Opts opts, String uuid, String instanceNamePath, String rootTabletDir) throws KeeperException, InterruptedException {
    // setup basic data in zookeeper
    zoo.putPersistentData(Constants.ZROOT, new byte[0], -1, NodeExistsPolicy.SKIP, Ids.OPEN_ACL_UNSAFE);
    zoo.putPersistentData(Constants.ZROOT + Constants.ZINSTANCES, new byte[0], -1, NodeExistsPolicy.SKIP, Ids.OPEN_ACL_UNSAFE);

    // setup instance name
    if (opts.clearInstanceName)
      zoo.recursiveDelete(instanceNamePath, NodeMissingPolicy.SKIP);
    zoo.putPersistentData(instanceNamePath, uuid.getBytes(UTF_8), NodeExistsPolicy.FAIL);

    final byte[] EMPTY_BYTE_ARRAY = new byte[0], ZERO_CHAR_ARRAY = new byte[] {'0'};

    // setup the instance
    String zkInstanceRoot = Constants.ZROOT + "/" + uuid;
    zoo.putPersistentData(zkInstanceRoot, EMPTY_BYTE_ARRAY, NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZTABLES, Constants.ZTABLES_INITIAL_ID, NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZNAMESPACES, new byte[0], NodeExistsPolicy.FAIL);
    TableManager.prepareNewNamespaceState(uuid, Namespace.ID.DEFAULT, Namespace.DEFAULT, NodeExistsPolicy.FAIL);
    TableManager.prepareNewNamespaceState(uuid, Namespace.ID.ACCUMULO, Namespace.ACCUMULO, NodeExistsPolicy.FAIL);
    TableManager.prepareNewTableState(uuid, RootTable.ID, Namespace.ID.ACCUMULO, RootTable.NAME, TableState.ONLINE, NodeExistsPolicy.FAIL);
    TableManager.prepareNewTableState(uuid, MetadataTable.ID, Namespace.ID.ACCUMULO, MetadataTable.NAME, TableState.ONLINE, NodeExistsPolicy.FAIL);
    TableManager.prepareNewTableState(uuid, ReplicationTable.ID, Namespace.ID.ACCUMULO, ReplicationTable.NAME, TableState.OFFLINE, NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZTSERVERS, EMPTY_BYTE_ARRAY, NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZPROBLEMS, EMPTY_BYTE_ARRAY, NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + RootTable.ZROOT_TABLET, EMPTY_BYTE_ARRAY, NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + RootTable.ZROOT_TABLET_WALOGS, EMPTY_BYTE_ARRAY, NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + RootTable.ZROOT_TABLET_CURRENT_LOGS, EMPTY_BYTE_ARRAY, NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + RootTable.ZROOT_TABLET_PATH, rootTabletDir.getBytes(UTF_8), NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZMASTERS, EMPTY_BYTE_ARRAY, NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZMASTER_LOCK, EMPTY_BYTE_ARRAY, NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZMASTER_GOAL_STATE, MasterGoalState.NORMAL.toString().getBytes(UTF_8), NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZGC, EMPTY_BYTE_ARRAY, NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZGC_LOCK, EMPTY_BYTE_ARRAY, NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZCONFIG, EMPTY_BYTE_ARRAY, NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZTABLE_LOCKS, EMPTY_BYTE_ARRAY, NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZHDFS_RESERVATIONS, EMPTY_BYTE_ARRAY, NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZNEXT_FILE, ZERO_CHAR_ARRAY, NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZRECOVERY, ZERO_CHAR_ARRAY, NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZMONITOR, EMPTY_BYTE_ARRAY, NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZMONITOR_LOCK, EMPTY_BYTE_ARRAY, NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + ReplicationConstants.ZOO_BASE, EMPTY_BYTE_ARRAY, NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + ReplicationConstants.ZOO_TSERVERS, EMPTY_BYTE_ARRAY, NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + WalStateManager.ZWALS, EMPTY_BYTE_ARRAY, NodeExistsPolicy.FAIL);
  }

  private String getInstanceNamePath(Opts opts) throws IOException, KeeperException, InterruptedException {
    // setup the instance name
    String instanceName, instanceNamePath = null;
    boolean exists = true;
    do {
      if (opts.cliInstanceName == null) {
        instanceName = getConsoleReader().readLine("Instance name : ");
      } else {
        instanceName = opts.cliInstanceName;
      }
      if (instanceName == null)
        System.exit(0);
      instanceName = instanceName.trim();
      if (instanceName.length() == 0)
        continue;
      instanceNamePath = Constants.ZROOT + Constants.ZINSTANCES + "/" + instanceName;
      if (opts.clearInstanceName) {
        exists = false;
        break;
      } else {
        // ACCUMULO-4401 setting exists=false is just as important as setting it to true
        exists = zoo.exists(instanceNamePath);
        if (exists) {
          String decision = getConsoleReader().readLine("Instance name \"" + instanceName + "\" exists. Delete existing entry from zookeeper? [Y/N] : ");
          if (decision == null)
            System.exit(0);
          if (decision.length() == 1 && decision.toLowerCase(Locale.ENGLISH).charAt(0) == 'y') {
            opts.clearInstanceName = true;
            exists = false;
          }
        }
      }
    } while (exists);
    return instanceNamePath;
  }

  private String getRootUserName(Opts opts) throws IOException {
    AccumuloConfiguration conf = SiteConfiguration.getInstance();
    final String keytab = conf.get(Property.GENERAL_KERBEROS_KEYTAB);
    if (keytab.equals(Property.GENERAL_KERBEROS_KEYTAB.getDefaultValue()) || !conf.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
      return DEFAULT_ROOT_USER;
    }

    ConsoleReader c = getConsoleReader();
    c.println("Running against secured HDFS");

    if (null != opts.rootUser) {
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

  private byte[] getRootPassword(Opts opts, String rootUser) throws IOException {
    if (opts.cliPassword != null) {
      return opts.cliPassword.getBytes(UTF_8);
    }
    String rootpass;
    String confirmpass;
    do {
      rootpass = getConsoleReader().readLine("Enter initial password for " + rootUser + getInitialPasswordWarning(), '*');
      if (rootpass == null)
        System.exit(0);
      confirmpass = getConsoleReader().readLine("Confirm initial password for " + rootUser + ": ", '*');
      if (confirmpass == null)
        System.exit(0);
      if (!rootpass.equals(confirmpass))
        log.error("Passwords do not match");
    } while (!rootpass.equals(confirmpass));
    return rootpass.getBytes(UTF_8);
  }

  /**
   * Create warning message related to initial password, if appropriate.
   *
   * ACCUMULO-2907 Remove unnecessary security warning from console message unless its actually appropriate. The warning message should only be displayed when
   * the value of <code>instance.security.authenticator</code> differs between the SiteConfiguration and the DefaultConfiguration values.
   *
   * @return String containing warning portion of console message.
   */
  private String getInitialPasswordWarning() {
    String optionalWarning;
    Property authenticatorProperty = Property.INSTANCE_SECURITY_AUTHENTICATOR;
    if (SiteConfiguration.getInstance().get(authenticatorProperty).equals(authenticatorProperty.getDefaultValue()))
      optionalWarning = ": ";
    else
      optionalWarning = " (this may not be applicable for your security setup): ";
    return optionalWarning;
  }

  private static void initSecurity(AccumuloServerContext context, Opts opts, String iid, String rootUser) throws AccumuloSecurityException,
      ThriftSecurityException, IOException {
    AuditedSecurityOperation.getInstance(context, true).initializeSecurity(context.rpcCreds(), rootUser, opts.rootpass);
  }

  public static void initSystemTablesConfig() throws IOException {
    try {
      Configuration conf = CachedConfiguration.getInstance();
      int max = conf.getInt("dfs.replication.max", 512);
      // Hadoop 0.23 switched the min value configuration name
      int min = Math.max(conf.getInt("dfs.replication.min", 1), conf.getInt("dfs.namenode.replication.min", 1));
      if (max < 5)
        setMetadataReplication(max, "max");
      if (min > 5)
        setMetadataReplication(min, "min");
      for (Entry<String,String> entry : initialMetadataConf.entrySet()) {
        if (!TablePropUtil.setTableProperty(RootTable.ID, entry.getKey(), entry.getValue()))
          throw new IOException("Cannot create per-table property " + entry.getKey());
        if (!TablePropUtil.setTableProperty(MetadataTable.ID, entry.getKey(), entry.getValue()))
          throw new IOException("Cannot create per-table property " + entry.getKey());
      }
      // Only add combiner config to accumulo.metadata table (ACCUMULO-3077)
      for (Entry<String,String> entry : initialMetadataCombinerConf.entrySet()) {
        if (!TablePropUtil.setTableProperty(MetadataTable.ID, entry.getKey(), entry.getValue()))
          throw new IOException("Cannot create per-table property " + entry.getKey());
      }

      // add configuration to the replication table
      for (Entry<String,String> entry : initialReplicationTableConf.entrySet()) {
        if (!TablePropUtil.setTableProperty(ReplicationTable.ID, entry.getKey(), entry.getValue()))
          throw new IOException("Cannot create per-table property " + entry.getKey());
      }
    } catch (Exception e) {
      log.error("FATAL: Error talking to ZooKeeper", e);
      throw new IOException(e);
    }
  }

  private static void setMetadataReplication(int replication, String reason) throws IOException {
    String rep = getConsoleReader().readLine(
        "Your HDFS replication " + reason + " is not compatible with our default " + MetadataTable.NAME + " replication of 5. What do you want to set your "
            + MetadataTable.NAME + " replication to? (" + replication + ") ");
    if (rep == null || rep.length() == 0)
      rep = Integer.toString(replication);
    else
      // Lets make sure it's a number
      Integer.parseInt(rep);
    initialMetadataConf.put(Property.TABLE_FILE_REPLICATION.getKey(), rep);
  }

  public static boolean isInitialized(VolumeManager fs) throws IOException {
    for (String baseDir : VolumeConfiguration.getVolumeUris(SiteConfiguration.getInstance())) {
      if (fs.exists(new Path(baseDir, ServerConstants.INSTANCE_ID_DIR)) || fs.exists(new Path(baseDir, ServerConstants.VERSION_DIR)))
        return true;
    }

    return false;
  }

  private static void addVolumes(VolumeManager fs) throws IOException {

    String[] volumeURIs = VolumeConfiguration.getVolumeUris(SiteConfiguration.getInstance());

    HashSet<String> initializedDirs = new HashSet<>();
    initializedDirs.addAll(Arrays.asList(ServerConstants.checkBaseUris(volumeURIs, true)));

    HashSet<String> uinitializedDirs = new HashSet<>();
    uinitializedDirs.addAll(Arrays.asList(volumeURIs));
    uinitializedDirs.removeAll(initializedDirs);

    Path aBasePath = new Path(initializedDirs.iterator().next());
    Path iidPath = new Path(aBasePath, ServerConstants.INSTANCE_ID_DIR);
    Path versionPath = new Path(aBasePath, ServerConstants.VERSION_DIR);

    UUID uuid = UUID.fromString(ZooUtil.getInstanceIDFromHdfs(iidPath, SiteConfiguration.getInstance()));
    for (Pair<Path,Path> replacementVolume : ServerConstants.getVolumeReplacements()) {
      if (aBasePath.equals(replacementVolume.getFirst()))
        log.error("{} is set to be replaced in {} and should not appear in {}"
            + ". It is highly recommended that this property be removed as data could still be written to this volume.", aBasePath,
            Property.INSTANCE_VOLUMES_REPLACEMENTS, Property.INSTANCE_VOLUMES);
    }

    if (ServerConstants.DATA_VERSION != Accumulo.getAccumuloPersistentVersion(versionPath.getFileSystem(CachedConfiguration.getInstance()), versionPath)) {
      throw new IOException("Accumulo " + Constants.VERSION + " cannot initialize data version " + Accumulo.getAccumuloPersistentVersion(fs));
    }

    initDirs(fs, uuid, uinitializedDirs.toArray(new String[uinitializedDirs.size()]), true);
  }

  static class Opts extends Help {
    @Parameter(names = "--add-volumes", description = "Initialize any uninitialized volumes listed in instance.volumes")
    boolean addVolumes = false;
    @Parameter(names = "--reset-security", description = "just update the security information, will prompt")
    boolean resetSecurity = false;
    @Parameter(names = {"-f", "--force"}, description = "force reset of the security information without prompting")
    boolean forceResetSecurity = false;
    @Parameter(names = "--clear-instance-name", description = "delete any existing instance name without prompting")
    boolean clearInstanceName = false;
    @Parameter(names = "--upload-accumulo-site", description = "Uploads properties in accumulo-site.xml to Zookeeper")
    boolean uploadAccumuloSite = false;
    @Parameter(names = "--instance-name", description = "the instance name, if not provided, will prompt")
    String cliInstanceName;
    @Parameter(names = "--password", description = "set the password on the command line")
    String cliPassword;
    @Parameter(names = {"-u", "--user"}, description = "the name of the user to grant system permissions to")
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

    try {
      zoo = ZooReaderWriter.getInstance();
      AccumuloConfiguration acuConf = SiteConfiguration.getInstance();
      SecurityUtil.serverLogin(acuConf);
      Configuration conf = CachedConfiguration.getInstance();

      VolumeManager fs = VolumeManagerImpl.get(acuConf);

      if (opts.resetSecurity) {
        log.info("Resetting security on accumulo.");
        Instance instance = HdfsZooInstance.getInstance();
        AccumuloServerContext context = new AccumuloServerContext(instance, new ServerConfigurationFactory(instance));
        if (isInitialized(fs)) {
          if (!opts.forceResetSecurity) {
            ConsoleReader c = getConsoleReader();
            String userEnteredName = c.readLine("WARNING: This will remove all users from Accumulo! If you wish to proceed enter the instance name: ");
            if (userEnteredName != null && !instance.getInstanceName().equals(userEnteredName)) {
              log.error("Aborted reset security: Instance name did not match current instance.");
              return;
            }
          }

          final String rootUser = getRootUserName(opts);
          opts.rootpass = getRootPassword(opts, rootUser);
          initSecurity(context, opts, HdfsZooInstance.getInstance().getInstanceID(), rootUser);
        } else {
          log.error("FATAL: Attempted to reset security on accumulo before it was initialized");
        }
      }

      if (opts.addVolumes) {
        addVolumes(fs);
      }

      if (!opts.resetSecurity && !opts.addVolumes)
        if (!doInit(opts, conf, fs))
          System.exit(-1);
    } catch (Exception e) {
      log.error("Fatal exception", e);
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) {
    new Initialize().execute(args);
  }
}
