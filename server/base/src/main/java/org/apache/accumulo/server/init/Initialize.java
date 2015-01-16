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

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.TIME_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.UUID;

import jline.console.ConsoleReader;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.impl.Namespaces;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.master.thrift.MasterGoalState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.volume.VolumeConfiguration;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.Accumulo;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.constraints.MetadataConstraints;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.iterators.MetadataBulkLoadFilter;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.accumulo.server.tablets.TabletTime;
import org.apache.accumulo.server.util.TablePropUtil;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

import com.beust.jcommander.Parameter;

/**
 * This class is used to setup the directory structure and the root tablet to get an instance started
 *
 */
public class Initialize {
  private static final Logger log = Logger.getLogger(Initialize.class);
  private static final String DEFAULT_ROOT_USER = "root";
  public static final String TABLE_TABLETS_TABLET_DIR = "/table_info";

  private static ConsoleReader reader = null;
  private static IZooReaderWriter zoo = ZooReaderWriter.getInstance();

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

  private static HashMap<String,String> initialMetadataConf = new HashMap<String,String>();

  static {
    initialMetadataConf.put(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), "32K");
    initialMetadataConf.put(Property.TABLE_FILE_REPLICATION.getKey(), "5");
    initialMetadataConf.put(Property.TABLE_WALOG_ENABLED.getKey(), "true");
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
        String.format("%s,%s", TabletsSection.TabletColumnFamily.NAME, TabletsSection.CurrentLocationColumnFamily.NAME));
    initialMetadataConf.put(Property.TABLE_LOCALITY_GROUP_PREFIX.getKey() + "server", String.format("%s,%s,%s,%s", TabletsSection.DataFileColumnFamily.NAME,
        TabletsSection.LogColumnFamily.NAME, TabletsSection.ServerColumnFamily.NAME, TabletsSection.FutureLocationColumnFamily.NAME));
    initialMetadataConf.put(Property.TABLE_LOCALITY_GROUPS.getKey(), "tablet,server");
    initialMetadataConf.put(Property.TABLE_DEFAULT_SCANTIME_VISIBILITY.getKey(), "");
    initialMetadataConf.put(Property.TABLE_INDEXCACHE_ENABLED.getKey(), "true");
    initialMetadataConf.put(Property.TABLE_BLOCKCACHE_ENABLED.getKey(), "true");
  }

  static boolean checkInit(Configuration conf, VolumeManager fs, SiteConfiguration sconf) throws IOException {
    @SuppressWarnings("deprecation")
    String fsUri = sconf.get(Property.INSTANCE_DFS_URI);
    if (fsUri.equals(""))
      fsUri = FileSystem.getDefaultUri(conf).toString();
    log.info("Hadoop Filesystem is " + fsUri);
    log.info("Accumulo data dirs are " + Arrays.asList(VolumeConfiguration.getVolumeUris(ServerConfiguration.getSiteConfiguration())));
    log.info("Zookeeper server is " + sconf.get(Property.INSTANCE_ZK_HOST));
    log.info("Checking if Zookeeper is available. If this hangs, then you need to make sure zookeeper is running");
    if (!zookeeperAvailable()) {
      log.fatal("Zookeeper needs to be up and running in order to init. Exiting ...");
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
      c.println("   bin/accumulo " + org.apache.accumulo.server.util.ChangeSecret.class.getName() + " oldPassword newPassword.");
      c.println("You will also need to edit your secret in your configuration file by adding the property instance.secret to your conf/accumulo-site.xml. "
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
    log.fatal("It appears the directories " + Arrays.asList(VolumeConfiguration.getVolumeUris(ServerConfiguration.getSiteConfiguration()))
        + " were previously initialized.");
    String instanceVolumes = sconf.get(Property.INSTANCE_VOLUMES);
    String instanceDfsUri = sconf.get(INSTANCE_DFS_URI);

    if (!instanceVolumes.isEmpty()) {
      log.fatal("Change the property " + Property.INSTANCE_VOLUMES + " to use different filesystems,");
    } else if (!instanceDfsDir.isEmpty()) {
      log.fatal("Change the property " + INSTANCE_DFS_URI + " to use a different filesystem,");
    } else {
      log.fatal("You are using the default URI for the filesystem. Set the property " + Property.INSTANCE_VOLUMES + " to use a different filesystem,");
    }
    log.fatal("or change the property " + INSTANCE_DFS_DIR + " to use a different directory.");
    log.fatal("The current value of " + INSTANCE_DFS_URI + " is |" + instanceDfsUri + "|");
    log.fatal("The current value of " + INSTANCE_DFS_DIR + " is |" + instanceDfsDir + "|");
    log.fatal("The current value of " + Property.INSTANCE_VOLUMES + " is |" + instanceVolumes + "|");
  }

  public static boolean doInit(Opts opts, Configuration conf, VolumeManager fs) throws IOException {
    if (!checkInit(conf, fs, ServerConfiguration.getSiteConfiguration())) {
      return false;
    }

    // prompt user for instance name and root password early, in case they
    // abort, we don't leave an inconsistent HDFS/ZooKeeper structure
    String instanceNamePath;
    try {
      instanceNamePath = getInstanceNamePath(opts);
    } catch (Exception e) {
      log.fatal("Failed to talk to zookeeper", e);
      return false;
    }
    opts.rootpass = getRootPassword(opts);
    return initialize(opts, instanceNamePath, fs);
  }

  public static boolean initialize(Opts opts, String instanceNamePath, VolumeManager fs) {

    UUID uuid = UUID.randomUUID();
    // the actual disk locations of the root table and tablets
    String[] configuredVolumes = VolumeConfiguration.getVolumeUris(ServerConfiguration.getSiteConfiguration());
    final String rootTabletDir = new Path(fs.choose(configuredVolumes) + Path.SEPARATOR + ServerConstants.TABLE_DIR + Path.SEPARATOR + RootTable.ID
        + RootTable.ROOT_TABLET_LOCATION).toString();

    try {
      initZooKeeper(opts, uuid.toString(), instanceNamePath, rootTabletDir);
    } catch (Exception e) {
      log.fatal("Failed to initialize zookeeper", e);
      return false;
    }

    try {
      initFileSystem(opts, fs, uuid, rootTabletDir);
    } catch (Exception e) {
      log.fatal("Failed to initialize filesystem", e);

      if (ServerConfiguration.getSiteConfiguration().get(Property.INSTANCE_VOLUMES).trim().equals("")) {
        Configuration fsConf = CachedConfiguration.getInstance();

        final String defaultFsUri = "file:///";
        String fsDefaultName = fsConf.get("fs.default.name", defaultFsUri), fsDefaultFS = fsConf.get("fs.defaultFS", defaultFsUri);

        // Try to determine when we couldn't find an appropriate core-site.xml on the classpath
        if (defaultFsUri.equals(fsDefaultName) && defaultFsUri.equals(fsDefaultFS)) {
          log.fatal("Default filesystem value ('fs.defaultFS' or 'fs.default.name') of '" + defaultFsUri + "' was found in the Hadoop configuration");
          log.fatal("Please ensure that the Hadoop core-site.xml is on the classpath using 'general.classpaths' in accumulo-site.xml");
        }
      }

      return false;
    }

    try {
      initSecurity(opts, uuid.toString());
    } catch (Exception e) {
      log.fatal("Failed to initialize security", e);
      return false;
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
      fs.mkdirs(new Path(new Path(baseDir, ServerConstants.VERSION_DIR), "" + ServerConstants.DATA_VERSION));

      // create an instance id
      Path iidLocation = new Path(baseDir, ServerConstants.INSTANCE_ID_DIR);
      fs.mkdirs(iidLocation);
      fs.createNewFile(new Path(iidLocation, uuid.toString()));
      if (print)
        log.info("Initialized volume " + baseDir);
    }
  }

  private static void initFileSystem(Opts opts, VolumeManager fs, UUID uuid, String rootTabletDir) throws IOException {
    initDirs(fs, uuid, VolumeConfiguration.getVolumeUris(ServerConfiguration.getSiteConfiguration()), false);

    // initialize initial metadata config in zookeeper
    initMetadataConfig();

    String tableMetadataTabletDir = fs.choose(ServerConstants.getBaseUris()) + Constants.HDFS_TABLES_DIR + Path.SEPARATOR + MetadataTable.ID
        + TABLE_TABLETS_TABLET_DIR;
    String defaultMetadataTabletDir = fs.choose(ServerConstants.getBaseUris()) + Constants.HDFS_TABLES_DIR + Path.SEPARATOR + MetadataTable.ID
        + Constants.DEFAULT_TABLET_LOCATION;

    // create table and default tablets directories
    createDirectories(fs, rootTabletDir, tableMetadataTabletDir, defaultMetadataTabletDir);

    // populate the root tablet with info about the metadata tablets
    String fileName = rootTabletDir + Path.SEPARATOR + "00000_00000." + FileOperations.getNewFileExtension(AccumuloConfiguration.getDefaultConfiguration());
    createMetadataFile(fs, fileName, MetadataTable.ID, tableMetadataTabletDir, defaultMetadataTabletDir);
  }

  /**
   * Create an rfile in the default tablet's directory for a new table. This method is used to create the initial root tablet contents, with information about
   * the metadata table's tablets
   *
   * @param volmanager
   *          The VolumeManager
   * @param fileName
   *          The location to create the file
   * @param tableId
   *          TableID that is being "created"
   * @param tableTabletDir
   *          The table_info directory for the new table
   * @param defaultTabletDir
   *          The default_tablet directory for the new table
   */
  private static void createMetadataFile(VolumeManager volmanager, String fileName, String tableId, String tableTabletDir, String defaultTabletDir)
      throws IOException {
    FileSystem fs = volmanager.getVolumeByPath(new Path(fileName)).getFileSystem();
    FileSKVWriter tabletWriter = FileOperations.getInstance().openWriter(fileName, fs, fs.getConf(), AccumuloConfiguration.getDefaultConfiguration());
    tabletWriter.startDefaultLocalityGroup();

    Text splitPoint = TabletsSection.getRange().getEndKey().getRow();
    createEntriesForTablet(tabletWriter, tableId, tableTabletDir, null, splitPoint);
    createEntriesForTablet(tabletWriter, tableId, defaultTabletDir, splitPoint, null);

    tabletWriter.close();
  }

  private static void createEntriesForTablet(FileSKVWriter writer, String tableId, String tabletDir, Text tabletPrevEndRow, Text tabletEndRow)
      throws IOException {
    Text extent = new Text(KeyExtent.getMetadataEntry(new Text(tableId), tabletEndRow));
    addEntry(writer, extent, DIRECTORY_COLUMN, new Value(tabletDir.getBytes(UTF_8)));
    addEntry(writer, extent, TIME_COLUMN, new Value((TabletTime.LOGICAL_TIME_ID + "0").getBytes(UTF_8)));
    addEntry(writer, extent, PREV_ROW_COLUMN, KeyExtent.encodePrevEndRow(tabletPrevEndRow));
  }

  private static void addEntry(FileSKVWriter writer, Text row, ColumnFQ col, Value value) throws IOException {
    writer.append(new Key(row, col.getColumnFamily(), col.getColumnQualifier(), 0), value);
  }

  private static void createDirectories(VolumeManager fs, String... dirs) throws IOException {
    for (String s : dirs) {
      Path dir = new Path(s);
      try {
        FileStatus fstat = fs.getFileStatus(dir);
        // TODO Remove deprecation warning suppression when Hadoop1 support is dropped
        @SuppressWarnings("deprecation")
        boolean isDirectory = fstat.isDir();
        if (!isDirectory) {
          log.fatal("location " + dir + " exists but is not a directory");
          return;
        }
      } catch (FileNotFoundException fnfe) {
        // attempt to create directory, since it doesn't exist
        if (!fs.mkdirs(dir)) {
          log.fatal("unable to create directory " + dir);
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
    TableManager.prepareNewNamespaceState(uuid, Namespaces.DEFAULT_NAMESPACE_ID, Namespaces.DEFAULT_NAMESPACE, NodeExistsPolicy.FAIL);
    TableManager.prepareNewNamespaceState(uuid, Namespaces.ACCUMULO_NAMESPACE_ID, Namespaces.ACCUMULO_NAMESPACE, NodeExistsPolicy.FAIL);
    TableManager.prepareNewTableState(uuid, RootTable.ID, Namespaces.ACCUMULO_NAMESPACE_ID, RootTable.NAME, TableState.ONLINE, NodeExistsPolicy.FAIL);
    TableManager.prepareNewTableState(uuid, MetadataTable.ID, Namespaces.ACCUMULO_NAMESPACE_ID, MetadataTable.NAME, TableState.ONLINE, NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZTSERVERS, EMPTY_BYTE_ARRAY, NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZPROBLEMS, EMPTY_BYTE_ARRAY, NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + RootTable.ZROOT_TABLET, EMPTY_BYTE_ARRAY, NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + RootTable.ZROOT_TABLET_WALOGS, EMPTY_BYTE_ARRAY, NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + RootTable.ZROOT_TABLET_PATH, rootTabletDir.getBytes(UTF_8), NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZTRACERS, EMPTY_BYTE_ARRAY, NodeExistsPolicy.FAIL);
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
  }

  private static String getInstanceNamePath(Opts opts) throws IOException, KeeperException, InterruptedException {
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
      } else if (exists = zoo.exists(instanceNamePath)) {
        String decision = getConsoleReader().readLine("Instance name \"" + instanceName + "\" exists. Delete existing entry from zookeeper? [Y/N] : ");
        if (decision == null)
          System.exit(0);
        if (decision.length() == 1 && decision.toLowerCase(Locale.ENGLISH).charAt(0) == 'y') {
          opts.clearInstanceName = true;
          exists = false;
        }
      }
    } while (exists);
    return instanceNamePath;
  }

  private static byte[] getRootPassword(Opts opts) throws IOException {
    if (opts.cliPassword != null) {
      return opts.cliPassword.getBytes(UTF_8);
    }
    String rootpass;
    String confirmpass;
    do {
      rootpass = getConsoleReader()
          .readLine("Enter initial password for " + DEFAULT_ROOT_USER + " (this may not be applicable for your security setup): ", '*');
      if (rootpass == null)
        System.exit(0);
      confirmpass = getConsoleReader().readLine("Confirm initial password for " + DEFAULT_ROOT_USER + ": ", '*');
      if (confirmpass == null)
        System.exit(0);
      if (!rootpass.equals(confirmpass))
        log.error("Passwords do not match");
    } while (!rootpass.equals(confirmpass));
    return rootpass.getBytes(UTF_8);
  }

  private static void initSecurity(Opts opts, String iid) throws AccumuloSecurityException, ThriftSecurityException {
    AuditedSecurityOperation.getInstance(iid, true).initializeSecurity(SystemCredentials.get().toThrift(HdfsZooInstance.getInstance()), DEFAULT_ROOT_USER,
        opts.rootpass);
  }

  public static void initMetadataConfig() throws IOException {
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
    } catch (Exception e) {
      log.fatal("error talking to zookeeper", e);
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
    for (String baseDir : VolumeConfiguration.getVolumeUris(ServerConfiguration.getSiteConfiguration())) {
      if (fs.exists(new Path(baseDir, ServerConstants.INSTANCE_ID_DIR)) || fs.exists(new Path(baseDir, ServerConstants.VERSION_DIR)))
        return true;
    }

    return false;
  }

  private static void addVolumes(VolumeManager fs) throws IOException {

    String[] volumeURIs = VolumeConfiguration.getVolumeUris(ServerConfiguration.getSiteConfiguration());

    HashSet<String> initializedDirs = new HashSet<String>();
    initializedDirs.addAll(Arrays.asList(ServerConstants.checkBaseUris(volumeURIs, true)));

    HashSet<String> uinitializedDirs = new HashSet<String>();
    uinitializedDirs.addAll(Arrays.asList(volumeURIs));
    uinitializedDirs.removeAll(initializedDirs);

    Path aBasePath = new Path(initializedDirs.iterator().next());
    Path iidPath = new Path(aBasePath, ServerConstants.INSTANCE_ID_DIR);
    Path versionPath = new Path(aBasePath, ServerConstants.VERSION_DIR);

    for (Pair<Path,Path> replacementVolume : ServerConstants.getVolumeReplacements()) {
      if (aBasePath.equals(replacementVolume.getSecond()))
        log.error(aBasePath + " is set to be replaced in " + Property.INSTANCE_VOLUMES_REPLACEMENTS + " and should not appear in " +
            Property.INSTANCE_VOLUMES + ". It is highly recommended that this property be removed as data could still be written to this volume.");
    }

    UUID uuid = UUID.fromString(ZooUtil.getInstanceIDFromHdfs(iidPath, ServerConfiguration.getSiteConfiguration()));

    if (ServerConstants.DATA_VERSION != Accumulo.getAccumuloPersistentVersion(versionPath.getFileSystem(CachedConfiguration.getInstance()), versionPath)) {
      throw new IOException("Accumulo " + Constants.VERSION + " cannot initialize data version " + Accumulo.getAccumuloPersistentVersion(fs));
    }

    initDirs(fs, uuid, uinitializedDirs.toArray(new String[uinitializedDirs.size()]), true);
  }

  static class Opts extends Help {
    @Parameter(names = "--add-volumes", description = "Initialize any uninitialized volumes listed in instance.volumes")
    boolean addVolumes = false;
    @Parameter(names = "--reset-security", description = "just update the security information")
    boolean resetSecurity = false;
    @Parameter(names = "--clear-instance-name", description = "delete any existing instance name without prompting")
    boolean clearInstanceName = false;
    @Parameter(names = "--instance-name", description = "the instance name, if not provided, will prompt")
    String cliInstanceName;
    @Parameter(names = "--password", description = "set the password on the command line")
    String cliPassword;

    byte[] rootpass = null;
  }

  public static void main(String[] args) {
    Opts opts = new Opts();
    opts.parseArgs(Initialize.class.getName(), args);

    try {
      AccumuloConfiguration acuConf = ServerConfiguration.getSiteConfiguration();
      SecurityUtil.serverLogin(acuConf);
      Configuration conf = CachedConfiguration.getInstance();

      VolumeManager fs = VolumeManagerImpl.get(acuConf);

      if (opts.resetSecurity) {
        if (isInitialized(fs)) {
          opts.rootpass = getRootPassword(opts);
          initSecurity(opts, HdfsZooInstance.getInstance().getInstanceID());
        } else {
          log.fatal("Attempted to reset security on accumulo before it was initialized");
        }
      }

      if (opts.addVolumes) {
        addVolumes(fs);
      }

      if (!opts.resetSecurity && !opts.addVolumes)
        if (!doInit(opts, conf, fs))
          System.exit(-1);
    } catch (Exception e) {
      log.fatal(e, e);
      throw new RuntimeException(e);
    }
  }
}
