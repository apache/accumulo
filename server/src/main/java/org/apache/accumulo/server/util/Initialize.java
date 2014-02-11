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
package org.apache.accumulo.server.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.UUID;

import jline.ConsoleReader;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.FileUtil;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.master.thrift.MasterGoalState;
import org.apache.accumulo.core.security.SecurityUtil;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.constraints.MetadataConstraints;
import org.apache.accumulo.server.iterators.MetadataBulkLoadFilter;
import org.apache.accumulo.server.master.state.tables.TableManager;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.accumulo.server.security.SecurityConstants;
import org.apache.accumulo.server.tabletserver.TabletTime;
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
        String.format("%s,%s", Constants.METADATA_TABLET_COLUMN_FAMILY.toString(), Constants.METADATA_CURRENT_LOCATION_COLUMN_FAMILY.toString()));
    initialMetadataConf.put(Property.TABLE_LOCALITY_GROUP_PREFIX.getKey() + "server", String.format("%s,%s,%s,%s",
        Constants.METADATA_DATAFILE_COLUMN_FAMILY.toString(), Constants.METADATA_LOG_COLUMN_FAMILY.toString(),
        Constants.METADATA_SERVER_COLUMN_FAMILY.toString(), Constants.METADATA_FUTURE_LOCATION_COLUMN_FAMILY.toString()));
    initialMetadataConf.put(Property.TABLE_LOCALITY_GROUPS.getKey(), "tablet,server");
    initialMetadataConf.put(Property.TABLE_DEFAULT_SCANTIME_VISIBILITY.getKey(), "");
    initialMetadataConf.put(Property.TABLE_INDEXCACHE_ENABLED.getKey(), "true");
    initialMetadataConf.put(Property.TABLE_BLOCKCACHE_ENABLED.getKey(), "true");
  }

  static boolean checkInit(Configuration conf, FileSystem fs, SiteConfiguration sconf) throws IOException {
    String fsUri = fs.getUri().toString();
    log.info("Hadoop Filesystem is " + fsUri);

    log.info("Accumulo data dir is " + ServerConstants.getBaseDir());
    log.info("Zookeeper server is " + sconf.get(Property.INSTANCE_ZK_HOST));
    log.info("Checking if Zookeeper is available. If this hangs, then you need to make sure zookeeper is running");
    if (!zookeeperAvailable()) {
      log.fatal("Zookeeper needs to be up and running in order to init. Exiting ...");
      return false;
    }
    if (sconf.get(Property.INSTANCE_SECRET).equals(Property.INSTANCE_SECRET.getDefaultValue())) {
      ConsoleReader c = getConsoleReader();
      c.beep();
      c.printNewline();
      c.printNewline();
      c.printString("Warning!!! Your instance secret is still set to the default, this is not secure. We highly recommend you change it.");
      c.printNewline();
      c.printNewline();
      c.printNewline();
      c.printString("You can change the instance secret in accumulo by using:");
      c.printNewline();
      c.printString("   bin/accumulo " + org.apache.accumulo.server.util.ChangeSecret.class.getName() + " oldPassword newPassword.");
      c.printNewline();
      c.printString("You will also need to edit your secret in your configuration file by adding the property instance.secret to your conf/accumulo-site.xml. Without this accumulo will not operate correctly");
      c.printNewline();
    }

    try {
      if (isInitialized(fs)) {
        String instanceDfsDir = sconf.get(Property.INSTANCE_DFS_DIR);
        log.fatal("It appears the directory " + fsUri + instanceDfsDir + " was previously initialized.");
        String instanceDfsUri = sconf.get(Property.INSTANCE_DFS_URI);
        if ("".equals(instanceDfsUri)) {
          log.fatal("You are using the default URI for the filesystem. Set the property " + Property.INSTANCE_DFS_URI + " to use a different filesystem,");
        } else {
          log.fatal("Change the property " + Property.INSTANCE_DFS_URI + " to use a different filesystem,");
        }
        log.fatal("or change the property " + Property.INSTANCE_DFS_DIR + " to use a different directory.");
        log.fatal("The current value of " + Property.INSTANCE_DFS_URI + " is |" + instanceDfsUri + "|");
        log.fatal("The current value of " + Property.INSTANCE_DFS_DIR + " is |" + instanceDfsDir + "|");
        return false;
      }
    } catch (IOException e) {
      throw new IOException("Failed to check if filesystem already initialized", e);
    }

    return true;
  }

  public static boolean doInit(Opts opts, Configuration conf, FileSystem fs) throws IOException {
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

  public static boolean initialize(Opts opts, String instanceNamePath, FileSystem fs) {

    UUID uuid = UUID.randomUUID();
    try {
      initZooKeeper(opts, uuid.toString(), instanceNamePath);
    } catch (Exception e) {
      log.fatal("Failed to initialize zookeeper", e);
      return false;
    }

    try {
      initFileSystem(opts, fs, fs.getConf(), uuid);
    } catch (Exception e) {
      log.fatal("Failed to initialize filesystem", e);
      
      // Try to warn the user about what the actual problem is
      Configuration fsConf = fs.getConf();
      
      final String defaultFsUri = "file:///";
      String fsDefaultName = fsConf.get("fs.default.name", defaultFsUri), fsDefaultFS = fsConf.get("fs.defaultFS", defaultFsUri);
      
      // Try to determine when we couldn't find an appropriate core-site.xml on the classpath
      if (defaultFsUri.equals(fsDefaultName) && defaultFsUri.equals(fsDefaultFS)) {
        log.fatal("Default filesystem value ('fs.defaultFS' or 'fs.default.name') was found in the Hadoop configuration");
        log.fatal("Please ensure that the Hadoop core-site.xml is on the classpath using 'general.classpaths' in accumulo-site.xml");
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

  /**
   * @return
   */
  private static boolean zookeeperAvailable() {
    try {
      return zoo.exists("/");
    } catch (KeeperException e) {
      return false;
    } catch (InterruptedException e) {
      return false;
    }
  }

  private static void initFileSystem(Opts opts, FileSystem fs, Configuration conf, UUID uuid) throws IOException {
    FileStatus fstat;

    // the actual disk location of the root tablet
    final Path rootTablet = new Path(ServerConstants.getRootTabletDir());

    final Path tableMetadataTablet = new Path(ServerConstants.getMetadataTableDir() + Constants.TABLE_TABLET_LOCATION);
    final Path defaultMetadataTablet = new Path(ServerConstants.getMetadataTableDir() + Constants.DEFAULT_TABLET_LOCATION);

    final Path metadataTableDir = new Path(ServerConstants.getMetadataTableDir());

    fs.mkdirs(new Path(ServerConstants.getDataVersionLocation(), "" + Constants.DATA_VERSION));

    // create an instance id
    fs.mkdirs(ServerConstants.getInstanceIdLocation());
    fs.createNewFile(new Path(ServerConstants.getInstanceIdLocation(), uuid.toString()));

    // initialize initial metadata config in zookeeper
    initMetadataConfig();

    // create metadata table
    try {
      fstat = fs.getFileStatus(metadataTableDir);
      if (!fstat.isDir()) {
        log.fatal("location " + metadataTableDir.toString() + " exists but is not a directory");
        return;
      }
    } catch (FileNotFoundException fnfe) {
      // create btl dir
      if (!fs.mkdirs(metadataTableDir)) {
        log.fatal("unable to create directory " + metadataTableDir.toString());
        return;
      }
    }

    // create root tablet
    try {
      fstat = fs.getFileStatus(rootTablet);
      if (!fstat.isDir()) {
        log.fatal("location " + rootTablet.toString() + " exists but is not a directory");
        return;
      }
    } catch (FileNotFoundException fnfe) {
      // create btl dir
      if (!fs.mkdirs(rootTablet)) {
        log.fatal("unable to create directory " + rootTablet.toString());
        return;
      }

      // populate the root tablet with info about the default tablet
      // the root tablet contains the key extent and locations of all the
      // metadata tablets
      String initRootTabFile = ServerConstants.getMetadataTableDir() + "/root_tablet/00000_00000."
          + FileOperations.getNewFileExtension(AccumuloConfiguration.getDefaultConfiguration());
      FileSKVWriter mfw = FileOperations.getInstance().openWriter(initRootTabFile, fs, conf, AccumuloConfiguration.getDefaultConfiguration());
      mfw.startDefaultLocalityGroup();

      // -----------] root tablet info
      Text rootExtent = Constants.ROOT_TABLET_EXTENT.getMetadataEntry();

      // root's directory
      Key rootDirKey = new Key(rootExtent, Constants.METADATA_DIRECTORY_COLUMN.getColumnFamily(), Constants.METADATA_DIRECTORY_COLUMN.getColumnQualifier(), 0);
      mfw.append(rootDirKey, new Value("/root_tablet".getBytes(Constants.UTF8)));

      // root's prev row
      Key rootPrevRowKey = new Key(rootExtent, Constants.METADATA_PREV_ROW_COLUMN.getColumnFamily(), Constants.METADATA_PREV_ROW_COLUMN.getColumnQualifier(), 0);
      mfw.append(rootPrevRowKey, new Value(new byte[] {0}));

      // ----------] table tablet info
      Text tableExtent = new Text(KeyExtent.getMetadataEntry(new Text(Constants.METADATA_TABLE_ID), Constants.METADATA_RESERVED_KEYSPACE_START_KEY.getRow()));

      // table tablet's directory
      Key tableDirKey = new Key(tableExtent, Constants.METADATA_DIRECTORY_COLUMN.getColumnFamily(), Constants.METADATA_DIRECTORY_COLUMN.getColumnQualifier(), 0);
      mfw.append(tableDirKey, new Value(Constants.TABLE_TABLET_LOCATION.getBytes(Constants.UTF8)));

      // table tablet time
      Key tableTimeKey = new Key(tableExtent, Constants.METADATA_TIME_COLUMN.getColumnFamily(), Constants.METADATA_TIME_COLUMN.getColumnQualifier(), 0);
      mfw.append(tableTimeKey, new Value((TabletTime.LOGICAL_TIME_ID + "0").getBytes(Constants.UTF8)));

      // table tablet's prevrow
      Key tablePrevRowKey = new Key(tableExtent, Constants.METADATA_PREV_ROW_COLUMN.getColumnFamily(), Constants.METADATA_PREV_ROW_COLUMN.getColumnQualifier(),
          0);
      mfw.append(tablePrevRowKey, KeyExtent.encodePrevEndRow(new Text(KeyExtent.getMetadataEntry(new Text(Constants.METADATA_TABLE_ID), null))));

      // ----------] default tablet info
      Text defaultExtent = new Text(KeyExtent.getMetadataEntry(new Text(Constants.METADATA_TABLE_ID), null));

      // default's directory
      Key defaultDirKey = new Key(defaultExtent, Constants.METADATA_DIRECTORY_COLUMN.getColumnFamily(),
          Constants.METADATA_DIRECTORY_COLUMN.getColumnQualifier(), 0);
      mfw.append(defaultDirKey, new Value(Constants.DEFAULT_TABLET_LOCATION.getBytes(Constants.UTF8)));

      // default's time
      Key defaultTimeKey = new Key(defaultExtent, Constants.METADATA_TIME_COLUMN.getColumnFamily(), Constants.METADATA_TIME_COLUMN.getColumnQualifier(), 0);
      mfw.append(defaultTimeKey, new Value((TabletTime.LOGICAL_TIME_ID + "0").getBytes(Constants.UTF8)));

      // default's prevrow
      Key defaultPrevRowKey = new Key(defaultExtent, Constants.METADATA_PREV_ROW_COLUMN.getColumnFamily(),
          Constants.METADATA_PREV_ROW_COLUMN.getColumnQualifier(), 0);
      mfw.append(defaultPrevRowKey, KeyExtent.encodePrevEndRow(Constants.METADATA_RESERVED_KEYSPACE_START_KEY.getRow()));

      mfw.close();
    }

    // create table and default tablets directories
    try {
      fstat = fs.getFileStatus(defaultMetadataTablet);
      if (!fstat.isDir()) {
        log.fatal("location " + defaultMetadataTablet.toString() + " exists but is not a directory");
        return;
      }
    } catch (FileNotFoundException fnfe) {
      try {
        fstat = fs.getFileStatus(tableMetadataTablet);
        if (!fstat.isDir()) {
          log.fatal("location " + tableMetadataTablet.toString() + " exists but is not a directory");
          return;
        }
      } catch (FileNotFoundException fnfe2) {
        // create table info dir
        if (!fs.mkdirs(tableMetadataTablet)) {
          log.fatal("unable to create directory " + tableMetadataTablet.toString());
          return;
        }
      }

      // create default dir
      if (!fs.mkdirs(defaultMetadataTablet)) {
        log.fatal("unable to create directory " + defaultMetadataTablet.toString());
        return;
      }
    }
  }

  private static void initZooKeeper(Opts opts, String uuid, String instanceNamePath) throws KeeperException, InterruptedException {
    // setup basic data in zookeeper
    ZooUtil.putPersistentData(zoo.getZooKeeper(), Constants.ZROOT, new byte[0], -1, NodeExistsPolicy.SKIP, Ids.OPEN_ACL_UNSAFE);
    ZooUtil.putPersistentData(zoo.getZooKeeper(), Constants.ZROOT + Constants.ZINSTANCES, new byte[0], -1, NodeExistsPolicy.SKIP, Ids.OPEN_ACL_UNSAFE);

    // setup instance name
    if (opts.clearInstanceName)
      zoo.recursiveDelete(instanceNamePath, NodeMissingPolicy.SKIP);
    zoo.putPersistentData(instanceNamePath, uuid.getBytes(Constants.UTF8), NodeExistsPolicy.FAIL);

    final byte[] EMPTY_BYTE_ARRAY = new byte[0], ZERO_CHAR_ARRAY = new byte[] {'0'};
    
    // setup the instance
    String zkInstanceRoot = Constants.ZROOT + "/" + uuid;
    zoo.putPersistentData(zkInstanceRoot, new byte[0], NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZTABLES, Constants.ZTABLES_INITIAL_ID, NodeExistsPolicy.FAIL);
    TableManager.prepareNewTableState(uuid, Constants.METADATA_TABLE_ID, Constants.METADATA_TABLE_NAME, TableState.ONLINE, NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZTSERVERS, EMPTY_BYTE_ARRAY, NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZPROBLEMS, EMPTY_BYTE_ARRAY, NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZROOT_TABLET, EMPTY_BYTE_ARRAY, NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZROOT_TABLET_WALOGS, EMPTY_BYTE_ARRAY, NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZTRACERS, EMPTY_BYTE_ARRAY, NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZMASTERS, EMPTY_BYTE_ARRAY, NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZMASTER_LOCK, EMPTY_BYTE_ARRAY, NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZMASTER_GOAL_STATE, MasterGoalState.NORMAL.toString().getBytes(Constants.UTF8), NodeExistsPolicy.FAIL);
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
      return opts.cliPassword.getBytes(Constants.UTF8);
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
    return rootpass.getBytes(Constants.UTF8);
  }

  private static void initSecurity(Opts opts, String iid) throws AccumuloSecurityException, ThriftSecurityException {
    AuditedSecurityOperation.getInstance(iid, true).initializeSecurity(SecurityConstants.getSystemCredentials(), DEFAULT_ROOT_USER, opts.rootpass);
  }

  protected static void initMetadataConfig() throws IOException {
    try {
      Configuration conf = CachedConfiguration.getInstance();
      int max = conf.getInt("dfs.replication.max", 512);
      // Hadoop 0.23 switched the min value configuration name
      int min = Math.max(conf.getInt("dfs.replication.min", 1), conf.getInt("dfs.namenode.replication.min", 1));
      if (max < 5)
        setMetadataReplication(max, "max");
      if (min > 5)
        setMetadataReplication(min, "min");
      for (Entry<String,String> entry : initialMetadataConf.entrySet())
        if (!TablePropUtil.setTableProperty(Constants.METADATA_TABLE_ID, entry.getKey(), entry.getValue()))
          throw new IOException("Cannot create per-table property " + entry.getKey());
    } catch (Exception e) {
      log.fatal("error talking to zookeeper", e);
      throw new IOException(e);
    }
  }

  private static void setMetadataReplication(int replication, String reason) throws IOException {
    String rep = getConsoleReader().readLine(
        "Your HDFS replication " + reason
            + " is not compatible with our default !METADATA replication of 5. What do you want to set your !METADATA replication to? (" + replication + ") ");
    if (rep == null || rep.length() == 0)
      rep = Integer.toString(replication);
    else
      // Lets make sure it's a number
      Integer.parseInt(rep);
    initialMetadataConf.put(Property.TABLE_FILE_REPLICATION.getKey(), rep);
  }

  public static boolean isInitialized(FileSystem fs) throws IOException {
    return (fs.exists(ServerConstants.getInstanceIdLocation()) || fs.exists(ServerConstants.getDataVersionLocation()));
  }

  static class Opts extends Help {
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
      SecurityUtil.serverLogin();
      Configuration conf = CachedConfiguration.getInstance();

      FileSystem fs = FileUtil.getFileSystem(conf, ServerConfiguration.getSiteConfiguration());

      if (opts.resetSecurity) {
        if (isInitialized(fs)) {
          opts.rootpass = getRootPassword(opts);
          initSecurity(opts, HdfsZooInstance.getInstance().getInstanceID());
        } else {
          log.fatal("Attempted to reset security on accumulo before it was initialized");
        }
      } else if (!doInit(opts, conf, fs))
        System.exit(-1);
    } catch (Exception e) {
      log.fatal(e, e);
      throw new RuntimeException(e);
    }
  }
}
