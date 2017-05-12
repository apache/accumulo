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

import static java.util.Objects.requireNonNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.ClientExec;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.trace.Tracer;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;
import org.apache.accumulo.fate.zookeeper.ZooLock;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.cli.ClientOpts;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.auto.service.AutoService;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;

@AutoService(KeywordExecutable.class)
public class Admin implements KeywordExecutable {
  private static final Logger log = LoggerFactory.getLogger(Admin.class);

  static class AdminOpts extends ClientOpts {
    @Parameter(names = {"-f", "--force"}, description = "force the given server to stop by removing its lock")
    boolean force = false;
  }

  @Parameters(commandDescription = "stop the tablet server on the given hosts")
  static class StopCommand {
    @Parameter(description = "<host> {<host> ... }")
    List<String> args = new ArrayList<>();
  }

  @Parameters(commandDescription = "Ping tablet servers.  If no arguments, pings all.")
  static class PingCommand {
    @Parameter(description = "{<host> ... }")
    List<String> args = new ArrayList<>();
  }

  @Parameters(commandDescription = "print tablets that are offline in online tables")
  static class CheckTabletsCommand {
    @Parameter(names = "--fixFiles", description = "Remove dangling file pointers")
    boolean fixFiles = false;

    @Parameter(names = {"-t", "--table"}, description = "Table to check, if not set checks all tables")
    String tableName = null;
  }

  @Parameters(commandDescription = "stop the master")
  static class StopMasterCommand {}

  @Parameters(commandDescription = "stop all the servers")
  static class StopAllCommand {}

  @Parameters(commandDescription = "list Accumulo instances in zookeeper")
  static class ListInstancesCommand {
    @Parameter(names = "--print-errors", description = "display errors while listing instances")
    boolean printErrors = false;
    @Parameter(names = "--print-all", description = "print information for all instances, not just those with names")
    boolean printAll = false;
  }

  @Parameters(commandDescription = "Accumulo volume utility")
  static class VolumesCommand {
    @Parameter(names = {"-l", "--list"}, description = "list volumes currently in use")
    boolean printErrors = false;
  }

  @Parameters(commandDescription = "print out non-default configuration settings")
  static class DumpConfigCommand {
    @Parameter(names = {"-a", "--all"}, description = "print the system and all table configurations")
    boolean allConfiguration = false;
    @Parameter(names = {"-d", "--directory"}, description = "directory to place config files")
    String directory = null;
    @Parameter(names = {"-s", "--system"}, description = "print the system configuration")
    boolean systemConfiguration = false;
    @Parameter(names = {"-n", "--namespaces"}, description = "print the namespace configuration")
    boolean namespaceConfiguration = false;
    @Parameter(names = {"-t", "--tables"}, description = "print per-table configuration")
    List<String> tables = new ArrayList<>();
    @Parameter(names = {"-u", "--users"}, description = "print users and their authorizations and permissions")
    boolean users = false;
  }

  @Parameters(commandDescription = "redistribute tablet directories across the current volume list")
  static class RandomizeVolumesCommand {
    @Parameter(names = {"-t"}, description = "table to update", required = true)
    String tableName = null;
  }

  public static void main(String[] args) {
    new Admin().execute(args);
  }

  @Override
  public String keyword() {
    return "admin";
  }

  @Override
  public String description() {
    return "Executes administrative commands";
  }

  @Override
  public void execute(final String[] args) {
    boolean everything;

    AdminOpts opts = new AdminOpts();
    JCommander cl = new JCommander(opts);
    cl.setProgramName("accumulo admin");

    CheckTabletsCommand checkTabletsCommand = new CheckTabletsCommand();
    cl.addCommand("checkTablets", checkTabletsCommand);

    ListInstancesCommand listIntancesOpts = new ListInstancesCommand();
    cl.addCommand("listInstances", listIntancesOpts);

    PingCommand pingCommand = new PingCommand();
    cl.addCommand("ping", pingCommand);

    DumpConfigCommand dumpConfigCommand = new DumpConfigCommand();
    cl.addCommand("dumpConfig", dumpConfigCommand);

    VolumesCommand volumesCommand = new VolumesCommand();
    cl.addCommand("volumes", volumesCommand);

    StopCommand stopOpts = new StopCommand();
    cl.addCommand("stop", stopOpts);
    StopAllCommand stopAllOpts = new StopAllCommand();
    cl.addCommand("stopAll", stopAllOpts);
    StopMasterCommand stopMasterOpts = new StopMasterCommand();
    cl.addCommand("stopMaster", stopMasterOpts);

    RandomizeVolumesCommand randomizeVolumesOpts = new RandomizeVolumesCommand();
    cl.addCommand("randomizeVolumes", randomizeVolumesOpts);

    cl.parse(args);

    if (opts.help || cl.getParsedCommand() == null) {
      cl.usage();
      return;
    }

    AccumuloConfiguration siteConf = SiteConfiguration.getInstance();
    // Login as the server on secure HDFS
    if (siteConf.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
      SecurityUtil.serverLogin(siteConf);
    }

    Instance instance = opts.getInstance();
    ServerConfigurationFactory confFactory = new ServerConfigurationFactory(instance);

    try {
      ClientContext context = new AccumuloServerContext(confFactory);

      int rc = 0;

      if (cl.getParsedCommand().equals("listInstances")) {
        ListInstances.listInstances(instance.getZooKeepers(), listIntancesOpts.printAll, listIntancesOpts.printErrors);
      } else if (cl.getParsedCommand().equals("ping")) {
        if (ping(context, pingCommand.args) != 0)
          rc = 4;
      } else if (cl.getParsedCommand().equals("checkTablets")) {
        System.out.println("\n*** Looking for offline tablets ***\n");
        if (FindOfflineTablets.findOffline(context, checkTabletsCommand.tableName) != 0)
          rc = 5;
        System.out.println("\n*** Looking for missing files ***\n");
        if (checkTabletsCommand.tableName == null) {
          if (RemoveEntriesForMissingFiles.checkAllTables(context, checkTabletsCommand.fixFiles) != 0)
            rc = 6;
        } else {
          if (RemoveEntriesForMissingFiles.checkTable(context, checkTabletsCommand.tableName, checkTabletsCommand.fixFiles) != 0)
            rc = 6;
        }

      } else if (cl.getParsedCommand().equals("stop")) {
        stopTabletServer(context, stopOpts.args, opts.force);
      } else if (cl.getParsedCommand().equals("dumpConfig")) {
        printConfig(context, dumpConfigCommand);
      } else if (cl.getParsedCommand().equals("volumes")) {
        ListVolumesUsed.listVolumes(context);
      } else if (cl.getParsedCommand().equals("randomizeVolumes")) {
        rc = RandomizeVolumes.randomize(context.getConnector(), randomizeVolumesOpts.tableName);
      } else {
        everything = cl.getParsedCommand().equals("stopAll");

        if (everything)
          flushAll(context);

        stopServer(context, everything);
      }

      if (rc != 0)
        System.exit(rc);
    } catch (AccumuloException e) {
      log.error("{}", e.getMessage(), e);
      System.exit(1);
    } catch (AccumuloSecurityException e) {
      log.error("{}", e.getMessage(), e);
      System.exit(2);
    } catch (Exception e) {
      log.error("{}", e.getMessage(), e);
      System.exit(3);
    }
  }

  private static int ping(ClientContext context, List<String> args) throws AccumuloException, AccumuloSecurityException {

    InstanceOperations io = context.getConnector().instanceOperations();

    if (args.size() == 0) {
      args = io.getTabletServers();
    }

    int unreachable = 0;

    for (String tserver : args) {
      try {
        io.ping(tserver);
        System.out.println(tserver + " OK");
      } catch (AccumuloException ae) {
        System.out.println(tserver + " FAILED (" + ae.getMessage() + ")");
        unreachable++;
      }
    }

    System.out.printf("\n%d of %d tablet servers unreachable\n\n", unreachable, args.size());
    return unreachable;
  }

  /**
   * flushing during shutdown is a performance optimization, its not required. The method will make an attempt to initiate flushes of all tables and give up if
   * it takes too long.
   *
   */
  private static void flushAll(final ClientContext context) throws AccumuloException, AccumuloSecurityException {

    final AtomicInteger flushesStarted = new AtomicInteger(0);

    Runnable flushTask = new Runnable() {

      @Override
      public void run() {
        try {
          Connector conn = context.getConnector();
          Set<String> tables = conn.tableOperations().tableIdMap().keySet();
          for (String table : tables) {
            if (table.equals(MetadataTable.NAME))
              continue;
            try {
              conn.tableOperations().flush(table, null, null, false);
              flushesStarted.incrementAndGet();
            } catch (TableNotFoundException e) {
              // ignore
            }
          }
        } catch (Exception e) {
          log.warn("Failed to intiate flush {}", e.getMessage());
        }
      }
    };

    Thread flusher = new Thread(flushTask);
    flusher.setDaemon(true);
    flusher.start();

    long start = System.currentTimeMillis();
    try {
      flusher.join(3000);
    } catch (InterruptedException e) {
      // ignore
    }

    while (flusher.isAlive() && System.currentTimeMillis() - start < 15000) {
      int flushCount = flushesStarted.get();
      try {
        flusher.join(1000);
      } catch (InterruptedException e) {
        // ignore
      }

      if (flushCount == flushesStarted.get()) {
        // no progress was made while waiting for join... maybe its stuck, stop waiting on it
        break;
      }
    }
  }

  private static void stopServer(final ClientContext context, final boolean tabletServersToo) throws AccumuloException, AccumuloSecurityException {
    MasterClient.executeVoid(context, new ClientExec<MasterClientService.Client>() {
      @Override
      public void execute(MasterClientService.Client client) throws Exception {
        client.shutdown(Tracer.traceInfo(), context.rpcCreds(), tabletServersToo);
      }
    });
  }

  private static void stopTabletServer(final ClientContext context, List<String> servers, final boolean force) throws AccumuloException,
      AccumuloSecurityException {
    if (context.getInstance().getMasterLocations().size() == 0) {
      log.info("No masters running. Not attempting safe unload of tserver.");
      return;
    }
    final Instance instance = context.getInstance();
    final String zTServerRoot = getTServersZkPath(instance);
    final ZooCache zc = new ZooCacheFactory().getZooCache(instance.getZooKeepers(), instance.getZooKeepersSessionTimeOut());
    for (String server : servers) {
      for (int port : context.getConfiguration().getPort(Property.TSERV_CLIENTPORT)) {
        HostAndPort address = AddressUtil.parseAddress(server, port);
        final String finalServer = qualifyWithZooKeeperSessionId(zTServerRoot, zc, address.toString());
        log.info("Stopping server " + finalServer);
        MasterClient.executeVoid(context, new ClientExec<MasterClientService.Client>() {
          @Override
          public void execute(MasterClientService.Client client) throws Exception {
            client.shutdownTabletServer(Tracer.traceInfo(), context.rpcCreds(), finalServer, force);
          }
        });
      }
    }
  }

  /**
   * Get the parent ZNode for tservers for the given instance
   *
   * @param instance
   *          The Instance
   * @return The tservers znode for the instance
   */
  static String getTServersZkPath(Instance instance) {
    requireNonNull(instance);
    final String instanceRoot = ZooUtil.getRoot(instance);
    return instanceRoot + Constants.ZTSERVERS;
  }

  /**
   * Look up the TabletServers in ZooKeeper and try to find a sessionID for this server reference
   *
   * @param hostAndPort
   *          The host and port for a TabletServer
   * @return The host and port with the session ID in square-brackets appended, or the original value.
   */
  static String qualifyWithZooKeeperSessionId(String zTServerRoot, ZooCache zooCache, String hostAndPort) {
    try {
      long sessionId = ZooLock.getSessionId(zooCache, zTServerRoot + "/" + hostAndPort);
      if (0 == sessionId) {
        return hostAndPort;
      }
      return hostAndPort + "[" + Long.toHexString(sessionId) + "]";
    } catch (InterruptedException | KeeperException e) {
      log.warn("Failed to communicate with ZooKeeper to find session ID for TabletServer.");
      return hostAndPort;
    }
  }

  private static final String ACCUMULO_SITE_BACKUP_FILE = "accumulo-site.xml.bak";
  private static final String NS_FILE_SUFFIX = "_ns.cfg";
  private static final String USER_FILE_SUFFIX = "_user.cfg";
  private static final MessageFormat configFormat = new MessageFormat("config -t {0} -s {1}\n");
  private static final MessageFormat createNsFormat = new MessageFormat("createnamespace {0}\n");
  private static final MessageFormat createTableFormat = new MessageFormat("createtable {0}\n");
  private static final MessageFormat createUserFormat = new MessageFormat("createuser {0}\n");
  private static final MessageFormat nsConfigFormat = new MessageFormat("config -ns {0} -s {1}\n");
  private static final MessageFormat sysPermFormat = new MessageFormat("grant System.{0} -s -u {1}\n");
  private static final MessageFormat nsPermFormat = new MessageFormat("grant Namespace.{0} -ns {1} -u {2}\n");
  private static final MessageFormat tablePermFormat = new MessageFormat("grant Table.{0} -t {1} -u {2}\n");
  private static final MessageFormat userAuthsFormat = new MessageFormat("setauths -u {0} -s {1}\n");

  private DefaultConfiguration defaultConfig;
  private Map<String,String> siteConfig, systemConfig;
  private List<String> localUsers;

  public void printConfig(ClientContext context, DumpConfigCommand opts) throws Exception {

    File outputDirectory = null;
    if (opts.directory != null) {
      outputDirectory = new File(opts.directory);
      if (!outputDirectory.isDirectory()) {
        throw new IllegalArgumentException(opts.directory + " does not exist on the local filesystem.");
      }
      if (!outputDirectory.canWrite()) {
        throw new IllegalArgumentException(opts.directory + " is not writable");
      }
    }
    Connector connector = context.getConnector();
    defaultConfig = DefaultConfiguration.getInstance();
    siteConfig = connector.instanceOperations().getSiteConfiguration();
    systemConfig = connector.instanceOperations().getSystemConfiguration();
    if (opts.allConfiguration || opts.users) {
      localUsers = Lists.newArrayList(connector.securityOperations().listLocalUsers());
      Collections.sort(localUsers);
    }

    if (opts.allConfiguration) {
      // print accumulo site
      printSystemConfiguration(connector, outputDirectory);
      // print namespaces
      for (String namespace : connector.namespaceOperations().list()) {
        printNameSpaceConfiguration(connector, namespace, outputDirectory);
      }
      // print tables
      SortedSet<String> tableNames = connector.tableOperations().list();
      for (String tableName : tableNames) {
        printTableConfiguration(connector, tableName, outputDirectory);
      }
      // print users
      for (String user : localUsers) {
        printUserConfiguration(connector, user, outputDirectory);
      }
    } else {
      if (opts.systemConfiguration) {
        printSystemConfiguration(connector, outputDirectory);
      }
      if (opts.namespaceConfiguration) {
        for (String namespace : connector.namespaceOperations().list()) {
          printNameSpaceConfiguration(connector, namespace, outputDirectory);
        }
      }
      if (opts.tables.size() > 0) {
        for (String tableName : opts.tables) {
          printTableConfiguration(connector, tableName, outputDirectory);
        }
      }
      if (opts.users) {
        for (String user : localUsers) {
          printUserConfiguration(connector, user, outputDirectory);
        }
      }
    }
  }

  private String getDefaultConfigValue(String key) {
    if (null == key)
      return null;

    String defaultValue = null;
    try {
      Property p = Property.getPropertyByKey(key);
      if (null == p)
        return defaultValue;
      defaultValue = defaultConfig.get(p);
    } catch (IllegalArgumentException e) {
      // ignore
    }
    return defaultValue;
  }

  private void printNameSpaceConfiguration(Connector connector, String namespace, File outputDirectory) throws IOException, AccumuloException,
      AccumuloSecurityException, NamespaceNotFoundException {
    File namespaceScript = new File(outputDirectory, namespace + NS_FILE_SUFFIX);
    FileWriter nsWriter = new FileWriter(namespaceScript);
    nsWriter.write(createNsFormat.format(new String[] {namespace}));
    TreeMap<String,String> props = new TreeMap<>();
    for (Entry<String,String> p : connector.namespaceOperations().getProperties(namespace)) {
      props.put(p.getKey(), p.getValue());
    }
    for (Entry<String,String> entry : props.entrySet()) {
      String defaultValue = getDefaultConfigValue(entry.getKey());
      if (defaultValue == null || !defaultValue.equals(entry.getValue())) {
        if (!entry.getValue().equals(siteConfig.get(entry.getKey())) && !entry.getValue().equals(systemConfig.get(entry.getKey()))) {
          nsWriter.write(nsConfigFormat.format(new String[] {namespace, entry.getKey() + "=" + entry.getValue()}));
        }
      }
    }
    nsWriter.close();
  }

  private static void printUserConfiguration(Connector connector, String user, File outputDirectory) throws IOException, AccumuloException,
      AccumuloSecurityException {
    File userScript = new File(outputDirectory, user + USER_FILE_SUFFIX);
    FileWriter userWriter = new FileWriter(userScript);
    userWriter.write(createUserFormat.format(new String[] {user}));
    Authorizations auths = connector.securityOperations().getUserAuthorizations(user);
    userWriter.write(userAuthsFormat.format(new String[] {user, auths.toString()}));
    for (SystemPermission sp : SystemPermission.values()) {
      if (connector.securityOperations().hasSystemPermission(user, sp)) {
        userWriter.write(sysPermFormat.format(new String[] {sp.name(), user}));
      }
    }
    for (String namespace : connector.namespaceOperations().list()) {
      for (NamespacePermission np : NamespacePermission.values()) {
        if (connector.securityOperations().hasNamespacePermission(user, namespace, np)) {
          userWriter.write(nsPermFormat.format(new String[] {np.name(), namespace, user}));
        }
      }
    }
    for (String tableName : connector.tableOperations().list()) {
      for (TablePermission perm : TablePermission.values()) {
        if (connector.securityOperations().hasTablePermission(user, tableName, perm)) {
          userWriter.write(tablePermFormat.format(new String[] {perm.name(), tableName, user}));
        }
      }
    }

    userWriter.close();
  }

  private void printSystemConfiguration(Connector connector, File outputDirectory) throws IOException, AccumuloException, AccumuloSecurityException {
    Configuration conf = new Configuration(false);
    TreeMap<String,String> site = new TreeMap<>(siteConfig);
    for (Entry<String,String> prop : site.entrySet()) {
      String defaultValue = getDefaultConfigValue(prop.getKey());
      if (!prop.getValue().equals(defaultValue) && !systemConfig.containsKey(prop.getKey())) {
        conf.set(prop.getKey(), prop.getValue());
      }
    }
    TreeMap<String,String> system = new TreeMap<>(systemConfig);
    for (Entry<String,String> prop : system.entrySet()) {
      String defaultValue = getDefaultConfigValue(prop.getKey());
      if (!prop.getValue().equals(defaultValue)) {
        conf.set(prop.getKey(), prop.getValue());
      }
    }
    File siteBackup = new File(outputDirectory, ACCUMULO_SITE_BACKUP_FILE);
    FileOutputStream fos = new FileOutputStream(siteBackup);
    try {
      conf.writeXml(fos);
    } finally {
      fos.close();
    }
  }

  private void printTableConfiguration(Connector connector, String tableName, File outputDirectory) throws AccumuloException, TableNotFoundException,
      IOException, AccumuloSecurityException {
    File tableBackup = new File(outputDirectory, tableName + ".cfg");
    FileWriter writer = new FileWriter(tableBackup);
    writer.write(createTableFormat.format(new String[] {tableName}));
    TreeMap<String,String> props = new TreeMap<>();
    for (Entry<String,String> p : connector.tableOperations().getProperties(tableName)) {
      props.put(p.getKey(), p.getValue());
    }
    for (Entry<String,String> prop : props.entrySet()) {
      if (prop.getKey().startsWith(Property.TABLE_PREFIX.getKey())) {
        String defaultValue = getDefaultConfigValue(prop.getKey());
        if (defaultValue == null || !defaultValue.equals(prop.getValue())) {
          if (!prop.getValue().equals(siteConfig.get(prop.getKey())) && !prop.getValue().equals(systemConfig.get(prop.getKey()))) {
            writer.write(configFormat.format(new String[] {tableName, prop.getKey() + "=" + prop.getValue()}));
          }
        }
      }
    }
    writer.close();
  }
}
