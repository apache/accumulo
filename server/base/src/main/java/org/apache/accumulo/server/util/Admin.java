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
package org.apache.accumulo.server.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.apache.accumulo.core.fate.FateTxId.parseTidFromUserInput;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.AdminUtil;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.fate.ReadOnlyTStore;
import org.apache.accumulo.core.fate.ZooStore;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.manager.thrift.FateService;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.singletons.SingletonManager;
import org.apache.accumulo.core.singletons.SingletonManager.Mode;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.tables.TableMap;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.accumulo.server.util.fateCommand.FateSummaryReport;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@AutoService(KeywordExecutable.class)
public class Admin implements KeywordExecutable {
  private static final Logger log = LoggerFactory.getLogger(Admin.class);

  static class AdminOpts extends ServerUtilOpts {
    @Parameter(names = {"-f", "--force"},
        description = "force the given server to stop by removing its lock")
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

    @Parameter(names = {"-t", "--table"},
        description = "Table to check, if not set checks all tables")
    String tableName = null;
  }

  @Parameters(commandDescription = "stop the manager")
  static class StopManagerCommand {}

  @Deprecated(since = "2.1.0")
  @Parameters(commandDescription = "stop the master (DEPRECATED -- use stopManager instead)")
  static class StopMasterCommand {}

  @Parameters(commandDescription = "stop all tablet servers and the manager")
  static class StopAllCommand {}

  @Parameters(commandDescription = "list Accumulo instances in zookeeper")
  static class ListInstancesCommand {
    @Parameter(names = "--print-errors", description = "display errors while listing instances")
    boolean printErrors = false;
    @Parameter(names = "--print-all",
        description = "print information for all instances, not just those with names")
    boolean printAll = false;
  }

  @Parameters(commandDescription = "Accumulo volume utility")
  static class VolumesCommand {
    @Parameter(names = {"-l", "--list"}, description = "list volumes currently in use")
    boolean printErrors = false;
  }

  @Parameters(commandDescription = "print out non-default configuration settings")
  static class DumpConfigCommand {
    @Parameter(names = {"-a", "--all"},
        description = "print the system and all table configurations")
    boolean allConfiguration = false;
    @Parameter(names = {"-d", "--directory"}, description = "directory to place config files")
    String directory = null;
    @Parameter(names = {"-s", "--system"}, description = "print the system configuration")
    boolean systemConfiguration = false;
    @Parameter(names = {"-n", "--namespaces"}, description = "print the namespace configuration")
    boolean namespaceConfiguration = false;
    @Parameter(names = {"-t", "--tables"}, description = "print per-table configuration")
    List<String> tables = new ArrayList<>();
    @Parameter(names = {"-u", "--users"},
        description = "print users and their authorizations and permissions")
    boolean users = false;
  }

  private static final String RV_DEPRECATION_MSG =
      "Randomizing tablet directories is deprecated and now does nothing. Accumulo now always"
          + " calls the volume chooser for each file created by a tablet, so its no longer "
          + "necessary.";

  @Parameters(commandDescription = RV_DEPRECATION_MSG)
  static class RandomizeVolumesCommand {
    @Parameter(names = {"-t"}, description = "table to update", required = true)
    String tableName = null;
  }

  @Parameters(commandDescription = "Verify all Tablets are assigned to tablet servers")
  static class VerifyTabletAssignmentsCommand {
    @Parameter(names = {"-v", "--verbose"},
        description = "verbose mode (prints locations of tablets)")
    boolean verbose = false;
  }

  /**
   * @since 2.1.0
   */
  @Parameters(
      commandDescription = "Changes the unique secret given to the instance that all servers must know.")
  static class ChangeSecretCommand {}

  @Parameters(
      commandDescription = "List or delete Tablet Server locks. Default with no arguments is to list the locks.")
  static class TabletServerLocksCommand {
    @Parameter(names = "-delete", description = "specify a tablet server lock to delete")
    String delete = null;
  }

  @Parameters(
      commandDescription = "Deletes specific instance name or id from zookeeper or cleans up all old instances.")
  static class DeleteZooInstanceCommand {
    @Parameter(names = {"-i", "--instance"}, description = "the instance name or id to delete")
    String instance;
    @Parameter(names = {"-c", "--clean"},
        description = "Cleans Zookeeper by deleting all old instances. This will not delete the instance pointed to by the local accumulo.properties file")
    boolean clean = false;
    @Parameter(names = {"--password"},
        description = "The system secret, if different than instance.secret in accumulo.properties",
        password = true)
    String auth;
  }

  @Parameters(commandDescription = "Restore Zookeeper data from a file.")
  static class RestoreZooCommand {
    @Parameter(names = "--overwrite")
    boolean overwrite = false;

    @Parameter(names = "--file")
    String file;
  }

  @Parameters(commandNames = "fate",
      commandDescription = "Operations performed on the Manager FaTE system.")
  static class FateOpsCommand {
    @Parameter(description = "[<txId>...]")
    List<String> txList = new ArrayList<>();

    @Parameter(names = {"-c", "--cancel"},
        description = "<txId>... Cancel new or submitted FaTE transactions")
    boolean cancel;

    @Parameter(names = {"-f", "--fail"},
        description = "<txId>... Transition FaTE transaction status to FAILED_IN_PROGRESS (requires Manager to be down)")
    boolean fail;

    @Parameter(names = {"-d", "--delete"},
        description = "<txId>... Delete locks associated with transactions (Requires Manager to be down)")
    boolean delete;

    @Parameter(names = {"-p", "--print", "-print", "-l", "--list", "-list"},
        description = "[<txId>...] Print information about FaTE transactions. Print only the 'txId's specified or print all transactions if empty. Use -s to only print certain states.")
    boolean print;

    @Parameter(names = "--summary", description = "Print a summary of all FaTE transactions")
    boolean summarize;

    @Parameter(names = {"-j", "--json"}, description = "Print transactions in json")
    boolean printJson;

    @Parameter(names = {"-s", "--state"},
        description = "<state>... Print transactions in the state(s) {NEW, IN_PROGRESS, FAILED_IN_PROGRESS, FAILED, SUCCESSFUL}")
    List<String> states = new ArrayList<>();
  }

  public static void main(String[] args) {
    new Admin().execute(args);
  }

  @Override
  public String keyword() {
    return "admin";
  }

  @Override
  public UsageGroup usageGroup() {
    return UsageGroup.CORE;
  }

  @Override
  public String description() {
    return "Executes administrative commands";
  }

  @SuppressFBWarnings(value = "DM_EXIT", justification = "System.exit okay for CLI tool")
  @Override
  public void execute(final String[] args) {
    boolean everything;

    AdminOpts opts = new AdminOpts();
    JCommander cl = new JCommander(opts);
    cl.setProgramName("accumulo admin");

    ChangeSecretCommand changeSecretCommand = new ChangeSecretCommand();
    cl.addCommand("changeSecret", changeSecretCommand);

    CheckTabletsCommand checkTabletsCommand = new CheckTabletsCommand();
    cl.addCommand("checkTablets", checkTabletsCommand);

    DeleteZooInstanceCommand deleteZooInstOpts = new DeleteZooInstanceCommand();
    cl.addCommand("deleteZooInstance", deleteZooInstOpts);

    DumpConfigCommand dumpConfigCommand = new DumpConfigCommand();
    cl.addCommand("dumpConfig", dumpConfigCommand);

    FateOpsCommand fateOpsCommand = new FateOpsCommand();
    cl.addCommand("fate", fateOpsCommand);

    ListInstancesCommand listInstancesOpts = new ListInstancesCommand();
    cl.addCommand("listInstances", listInstancesOpts);

    TabletServerLocksCommand tServerLocksOpts = new TabletServerLocksCommand();
    cl.addCommand("locks", tServerLocksOpts);

    PingCommand pingCommand = new PingCommand();
    cl.addCommand("ping", pingCommand);

    RestoreZooCommand restoreZooOpts = new RestoreZooCommand();
    cl.addCommand("restoreZoo", restoreZooOpts);

    RandomizeVolumesCommand randomizeVolumesOpts = new RandomizeVolumesCommand();
    cl.addCommand("randomizeVolumes", randomizeVolumesOpts);

    StopCommand stopOpts = new StopCommand();
    cl.addCommand("stop", stopOpts);

    StopAllCommand stopAllOpts = new StopAllCommand();
    cl.addCommand("stopAll", stopAllOpts);

    StopManagerCommand stopManagerOpts = new StopManagerCommand();
    cl.addCommand("stopManager", stopManagerOpts);

    StopMasterCommand stopMasterOpts = new StopMasterCommand();
    cl.addCommand("stopMaster", stopMasterOpts);

    VerifyTabletAssignmentsCommand verifyTabletAssignmentsOpts =
        new VerifyTabletAssignmentsCommand();
    cl.addCommand("verifyTabletAssigns", verifyTabletAssignmentsOpts);

    VolumesCommand volumesCommand = new VolumesCommand();
    cl.addCommand("volumes", volumesCommand);

    cl.parse(args);

    if (opts.help || cl.getParsedCommand() == null) {
      cl.usage();
      return;
    }

    ServerContext context = opts.getServerContext();

    AccumuloConfiguration conf = context.getConfiguration();
    // Login as the server on secure HDFS
    if (conf.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
      SecurityUtil.serverLogin(conf);
    }

    try {

      int rc = 0;

      if (cl.getParsedCommand().equals("listInstances")) {
        ListInstances.listInstances(context.getZooKeepers(), listInstancesOpts.printAll,
            listInstancesOpts.printErrors);
      } else if (cl.getParsedCommand().equals("ping")) {
        if (ping(context, pingCommand.args) != 0) {
          rc = 4;
        }
      } else if (cl.getParsedCommand().equals("checkTablets")) {
        System.out.println("\n*** Looking for offline tablets ***\n");
        if (FindOfflineTablets.findOffline(context, checkTabletsCommand.tableName) != 0) {
          rc = 5;
        }
        System.out.println("\n*** Looking for missing files ***\n");
        if (checkTabletsCommand.tableName == null) {
          if (RemoveEntriesForMissingFiles.checkAllTables(context, checkTabletsCommand.fixFiles)
              != 0) {
            rc = 6;
          }
        } else {
          if (RemoveEntriesForMissingFiles.checkTable(context, checkTabletsCommand.tableName,
              checkTabletsCommand.fixFiles) != 0) {
            rc = 6;
          }
        }

      } else if (cl.getParsedCommand().equals("stop")) {
        stopTabletServer(context, stopOpts.args, opts.force);
      } else if (cl.getParsedCommand().equals("dumpConfig")) {
        printConfig(context, dumpConfigCommand);
      } else if (cl.getParsedCommand().equals("volumes")) {
        ListVolumesUsed.listVolumes(context);
      } else if (cl.getParsedCommand().equals("randomizeVolumes")) {
        System.out.println(RV_DEPRECATION_MSG);
      } else if (cl.getParsedCommand().equals("verifyTabletAssigns")) {
        VerifyTabletAssignments.execute(opts.getClientProps(), verifyTabletAssignmentsOpts.verbose);
      } else if (cl.getParsedCommand().equals("changeSecret")) {
        ChangeSecret.execute(context, conf);
      } else if (cl.getParsedCommand().equals("deleteZooInstance")) {
        DeleteZooInstance.execute(context, deleteZooInstOpts.clean, deleteZooInstOpts.instance,
            deleteZooInstOpts.auth);
      } else if (cl.getParsedCommand().equals("restoreZoo")) {
        RestoreZookeeper.execute(conf, restoreZooOpts.file, restoreZooOpts.overwrite);
      } else if (cl.getParsedCommand().equals("locks")) {
        TabletServerLocks.execute(context, args.length > 2 ? args[2] : null,
            tServerLocksOpts.delete);
      } else if (cl.getParsedCommand().equals("fate")) {
        executeFateOpsCommand(context, fateOpsCommand);
      } else {
        everything = cl.getParsedCommand().equals("stopAll");

        if (everything) {
          flushAll(context);
        }

        stopServer(context, everything);
      }

      if (rc != 0) {
        System.exit(rc);
      }
    } catch (AccumuloException e) {
      log.error("{}", e.getMessage(), e);
      System.exit(1);
    } catch (AccumuloSecurityException e) {
      log.error("{}", e.getMessage(), e);
      System.exit(2);
    } catch (Exception e) {
      log.error("{}", e.getMessage(), e);
      System.exit(3);
    } finally {
      SingletonManager.setMode(Mode.CLOSED);
    }
  }

  private static int ping(ClientContext context, List<String> args) {

    InstanceOperations io = context.instanceOperations();

    if (args.isEmpty()) {
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
   * Flushing during shutdown is a performance optimization, it's not required. This method will
   * attempt to initiate flushes of all tables and give up if it takes too long.
   */
  private static void flushAll(final ClientContext context) {

    final AtomicInteger flushesStarted = new AtomicInteger(0);

    Runnable flushTask = () -> {
      try {
        Set<String> tables = context.tableOperations().tableIdMap().keySet();
        for (String table : tables) {
          if (table.equals(MetadataTable.NAME)) {
            continue;
          }
          try {
            context.tableOperations().flush(table, null, null, false);
            flushesStarted.incrementAndGet();
          } catch (TableNotFoundException e) {
            // ignore
          }
        }
      } catch (Exception e) {
        log.warn("Failed to initiate flush {}", e.getMessage());
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

  private static void stopServer(final ClientContext context, final boolean tabletServersToo)
      throws AccumuloException, AccumuloSecurityException {

    ThriftClientTypes.MANAGER.executeVoid(context,
        client -> client.shutdown(TraceUtil.traceInfo(), context.rpcCreds(), tabletServersToo));
  }

  private static void stopTabletServer(final ClientContext context, List<String> servers,
      final boolean force) throws AccumuloException, AccumuloSecurityException {
    if (context.getManagerLocations().isEmpty()) {
      log.info("No managers running. Not attempting safe unload of tserver.");
      return;
    }
    if (servers.isEmpty()) {
      log.error("No tablet servers provided.");
      return;
    }

    final String zTServerRoot = getTServersZkPath(context);
    final ZooCache zc = context.getZooCache();
    List<String> runningServers;

    for (String server : servers) {
      runningServers = context.instanceOperations().getTabletServers();
      if (runningServers.size() == 1 && !force) {
        log.info("Only 1 tablet server running. Not attempting shutdown of {}", server);
        return;
      }
      for (int port : context.getConfiguration().getPort(Property.TSERV_CLIENTPORT)) {
        HostAndPort address = AddressUtil.parseAddress(server, port);
        final String finalServer =
            qualifyWithZooKeeperSessionId(zTServerRoot, zc, address.toString());
        log.info("Stopping server {}", finalServer);
        ThriftClientTypes.MANAGER.executeVoid(context, client -> client
            .shutdownTabletServer(TraceUtil.traceInfo(), context.rpcCreds(), finalServer, force));
      }
    }
  }

  /**
   * Get the parent ZNode for tservers for the given instance
   *
   * @param context ClientContext
   * @return The tservers znode for the instance
   */
  static String getTServersZkPath(ClientContext context) {
    requireNonNull(context);
    return context.getZooKeeperRoot() + Constants.ZTSERVERS;
  }

  /**
   * Look up the TabletServers in ZooKeeper and try to find a sessionID for this server reference
   *
   * @param hostAndPort The host and port for a TabletServer
   * @return The host and port with the session ID in square-brackets appended, or the original
   *         value.
   */
  static String qualifyWithZooKeeperSessionId(String zTServerRoot, ZooCache zooCache,
      String hostAndPort) {
    var zLockPath = ServiceLock.path(zTServerRoot + "/" + hostAndPort);
    long sessionId = ServiceLock.getSessionId(zooCache, zLockPath);
    if (sessionId == 0) {
      return hostAndPort;
    }
    return hostAndPort + "[" + Long.toHexString(sessionId) + "]";
  }

  private static final String ACCUMULO_SITE_BACKUP_FILE = "accumulo.properties.bak";
  private static final String NS_FILE_SUFFIX = "_ns.cfg";
  private static final String USER_FILE_SUFFIX = "_user.cfg";
  private static final MessageFormat configFormat = new MessageFormat("config -t {0} -s {1}\n");
  private static final MessageFormat createNsFormat = new MessageFormat("createnamespace {0}\n");
  private static final MessageFormat createTableFormat = new MessageFormat("createtable {0}\n");
  private static final MessageFormat createUserFormat = new MessageFormat("createuser {0}\n");
  private static final MessageFormat nsConfigFormat = new MessageFormat("config -ns {0} -s {1}\n");
  private static final MessageFormat sysPermFormat =
      new MessageFormat("grant System.{0} -s -u {1}\n");
  private static final MessageFormat nsPermFormat =
      new MessageFormat("grant Namespace.{0} -ns {1} -u {2}\n");
  private static final MessageFormat tablePermFormat =
      new MessageFormat("grant Table.{0} -t {1} -u {2}\n");
  private static final MessageFormat userAuthsFormat =
      new MessageFormat("setauths -u {0} -s {1}\n");

  private DefaultConfiguration defaultConfig;
  private Map<String,String> siteConfig, systemConfig;
  private List<String> localUsers;

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "code runs in same security context as user who provided input")
  public void printConfig(ClientContext context, DumpConfigCommand opts) throws Exception {

    File outputDirectory = null;
    if (opts.directory != null) {
      outputDirectory = new File(opts.directory);
      if (!outputDirectory.isDirectory()) {
        throw new IllegalArgumentException(
            opts.directory + " does not exist on the local filesystem.");
      }
      if (!outputDirectory.canWrite()) {
        throw new IllegalArgumentException(opts.directory + " is not writable");
      }
    }
    defaultConfig = DefaultConfiguration.getInstance();
    siteConfig = context.instanceOperations().getSiteConfiguration();
    systemConfig = context.instanceOperations().getSystemConfiguration();
    if (opts.allConfiguration || opts.users) {
      localUsers = Lists.newArrayList(context.securityOperations().listLocalUsers());
      Collections.sort(localUsers);
    }

    if (opts.allConfiguration) {
      // print accumulo site
      printSystemConfiguration(outputDirectory);
      // print namespaces
      for (String namespace : context.namespaceOperations().list()) {
        printNameSpaceConfiguration(context, namespace, outputDirectory);
      }
      // print tables
      SortedSet<String> tableNames = context.tableOperations().list();
      for (String tableName : tableNames) {
        printTableConfiguration(context, tableName, outputDirectory);
      }
      // print users
      for (String user : localUsers) {
        printUserConfiguration(context, user, outputDirectory);
      }
    } else {
      if (opts.systemConfiguration) {
        printSystemConfiguration(outputDirectory);
      }
      if (opts.namespaceConfiguration) {
        for (String namespace : context.namespaceOperations().list()) {
          printNameSpaceConfiguration(context, namespace, outputDirectory);
        }
      }
      if (!opts.tables.isEmpty()) {
        for (String tableName : opts.tables) {
          printTableConfiguration(context, tableName, outputDirectory);
        }
      }
      if (opts.users) {
        for (String user : localUsers) {
          printUserConfiguration(context, user, outputDirectory);
        }
      }
    }
  }

  private String getDefaultConfigValue(String key) {
    if (key == null) {
      return null;
    }

    String defaultValue = null;
    try {
      Property p = Property.getPropertyByKey(key);
      if (p == null) {
        return defaultValue;
      }
      defaultValue = defaultConfig.get(p);
    } catch (IllegalArgumentException e) {
      // ignore
    }
    return defaultValue;
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "code runs in same security context as user who provided input")
  private void printNameSpaceConfiguration(AccumuloClient accumuloClient, String namespace,
      File outputDirectory)
      throws IOException, AccumuloException, AccumuloSecurityException, NamespaceNotFoundException {
    File namespaceScript = new File(outputDirectory, namespace + NS_FILE_SUFFIX);
    try (BufferedWriter nsWriter = new BufferedWriter(new FileWriter(namespaceScript, UTF_8))) {
      nsWriter.write(createNsFormat.format(new String[] {namespace}));
      Map<String,String> props = ImmutableSortedMap
          .copyOf(accumuloClient.namespaceOperations().getConfiguration(namespace));
      for (Entry<String,String> entry : props.entrySet()) {
        String defaultValue = getDefaultConfigValue(entry.getKey());
        if (defaultValue == null || !defaultValue.equals(entry.getValue())) {
          if (!entry.getValue().equals(siteConfig.get(entry.getKey()))
              && !entry.getValue().equals(systemConfig.get(entry.getKey()))) {
            nsWriter.write(nsConfigFormat
                .format(new String[] {namespace, entry.getKey() + "=" + entry.getValue()}));
          }
        }
      }
    }
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "code runs in same security context as user who provided input")
  private static void printUserConfiguration(AccumuloClient accumuloClient, String user,
      File outputDirectory) throws IOException, AccumuloException, AccumuloSecurityException {
    File userScript = new File(outputDirectory, user + USER_FILE_SUFFIX);
    try (BufferedWriter userWriter = new BufferedWriter(new FileWriter(userScript, UTF_8))) {
      userWriter.write(createUserFormat.format(new String[] {user}));
      Authorizations auths = accumuloClient.securityOperations().getUserAuthorizations(user);
      userWriter.write(userAuthsFormat.format(new String[] {user, auths.toString()}));
      for (SystemPermission sp : SystemPermission.values()) {
        if (accumuloClient.securityOperations().hasSystemPermission(user, sp)) {
          userWriter.write(sysPermFormat.format(new String[] {sp.name(), user}));
        }
      }
      for (String namespace : accumuloClient.namespaceOperations().list()) {
        for (NamespacePermission np : NamespacePermission.values()) {
          if (accumuloClient.securityOperations().hasNamespacePermission(user, namespace, np)) {
            userWriter.write(nsPermFormat.format(new String[] {np.name(), namespace, user}));
          }
        }
      }
      for (String tableName : accumuloClient.tableOperations().list()) {
        for (TablePermission perm : TablePermission.values()) {
          if (accumuloClient.securityOperations().hasTablePermission(user, tableName, perm)) {
            userWriter.write(tablePermFormat.format(new String[] {perm.name(), tableName, user}));
          }
        }
      }
    }
  }

  private void printSystemConfiguration(File outputDirectory) throws IOException {
    TreeMap<String,String> conf = new TreeMap<>();
    TreeMap<String,String> site = new TreeMap<>(siteConfig);
    for (Entry<String,String> prop : site.entrySet()) {
      String defaultValue = getDefaultConfigValue(prop.getKey());
      if (!prop.getValue().equals(defaultValue) && !systemConfig.containsKey(prop.getKey())) {
        conf.put(prop.getKey(), prop.getValue());
      }
    }
    TreeMap<String,String> system = new TreeMap<>(systemConfig);
    for (Entry<String,String> prop : system.entrySet()) {
      String defaultValue = getDefaultConfigValue(prop.getKey());
      if (!prop.getValue().equals(defaultValue)) {
        conf.put(prop.getKey(), prop.getValue());
      }
    }
    File siteBackup = new File(outputDirectory, ACCUMULO_SITE_BACKUP_FILE);
    try (BufferedWriter fw = new BufferedWriter(new FileWriter(siteBackup, UTF_8))) {
      for (Entry<String,String> prop : conf.entrySet()) {
        fw.write(prop.getKey() + "=" + prop.getValue() + "\n");
      }
    }
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "code runs in same security context as user who provided input")
  private void printTableConfiguration(AccumuloClient accumuloClient, String tableName,
      File outputDirectory)
      throws AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    File tableBackup = new File(outputDirectory, tableName + ".cfg");
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(tableBackup, UTF_8))) {
      writer.write(createTableFormat.format(new String[] {tableName}));
      Map<String,String> props =
          ImmutableSortedMap.copyOf(accumuloClient.tableOperations().getConfiguration(tableName));
      for (Entry<String,String> prop : props.entrySet()) {
        if (prop.getKey().startsWith(Property.TABLE_PREFIX.getKey())) {
          String defaultValue = getDefaultConfigValue(prop.getKey());
          if (defaultValue == null || !defaultValue.equals(prop.getValue())) {
            if (!prop.getValue().equals(siteConfig.get(prop.getKey()))
                && !prop.getValue().equals(systemConfig.get(prop.getKey()))) {
              writer.write(configFormat
                  .format(new String[] {tableName, prop.getKey() + "=" + prop.getValue()}));
            }
          }
        }
      }
    }
  }

  // Fate Operations
  private void executeFateOpsCommand(ServerContext context, FateOpsCommand fateOpsCommand)
      throws AccumuloException, AccumuloSecurityException, InterruptedException, KeeperException {

    validateFateUserInput(fateOpsCommand);

    AdminUtil<Admin> admin = new AdminUtil<>(true);
    final String zkRoot = context.getZooKeeperRoot();
    var zLockManagerPath = ServiceLock.path(zkRoot + Constants.ZMANAGER_LOCK);
    var zTableLocksPath = ServiceLock.path(zkRoot + Constants.ZTABLE_LOCKS);
    String fateZkPath = zkRoot + Constants.ZFATE;
    ZooReaderWriter zk = context.getZooReaderWriter();
    ZooStore<Admin> zs = new ZooStore<>(fateZkPath, zk);

    if (fateOpsCommand.cancel) {
      cancelSubmittedFateTxs(context, fateOpsCommand.txList);
    } else if (fateOpsCommand.fail) {
      for (String txid : fateOpsCommand.txList) {
        if (!admin.prepFail(zs, zk, zLockManagerPath, txid)) {
          throw new AccumuloException("Could not fail transaction: " + txid);
        }
      }
    } else if (fateOpsCommand.delete) {
      for (String txid : fateOpsCommand.txList) {
        if (!admin.prepDelete(zs, zk, zLockManagerPath, txid)) {
          throw new AccumuloException("Could not delete transaction: " + txid);
        }
        admin.deleteLocks(zk, zTableLocksPath, txid);
      }
    }

    if (fateOpsCommand.print) {
      final Set<Long> sortedTxs = new TreeSet<>();
      fateOpsCommand.txList.forEach(s -> sortedTxs.add(parseTidFromUserInput(s)));
      EnumSet<ReadOnlyTStore.TStatus> statusFilter = getCmdLineStatusFilters(fateOpsCommand.states);
      admin.print(zs, zk, zTableLocksPath, new Formatter(System.out), sortedTxs, statusFilter);
      // print line break at the end
      System.out.println();
    }

    if (fateOpsCommand.summarize) {
      summarizeFateTx(context, fateOpsCommand, admin, zs, zTableLocksPath);
    }
  }

  private void validateFateUserInput(FateOpsCommand cmd) {
    if (cmd.cancel && cmd.fail || cmd.cancel && cmd.delete || cmd.fail && cmd.delete) {
      throw new IllegalArgumentException(
          "Can only perform one of the following at a time: cancel, fail or delete.");
    }
    if ((cmd.cancel || cmd.fail || cmd.delete) && cmd.txList.isEmpty()) {
      throw new IllegalArgumentException(
          "At least one txId required when using cancel, fail or delete");
    }
  }

  private void cancelSubmittedFateTxs(ServerContext context, List<String> txList)
      throws AccumuloException {
    for (String txStr : txList) {
      long txid = Long.parseLong(txStr, 16);
      boolean cancelled = cancelFateOperation(context, txid);
      if (cancelled) {
        System.out.println("FaTE transaction " + FateTxId.formatTid(txid)
            + " was cancelled or already completed.");
      } else {
        System.out.println("FaTE transaction " + FateTxId.formatTid(txid)
            + " was not cancelled, status may have changed.");
      }
    }
  }

  private boolean cancelFateOperation(ClientContext context, long txid) throws AccumuloException {
    FateService.Client client = null;
    try {
      client = ThriftClientTypes.FATE.getConnectionWithRetry(context);
      return client.cancelFateOperation(TraceUtil.traceInfo(), context.rpcCreds(), txid);
    } catch (Exception e) {
      throw new AccumuloException(e);
    } finally {
      if (client != null) {
        ThriftUtil.close(client, context);
      }
    }
  }

  private void summarizeFateTx(ServerContext context, FateOpsCommand cmd, AdminUtil<Admin> admin,
      ReadOnlyTStore<Admin> zs, ServiceLock.ServiceLockPath tableLocksPath)
      throws InterruptedException, AccumuloException, AccumuloSecurityException, KeeperException {

    ZooReaderWriter zk = context.getZooReaderWriter();
    var transactions = admin.getStatus(zs, zk, tableLocksPath, null, null);

    // build id map - relies on unique ids for tables and namespaces
    // used to look up the names of either table or namespace by id.
    Map<TableId,String> tidToNameMap = new TableMap(context).getIdtoNameMap();
    Map<String,String> idsToNameMap = new HashMap<>(tidToNameMap.size() * 2);
    tidToNameMap.forEach((tid, name) -> idsToNameMap.put(tid.canonical(), "t:" + name));
    context.namespaceOperations().namespaceIdMap().forEach((name, nsid) -> {
      String prev = idsToNameMap.put(nsid, "ns:" + name);
      if (prev != null) {
        log.warn("duplicate id found for table / namespace id. table name: {}, namespace name: {}",
            prev, name);
      }
    });

    EnumSet<ReadOnlyTStore.TStatus> statusFilter = getCmdLineStatusFilters(cmd.states);

    FateSummaryReport report = new FateSummaryReport(idsToNameMap, statusFilter);

    // gather statistics
    transactions.getTransactions().forEach(report::gatherTxnStatus);
    if (cmd.printJson) {
      printLines(Collections.singletonList(report.toJson()));
    } else {
      printLines(report.formatLines());
    }
  }

  private void printLines(List<String> lines) {
    for (String nextLine : lines) {
      if (nextLine == null) {
        continue;
      }
      System.out.println(nextLine);
    }
  }

  /**
   * If provided on the command line, get the TStatus values provided.
   *
   * @return a set of status filters, or an empty set if none provides
   */
  private EnumSet<ReadOnlyTStore.TStatus> getCmdLineStatusFilters(List<String> states) {
    EnumSet<ReadOnlyTStore.TStatus> statusFilter = null;
    if (!states.isEmpty()) {
      statusFilter = EnumSet.noneOf(ReadOnlyTStore.TStatus.class);
      for (String element : states) {
        statusFilter.add(ReadOnlyTStore.TStatus.valueOf(element));
      }
    }
    return statusFilter;
  }
}
