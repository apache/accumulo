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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.impl.ClientExec;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.server.cli.ClientOpts;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.trace.instrument.Tracer;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;

public class Admin {
  private static final Logger log = Logger.getLogger(Admin.class);

  static class AdminOpts extends ClientOpts {
    @Parameter(names = {"-f", "--force"}, description = "force the given server to stop by removing its lock")
    boolean force = false;
  }

  @Parameters(commandDescription = "stop the tablet server on the given hosts")
  static class StopCommand {
    @Parameter(description = "<host> {<host> ... }")
    List<String> args = new ArrayList<String>();
  }

  @Parameters(commandDescription = "Ping tablet servers.  If no arguments, pings all.")
  static class PingCommand {
    @Parameter(description = "{<host> ... }")
    List<String> args = new ArrayList<String>();
  }

  @Parameters(commandDescription = "print tablets that are offline in online tables")
  static class CheckTabletsCommand {
    @Parameter(names = "--fixFiles", description = "Remove dangling file pointers")
    boolean fixFiles = false;

    @Parameter(names = {"-t", "--table"}, description = "Table to check, if not set checks all tables")
    String table = null;
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
    @Parameter(names = {"-t", "--tables"}, description = "print per-table configuration")
    List<String> tables = new ArrayList<String>();
    @Parameter(names = {"-a", "--all"}, description = "print the system and all table configurations")
    boolean allConfiguration = false;
    @Parameter(names = {"-s", "--system"}, description = "print the system configuration")
    boolean systemConfiguration = false;
    @Parameter(names = {"-p", "--permissions"}, description = "print user permissions (must be used in combination with -a, -s, or -t)")
    boolean userPermissions = false;
    @Parameter(names = {"-d", "--directory"}, description = "directory to place config files")
    String directory = null;
  }
  
  @Parameters(commandDescription = "redistribute tablet directories across the current volume list")
  static class RandomizeVolumesCommand {
    @Parameter(names={"-t"}, description = "table to update", required=true)
    String table = null;
  }

  public static void main(String[] args) {
    boolean everything;

    AdminOpts opts = new AdminOpts();
    JCommander cl = new JCommander(opts);
    cl.setProgramName(Admin.class.getName());

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
    Instance instance = opts.getInstance();
    AccumuloConfiguration conf = new ServerConfigurationFactory(instance).getConfiguration();

    try {
      String principal;
      AuthenticationToken token;
      if (opts.getToken() == null) {
        principal = SystemCredentials.get().getPrincipal();
        token = SystemCredentials.get().getToken();
      } else {
        principal = opts.principal;
        token = opts.getToken();
      }

      int rc = 0;

      if (cl.getParsedCommand().equals("listInstances")) {
        ListInstances.listInstances(instance.getZooKeepers(), listIntancesOpts.printAll, listIntancesOpts.printErrors);
      } else if (cl.getParsedCommand().equals("ping")) {
        if (ping(instance, principal, token, pingCommand.args) != 0)
          rc = 4;
      } else if (cl.getParsedCommand().equals("checkTablets")) {
        System.out.println("\n*** Looking for offline tablets ***\n");
        if (FindOfflineTablets.findOffline(instance, new Credentials(principal, token), checkTabletsCommand.table) != 0)
          rc = 5;
        System.out.println("\n*** Looking for missing files ***\n");
        if (checkTabletsCommand.table == null) {
          if (RemoveEntriesForMissingFiles.checkAllTables(instance, principal, token, checkTabletsCommand.fixFiles) != 0)
            rc = 6;
        } else {
          if (RemoveEntriesForMissingFiles.checkTable(instance, principal, token, checkTabletsCommand.table, checkTabletsCommand.fixFiles) != 0)
            rc = 6;
        }

      } else if (cl.getParsedCommand().equals("stop")) {
        stopTabletServer(conf, instance, new Credentials(principal, token), stopOpts.args, opts.force);
      } else if (cl.getParsedCommand().equals("dumpConfig")) {
        printConfig(instance, principal, token, dumpConfigCommand);
      } else if (cl.getParsedCommand().equals("volumes")) {
        ListVolumesUsed.listVolumes(instance, principal, token);
      } else if (cl.getParsedCommand().equals("randomizeVolumes")) {
        RandomizeVolumes.randomize(instance, new Credentials(principal, token), randomizeVolumesOpts.table);
      } else {
        everything = cl.getParsedCommand().equals("stopAll");

        if (everything)
          flushAll(instance, principal, token);

        stopServer(instance, new Credentials(principal, token), everything);
      }

      if (rc != 0)
        System.exit(rc);
    } catch (AccumuloException e) {
      log.error(e, e);
      System.exit(1);
    } catch (AccumuloSecurityException e) {
      log.error(e, e);
      System.exit(2);
    } catch (Exception e) {
      log.error(e, e);
      System.exit(3);
    }
  }

  private static int ping(Instance instance, String principal, AuthenticationToken token, List<String> args) throws AccumuloException,
      AccumuloSecurityException {

    InstanceOperations io = instance.getConnector(principal, token).instanceOperations();

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
  private static void flushAll(final Instance instance, final String principal, final AuthenticationToken token) throws AccumuloException,
      AccumuloSecurityException {

    final AtomicInteger flushesStarted = new AtomicInteger(0);

    Runnable flushTask = new Runnable() {

      @Override
      public void run() {
        try {
          Connector conn = instance.getConnector(principal, token);
          Set<String> tables = conn.tableOperations().tableIdMap().keySet();
          for (String table : tables) {
            if (table.equals(MetadataTable.NAME))
              continue;
            try {
              conn.tableOperations().flush(table, null, null, false);
              flushesStarted.incrementAndGet();
            } catch (TableNotFoundException e) {}
          }
        } catch (Exception e) {
          log.warn("Failed to intiate flush " + e.getMessage());
        }
      }
    };

    Thread flusher = new Thread(flushTask);
    flusher.setDaemon(true);
    flusher.start();

    long start = System.currentTimeMillis();
    try {
      flusher.join(3000);
    } catch (InterruptedException e) {}

    while (flusher.isAlive() && System.currentTimeMillis() - start < 15000) {
      int flushCount = flushesStarted.get();
      try {
        flusher.join(1000);
      } catch (InterruptedException e) {}

      if (flushCount == flushesStarted.get()) {
        // no progress was made while waiting for join... maybe its stuck, stop waiting on it
        break;
      }
    }
  }

  private static void stopServer(final Instance instance, final Credentials credentials, final boolean tabletServersToo) throws AccumuloException,
      AccumuloSecurityException {
    MasterClient.execute(instance, new ClientExec<MasterClientService.Client>() {
      @Override
      public void execute(MasterClientService.Client client) throws Exception {
        client.shutdown(Tracer.traceInfo(), credentials.toThrift(instance), tabletServersToo);
      }
    });
  }

  private static void stopTabletServer(final AccumuloConfiguration conf, final Instance instance, final Credentials creds, List<String> servers, final boolean force) throws AccumuloException,
      AccumuloSecurityException {
    if (instance.getMasterLocations().size() == 0) {
      log.info("No masters running. Not attempting safe unload of tserver.");
      return;
    }
    for (String server : servers) {
      HostAndPort address = AddressUtil.parseAddress(server, conf.getPort(Property.TSERV_CLIENTPORT));
      final String finalServer = address.toString();
      log.info("Stopping server " + finalServer);
      MasterClient.execute(instance, new ClientExec<MasterClientService.Client>() {
        @Override
        public void execute(MasterClientService.Client client) throws Exception {
          client.shutdownTabletServer(Tracer.traceInfo(), creds.toThrift(instance), finalServer, force);
        }
      });
    }
  }

  private static final String ACCUMULO_SITE_BACKUP_FILE = "accumulo-site.xml.bak";
  private static final String PERMISSION_FILE_SUFFIX = "_perm.cfg";
  private static final MessageFormat configFormat = new MessageFormat("config -t {0} -s {1}\n");
  private static final MessageFormat sysPermFormat = new MessageFormat("grant System.{0} -s -u {1}\n");
  private static final MessageFormat tablePermFormat = new MessageFormat("grant Table.{0} -t {1} -u {2}\n");

  private static DefaultConfiguration defaultConfig;
  private static Map<String,String> siteConfig, systemConfig;
  private static List<String> localUsers;

  public static void printConfig(Instance instance, String principal, AuthenticationToken token, DumpConfigCommand opts) throws Exception {

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
    Connector connector = instance.getConnector(principal, token);
    defaultConfig = AccumuloConfiguration.getDefaultConfiguration();
    siteConfig = connector.instanceOperations().getSiteConfiguration();
    systemConfig = connector.instanceOperations().getSystemConfiguration();
    if (opts.userPermissions) {
      localUsers = Lists.newArrayList(connector.securityOperations().listLocalUsers());
      Collections.sort(localUsers);
    }
    if (opts.allConfiguration) {
      printSystemConfiguration(connector, outputDirectory, opts.userPermissions);
      SortedSet<String> tableNames = connector.tableOperations().list();
      for (String tableName : tableNames) {
        printTableConfiguration(connector, tableName, outputDirectory, opts.userPermissions);
      }

    } else {
      if (opts.systemConfiguration) {
        printSystemConfiguration(connector, outputDirectory, opts.userPermissions);
      }

      for (String tableName : opts.tables) {
        printTableConfiguration(connector, tableName, outputDirectory, opts.userPermissions);
      }
    }
  }

  private static String getDefaultConfigValue(String key) {
    if (null == key)
      return null;

    String defaultValue = null;
    try {
      Property p = Property.getPropertyByKey(key);
      if (null == p)
        return defaultValue;
      defaultValue = defaultConfig.get(p);
    } catch (IllegalArgumentException e) {}
    return defaultValue;
  }

  private static void printSystemConfiguration(Connector connector, File outputDirectory, boolean userPermissions) throws IOException, AccumuloException,
      AccumuloSecurityException {
    Configuration conf = new Configuration(false);
    for (Entry<String,String> prop : siteConfig.entrySet()) {
      String defaultValue = getDefaultConfigValue(prop.getKey());
      if (!prop.getValue().equals(defaultValue) && !systemConfig.containsKey(prop.getKey())) {
        conf.set(prop.getKey(), prop.getValue());
      }
    }
    for (Entry<String,String> prop : systemConfig.entrySet()) {
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
    if (userPermissions) {
      File permScript = new File(outputDirectory, "system" + PERMISSION_FILE_SUFFIX);
      FileWriter writer = new FileWriter(permScript);
      for (String principal : localUsers) {
        for (SystemPermission perm : SystemPermission.values()) {
          if (connector.securityOperations().hasSystemPermission(principal, perm)) {
            writer.write(sysPermFormat.format(new String[] {perm.name(), principal}));
          }
        }
      }
      writer.close();
    }
  }

  private static void printTableConfiguration(Connector connector, String tableName, File outputDirectory, boolean userPermissions) throws AccumuloException,
      TableNotFoundException, IOException, AccumuloSecurityException {
    Iterable<Entry<String,String>> tableConfig = connector.tableOperations().getProperties(tableName);
    File tableBackup = new File(outputDirectory, tableName + ".cfg");
    FileWriter writer = new FileWriter(tableBackup);
    for (Entry<String,String> prop : tableConfig) {
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

    if (userPermissions) {
      File permScript = new File(outputDirectory, tableName + PERMISSION_FILE_SUFFIX);
      FileWriter permWriter = new FileWriter(permScript);
      for (String principal : localUsers) {
        for (TablePermission perm : TablePermission.values()) {
          if (connector.securityOperations().hasTablePermission(principal, tableName, perm)) {
            permWriter.write(tablePermFormat.format(new String[] {perm.name(), tableName, principal}));
          }
        }
      }
      permWriter.close();
    }
  }
}
