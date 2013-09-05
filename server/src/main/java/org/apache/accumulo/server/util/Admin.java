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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.server.cli.ClientOpts;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.trace.instrument.Tracer;
import org.apache.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

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

    StopCommand stopOpts = new StopCommand();
    cl.addCommand("stop", stopOpts);
    StopAllCommand stopAllOpts = new StopAllCommand();
    cl.addCommand("stopAll", stopAllOpts);
    StopMasterCommand stopMasterOpts = new StopMasterCommand();
    cl.addCommand("stopMaster", stopMasterOpts);
    cl.parse(args);
    
    if (opts.help || cl.getParsedCommand() == null) {
      cl.usage();
      return;
    }
    Instance instance = opts.getInstance();
    
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

      }else if (cl.getParsedCommand().equals("stop")) {
        stopTabletServer(instance, new Credentials(principal, token), stopOpts.args, opts.force);
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
  
  private static void stopTabletServer(final Instance instance, final Credentials creds, List<String> servers, final boolean force) throws AccumuloException,
      AccumuloSecurityException {
    for (String server : servers) {
      InetSocketAddress address = AddressUtil.parseAddress(server);
      final String finalServer = org.apache.accumulo.core.util.AddressUtil.toString(address);
      log.info("Stopping server " + finalServer);
      MasterClient.execute(instance, new ClientExec<MasterClientService.Client>() {
        @Override
        public void execute(MasterClientService.Client client) throws Exception {
          client.shutdownTabletServer(Tracer.traceInfo(), creds.toThrift(instance), finalServer, force);
        }
      });
    }
  }
}
