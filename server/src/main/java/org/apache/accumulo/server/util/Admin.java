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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.ClientExec;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.security.CredentialHelper;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.server.cli.ClientOpts;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.security.SecurityConstants;
import org.apache.accumulo.trace.instrument.Tracer;
import org.apache.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

public class Admin {
  private static final Logger log = Logger.getLogger(Admin.class);
  
  static class AdminOpts extends ClientOpts {
    @Parameter(names={"-f", "--force"}, description="force the given server to stop by removing its lock")
    boolean force = false;
  }

  @Parameters(commandDescription="stop the tablet server on the given hosts")
  static class StopCommand {
    @Parameter(description="<host> {<host> ... }")
    List<String> args = new ArrayList<String>();
  }
  
  @Parameters(commandDescription="stop the master")
  static class StopMasterCommand {
  }

  @Parameters(commandDescription="stop all the servers")
  static class StopAllCommand {
  }

  public static void main(String[] args) {
    boolean everything;

    AdminOpts opts = new AdminOpts();
    JCommander cl = new JCommander(opts);
    cl.setProgramName(Admin.class.getName());
    StopCommand stopOpts = new StopCommand();
    cl.addCommand("stop", stopOpts);
    StopMasterCommand stopMasterOpts = new StopMasterCommand();
    cl.addCommand("stopMaster", stopMasterOpts);
    StopAllCommand stopAllOpts = new StopAllCommand();
    cl.addCommand("stopAll", stopAllOpts);
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
        principal = SecurityConstants.getSystemPrincipal();
        token = SecurityConstants.getSystemToken();
      } else {
        principal = opts.principal;
        token = opts.getToken();
      }

      if (cl.getParsedCommand().equals("stop")) {
        stopTabletServer(instance, CredentialHelper.create(principal, token, instance.getInstanceID()), stopOpts.args, opts.force);
      } else {
        everything = cl.getParsedCommand().equals("stopAll");
        stopServer(instance, CredentialHelper.create(principal, token, instance.getInstanceID()), everything);
      }
    } catch (AccumuloException e) {
      log.error(e,e);
      System.exit(1);
    } catch (AccumuloSecurityException e) {
      log.error(e,e);
      System.exit(2);
    }
  }
  
  private static void stopServer(Instance instance, final TCredentials credentials, final boolean tabletServersToo) throws AccumuloException, AccumuloSecurityException {
    MasterClient.execute(HdfsZooInstance.getInstance(), new ClientExec<MasterClientService.Client>() {
      @Override
      public void execute(MasterClientService.Client client) throws Exception {
        client.shutdown(Tracer.traceInfo(), credentials, tabletServersToo);
      }
    });
  }
  
  private static void stopTabletServer(Instance instance, final TCredentials creds, List<String> servers, final boolean force) throws AccumuloException, AccumuloSecurityException {
    for (String server : servers) {
      InetSocketAddress address = AddressUtil.parseAddress(server, Property.TSERV_CLIENTPORT);
      final String finalServer = org.apache.accumulo.core.util.AddressUtil.toString(address);
      log.info("Stopping server " + finalServer);
      MasterClient.execute(HdfsZooInstance.getInstance(), new ClientExec<MasterClientService.Client>() {
        @Override
        public void execute(MasterClientService.Client client) throws Exception {
          client.shutdownTabletServer(Tracer.traceInfo(), creds, finalServer, force);
        }
      });
    }
  }
}
