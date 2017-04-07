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
package org.apache.accumulo.test.randomwalk.concurrent;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.master.thrift.MasterClientService.Client;
import org.apache.accumulo.core.master.thrift.MasterGoalState;
import org.apache.accumulo.core.trace.Tracer;
import org.apache.accumulo.master.state.SetGoalState;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.test.randomwalk.Environment;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

public class Shutdown extends Test {

  @Override
  public void visit(State state, Environment env, Properties props) throws Exception {
    log.info("shutting down");
    SetGoalState.main(new String[] {MasterGoalState.CLEAN_STOP.name()});

    while (!env.getConnector().instanceOperations().getTabletServers().isEmpty()) {
      sleepUninterruptibly(1, TimeUnit.SECONDS);
    }

    while (true) {
      try {
        AccumuloServerContext context = new AccumuloServerContext(new ServerConfigurationFactory(HdfsZooInstance.getInstance()));
        Client client = MasterClient.getConnection(context);
        client.getMasterStats(Tracer.traceInfo(), context.rpcCreds());
      } catch (Exception e) {
        // assume this is due to server shutdown
        break;
      }
      sleepUninterruptibly(1, TimeUnit.SECONDS);
    }

    log.info("servers stopped");
    sleepUninterruptibly(10, TimeUnit.SECONDS);
  }

}
