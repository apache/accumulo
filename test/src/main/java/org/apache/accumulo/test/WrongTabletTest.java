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
package org.apache.accumulo.test;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.tabletserver.thrift.TDurability;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.trace.Tracer;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.cli.ClientOpts;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.hadoop.io.Text;

import com.beust.jcommander.Parameter;

public class WrongTabletTest {

  static class Opts extends ClientOpts {
    @Parameter(names = "--location", required = true)
    String location;
  }

  public static void main(String[] args) {
    final Opts opts = new Opts();
    opts.parseArgs(WrongTabletTest.class.getName(), args);

    final HostAndPort location = HostAndPort.fromString(opts.location);
    final Instance inst = opts.getInstance();
    final ServerConfigurationFactory conf = new ServerConfigurationFactory(inst);
    final ClientContext context = new AccumuloServerContext(inst, conf) {
      @Override
      public synchronized Credentials getCredentials() {
        try {
          return new Credentials(opts.getPrincipal(), opts.getToken());
        } catch (AccumuloSecurityException e) {
          throw new RuntimeException(e);
        }
      }
    };
    try {
      TabletClientService.Iface client = ThriftUtil.getTServerClient(location, context);

      Mutation mutation = new Mutation(new Text("row_0003750001"));
      mutation.putDelete(new Text("colf"), new Text("colq"));
      client.update(Tracer.traceInfo(), context.rpcCreds(), new KeyExtent(Table.ID.of("!!"), null, new Text("row_0003750000")).toThrift(), mutation.toThrift(),
          TDurability.DEFAULT);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
