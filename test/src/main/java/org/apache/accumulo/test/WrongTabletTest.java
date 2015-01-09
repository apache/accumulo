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

import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.server.cli.ClientOpts;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.trace.instrument.Tracer;
import org.apache.hadoop.io.Text;

import com.beust.jcommander.Parameter;

public class WrongTabletTest {

  static class Opts extends ClientOpts {
    @Parameter(names = "--location", required = true)
    String location;
  }

  public static void main(String[] args) {
    Opts opts = new Opts();
    opts.parseArgs(WrongTabletTest.class.getName(), args);

    ServerConfiguration conf = new ServerConfiguration(opts.getInstance());
    try {
      TabletClientService.Iface client = ThriftUtil.getTServerClient(opts.location, conf.getConfiguration());

      Mutation mutation = new Mutation(new Text("row_0003750001"));
      mutation.putDelete(new Text("colf"), new Text("colq"));
      client.update(Tracer.traceInfo(), new Credentials(opts.principal, opts.getToken()).toThrift(opts.getInstance()), new KeyExtent(new Text("!!"), null,
          new Text("row_0003750000")).toThrift(), mutation.toThrift());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
