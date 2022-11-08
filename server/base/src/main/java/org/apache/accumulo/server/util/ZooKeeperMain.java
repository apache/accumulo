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

import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.start.spi.KeywordExecutable;

import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;

@AutoService(KeywordExecutable.class)
public class ZooKeeperMain implements KeywordExecutable {

  static class Opts extends Help {

    @Parameter(names = {"-z", "--keepers"},
        description = "Comma separated list of zookeeper hosts (host:port,host:port)")
    String servers = null;

    @Parameter(names = {"-t", "--timeout"},
        description = "timeout, in seconds to timeout the zookeeper connection")
    long timeout = 30;
  }

  public static void main(String[] args) throws Exception {
    new ZooKeeperMain().execute(args);
  }

  @Override
  public String keyword() {
    return "zookeeper";
  }

  @Override
  public UsageGroup usageGroup() {
    return UsageGroup.PROCESS;
  }

  @Override
  public String description() {
    return "Starts Apache Zookeeper instance";
  }

  @Override
  public void execute(final String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(ZooKeeperMain.class.getName(), args);
    try (var context = new ServerContext(SiteConfiguration.auto())) {
      if (opts.servers == null) {
        opts.servers = context.getZooKeepers();
      }
      System.out.println("The accumulo instance id is " + context.getInstanceID());
      if (!opts.servers.contains("/")) {
        opts.servers += "/accumulo/" + context.getInstanceID();
      }
      org.apache.zookeeper.ZooKeeperMain
          .main(new String[] {"-server", opts.servers, "-timeout", "" + (opts.timeout * 1000)});
    }
  }
}
