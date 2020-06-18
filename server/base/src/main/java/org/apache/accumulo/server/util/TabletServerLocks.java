/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

import java.util.List;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooLock;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.server.ServerContext;

import com.beust.jcommander.Parameter;

public class TabletServerLocks {

  static class Opts extends Help {
    @Parameter(names = "-list")
    boolean list = false;
    @Parameter(names = "-delete")
    String delete = null;
  }

  public static void main(String[] args) throws Exception {

    try (var context = new ServerContext(SiteConfiguration.auto())) {
      String tserverPath = context.getZooKeeperRoot() + Constants.ZTSERVERS;
      Opts opts = new Opts();
      opts.parseArgs(TabletServerLocks.class.getName(), args);

      ZooCache cache = context.getZooCache();
      ZooReaderWriter zoo = context.getZooReaderWriter();

      if (opts.list) {

        List<String> tabletServers = zoo.getChildren(tserverPath);

        for (String tabletServer : tabletServers) {
          byte[] lockData = ZooLock.getLockData(cache, tserverPath + "/" + tabletServer, null);
          String holder = null;
          if (lockData != null) {
            holder = new String(lockData, UTF_8);
          }

          System.out.printf("%32s %16s%n", tabletServer, holder);
        }
      } else if (opts.delete != null) {
        ZooLock.deleteLock(zoo, tserverPath + "/" + args[1]);
      } else {
        System.out.println(
            "Usage : " + TabletServerLocks.class.getName() + " -list|-delete <tserver lock>");
      }

    }
  }

}
