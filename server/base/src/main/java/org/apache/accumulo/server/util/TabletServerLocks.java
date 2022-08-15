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

import java.util.List;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.fate.zookeeper.ServiceLock;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.server.ServerContext;

import com.beust.jcommander.Parameter;

public class TabletServerLocks {

  static class Opts extends Help {
    @Parameter(names = "-delete")
    String delete = null;
  }

  public static void tabletServerLocks(String commandName, final ServerContext context,
      final String lock, final String delete) throws Exception {

    String tserverPath = context.getZooKeeperRoot() + Constants.ZTSERVERS;

    ZooCache cache = context.getZooCache();
    ZooReaderWriter zoo = context.getZooReaderWriter();

    if (delete == null) {
      List<String> tabletServers = zoo.getChildren(tserverPath);

      for (String tabletServer : tabletServers) {
        var zLockPath = ServiceLock.path(tserverPath + "/" + tabletServer);
        byte[] lockData = ServiceLock.getLockData(cache, zLockPath, null);
        String holder = null;
        if (lockData != null) {
          holder = new String(lockData, UTF_8);
        }

        System.out.printf("%32s %16s%n", tabletServer, holder);
      }
    } else {
      if (lock == null) {
        printUsage(commandName);
      }
      ServiceLock.deleteLock(zoo, ServiceLock.path(tserverPath + "/" + lock));
    }
  }

  public static void main(String[] args) throws Exception {

    try (var context = new ServerContext(SiteConfiguration.auto())) {
      Opts opts = new Opts();
      opts.parseArgs(TabletServerLocks.class.getName(), args);

      tabletServerLocks(TabletServerLocks.class.getName(), context, args[1], opts.delete);
    }
  }

  private static void printUsage(String commandName) {
    System.out.println("Usage : " + commandName + "-delete <tserver lock>");
  }

}
