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

import java.text.DateFormat;
import java.util.Date;
import java.util.List;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.tabletserver.thrift.ActionStats;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.security.SecurityConstants;
import org.apache.thrift.TException;

public class DumpTabletsOnServer {
  
  private static void print(String fmt, Object... args) {
    System.out.println(String.format(fmt, args));
  }
  
  /**
   * @param args
   * @throws TException
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println(String.format("Usage: accumulo %s hostname[:port] table", DumpTabletsOnServer.class.getName()));
      System.exit(-1);
    }
    Instance instance = HdfsZooInstance.getInstance();
    String tableId = Tables.getTableId(instance, args[1]);
    if (tableId == null) {
      System.err.println("Cannot find table " + args[1] + " in zookeeper");
      System.exit(-1);
    }
    List<TabletStats> onlineTabletsForTable = ThriftUtil.getTServerClient(args[0], ServerConfiguration.getSystemConfiguration()).getTabletStats(null,
        SecurityConstants.getSystemCredentials(), tableId);
    for (TabletStats stats : onlineTabletsForTable) {
      print("%s", stats.extent);
      print("  ingest %.2f", stats.ingestRate);
      print("  query %.2f", stats.queryRate);
      print("  entries %.2f", stats.queryRate);
      print("  split time %s", stats.splitCreationTime == 0 ? "never" : DateFormat.getInstance().format(new Date(stats.splitCreationTime)));
      printStats("split", stats.split);
      printStats("major", stats.major);
      printStats("minor", stats.minor);
    }
  }
  
  private static void printStats(String which, ActionStats stats) {
    print("  %s count %d", which, stats.count);
    print("  %s elapsed %.2f", which, stats.elapsed);
    print("  %s fail %d", which, stats.fail);
    print("  %s num %d", which, stats.num);
    print("  %s status %d", which, stats.status);
    print("  %s queue time %.2f", which, stats.queueTime);
    print("  %s sum deviation %.2f", which, stats.sumDev);
    print("  %s queue sum deviation %.2f", which, stats.queueSumDev);
  }
}
