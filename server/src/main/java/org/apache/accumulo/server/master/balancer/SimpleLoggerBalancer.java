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
package org.apache.accumulo.server.master.balancer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class SimpleLoggerBalancer implements LoggerBalancer {
  
  private final long minimumTimeBetweenRebalances;
  private long lastBalance;
  
  public SimpleLoggerBalancer() {
    this(60 * 1000);
  }
  
  public SimpleLoggerBalancer(long minimumTimeBetweenRebalances) {
    this.lastBalance = System.currentTimeMillis();
    this.minimumTimeBetweenRebalances = minimumTimeBetweenRebalances;
  }
  
  @Override
  public void balance(List<LoggerUser> current, List<String> loggers, Map<LoggerUser,List<String>> assignmentsOut, int loggersPerServer) {
    if (System.currentTimeMillis() - lastBalance < minimumTimeBetweenRebalances)
      return;
    // any loggers?
    if (loggers.size() <= 0)
      return;
    // more than one user of loggers?
    if (current.size() < 2)
      return;
    
    // compute the "load" on loggers, create a list of the users for each logger
    Map<String,List<LoggerUser>> counts = new HashMap<String,List<LoggerUser>>();
    for (String logger : loggers) {
      counts.put(logger, new ArrayList<LoggerUser>());
    }
    int uses = 0;
    for (LoggerUser user : current) {
      for (String logger : user.getLoggers()) {
        uses++;
        if (!counts.containsKey(logger))
          counts.put(logger, new ArrayList<LoggerUser>());
        counts.get(logger).add(user);
      }
    }
    
    // sort based on counts from high to low
    List<Entry<String,List<LoggerUser>>> byCount = new ArrayList<Entry<String,List<LoggerUser>>>(counts.entrySet());
    Collections.sort(byCount, new Comparator<Entry<String,List<LoggerUser>>>() {
      @Override
      public int compare(Entry<String,List<LoggerUser>> o1, Entry<String,List<LoggerUser>> o2) {
        return o2.getValue().size() - o1.getValue().size();
      }
    });
    // already balanced?
    // balanced means that no logger is being used by more than ceiling(average(number servers per logger))
    final int average = (int) Math.ceil((double) uses / loggers.size());
    if (byCount.get(0).getValue().size() <= average)
      return;
    
    // Rebalance
    // move a server on high-use loggers to low-use loggers if it is not currently using that logger
    for (Entry<String,List<LoggerUser>> entry : byCount) {
      // String logger = entry.getKey();
      List<LoggerUser> servers = entry.getValue();
      if (servers.size() <= average)
        return;
      // Walk backwards from the low-use loggers, looking for a logger that is:
      // 1) still low-use and 2) not used by this server
      for (int i = byCount.size() - 1; i >= 0; i--) {
        String lowCountLogger = byCount.get(i).getKey();
        List<LoggerUser> lowCountUsers = byCount.get(i).getValue();
        if (lowCountUsers.size() >= average)
          continue;
        Set<LoggerUser> notUsingLowCountLogger = (Set<LoggerUser>) new HashSet<LoggerUser>(servers);
        notUsingLowCountLogger.removeAll(lowCountUsers);
        if (notUsingLowCountLogger.isEmpty())
          continue;
        for (LoggerUser user : notUsingLowCountLogger) {
          if (lowCountUsers.size() >= average)
            break;
          if (!assignmentsOut.containsKey(user))
            assignmentsOut.put(user, new ArrayList<String>());
          if (assignmentsOut.get(user).size() < loggersPerServer) {
            assignmentsOut.get(user).add(lowCountLogger);
            lowCountUsers.add(user);
          }
        }
      }
    }
  }
}
