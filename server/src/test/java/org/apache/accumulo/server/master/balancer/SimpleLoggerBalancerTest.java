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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.util.StringUtil;
import org.apache.accumulo.server.master.balancer.LoggerUser;
import org.apache.accumulo.server.master.balancer.SimpleLoggerBalancer;
import org.junit.Test;

public class SimpleLoggerBalancerTest {
  
  class MockServer implements LoggerUser {
    
    Set<String> loggers;
    String name;
    
    MockServer(String name, String... loggers) {
      this.name = name;
      this.loggers = new HashSet<String>(Arrays.asList(loggers));
    }
    
    @Override
    public Set<String> getLoggers() {
      return loggers;
    }
    
    @Override
    public int compareTo(LoggerUser o) {
      if (o instanceof MockServer)
        return name.compareTo(((MockServer) o).name);
      return -1;
    }
    
    @Override
    public int hashCode() {
      return name.hashCode();
    }
    
    @Override
    public String toString() {
      return name;
    }
  }
  
  Map<String,Integer> computeUses(LoggerUser[] tservers, Map<LoggerUser,List<String>> uses, int numberOfLoggers) {
    for (LoggerUser lu : tservers) {
      MockServer user = (MockServer) lu;
      List<String> adjustments = uses.get(user);
      if (adjustments != null) {
        Set<String> newLoggers = new HashSet<String>(adjustments);
        for (String logger : user.loggers) {
          if (newLoggers.size() >= numberOfLoggers)
            break;
          newLoggers.add(logger);
        }
        user.loggers = newLoggers;
      }
    }
    Map<String,Integer> result = new HashMap<String,Integer>();
    for (LoggerUser server : tservers) {
      for (String logger : server.getLoggers()) {
        if (!result.containsKey(logger))
          result.put(logger, 0);
        result.put(logger, result.get(logger) + 1);
      }
    }
    return result;
  }
  
  void printAssignments(Map<LoggerUser,List<String>> assignments) {
    PrintStream out = System.out;
    for (Entry<LoggerUser,List<String>> entry : assignments.entrySet()) {
      out.println(entry.getKey() + ": " + StringUtil.join(entry.getValue(), ", "));
    }
  }
  
  @Test
  public void testBalanceNewLoggers() {
    SimpleLoggerBalancer balancer = new SimpleLoggerBalancer(0);
    List<String> loggers = Arrays.asList("logger1", "logger2", "logger3");
    LoggerUser[] tservers = {new MockServer("tserv1", "logger1"), new MockServer("tserv2", "logger1"), new MockServer("tserv3", "logger1")};
    Map<LoggerUser,List<String>> assignmentsOut = new HashMap<LoggerUser,List<String>>();
    balancer.balance(Arrays.asList(tservers), loggers, assignmentsOut, 1);
    assertEquals(2, assignmentsOut.size());
    Map<String,Integer> computeUses = computeUses(tservers, assignmentsOut, 1);
    
    assertEquals(new Integer(1), computeUses.get("logger1"));
    assertEquals(new Integer(1), computeUses.get("logger2"));
    assertEquals(new Integer(1), computeUses.get("logger3"));
  }
  
  @Test
  public void testBalanceTrivial() {
    SimpleLoggerBalancer balancer = new SimpleLoggerBalancer(0);
    LoggerUser[] tservers = {new MockServer("tserv1", "logger1"), new MockServer("tserv2", "logger2"), new MockServer("tserv3", "logger3")};
    List<String> loggers = Arrays.asList("logger1", "logger2", "logger3");
    Map<LoggerUser,List<String>> assignmentsOut = new HashMap<LoggerUser,List<String>>();
    balancer.balance(Arrays.asList(tservers), loggers, assignmentsOut, 1);
    assertEquals(0, assignmentsOut.size());
  }
  
  @Test
  public void testBalanceUneven() {
    SimpleLoggerBalancer balancer = new SimpleLoggerBalancer(0);
    List<String> loggers = Arrays.asList("logger1", "logger2", "logger3");
    LoggerUser[] tservers = {new MockServer("tserv1", "logger1", "logger2"), new MockServer("tserv2", "logger1", "logger2"),
        new MockServer("tserv3", "logger1", "logger2")};
    Map<LoggerUser,List<String>> assignmentsOut = new HashMap<LoggerUser,List<String>>();
    balancer.balance(Arrays.asList(tservers), loggers, assignmentsOut, 2);
    Map<String,Integer> computeUses = computeUses(tservers, assignmentsOut, 2);
    // make sure our unloaded logger got some work
    assertEquals(new Integer(2), computeUses.get("logger3"));
    // make sure the other loggers are still used
    Integer uses = computeUses.get("logger1").intValue() + computeUses.get("logger2").intValue();
    assertEquals(new Integer(4), uses);
    // re-run balancer
    balancer.balance(Arrays.asList(tservers), loggers, assignmentsOut, 2);
    computeUses = computeUses(tservers, assignmentsOut, 2);
    assertEquals(new Integer(2), computeUses.get("logger1"));
    assertEquals(new Integer(2), computeUses.get("logger2"));
    assertEquals(new Integer(2), computeUses.get("logger3"));
  }
  
  @Test
  public void testBalanceLotsOfLoggers() {
    SimpleLoggerBalancer balancer = new SimpleLoggerBalancer(0);
    List<String> loggers = Arrays.asList("logger1", "logger2", "logger3", "logger4");
    LoggerUser[] tservers = {new MockServer("tserv1", "logger1", "logger2"), new MockServer("tserv2", "logger1", "logger2"),
        new MockServer("tserv3", "logger1", "logger2")};
    Map<LoggerUser,List<String>> assignmentsOut = new HashMap<LoggerUser,List<String>>();
    balancer.balance(Arrays.asList(tservers), loggers, assignmentsOut, 2);
    Map<String,Integer> computeUses = computeUses(tservers, assignmentsOut, 2);
    assertTrue(computeUses.get("logger1") <= 2);
    assertTrue(computeUses.get("logger2") <= 2);
    assertTrue(computeUses.get("logger3") <= 2);
    assertTrue(computeUses.get("logger4") <= 2);
  }
  
  @Test
  public void testWildlyUnbalanced() {
    SimpleLoggerBalancer balancer = new SimpleLoggerBalancer(0);
    final int MANY = 100;
    List<String> loggers = new ArrayList<String>();
    for (int i = 0; i < MANY; i++) {
      loggers.add(String.format("logger%d", i));
    }
    LoggerUser[] tservers = new LoggerUser[MANY];
    for (int i = 0; i < MANY; i++) {
      tservers[i] = new MockServer(String.format("tserv%s", i), "logger0", "logger1");
    }
    Map<LoggerUser,List<String>> assignmentsOut = new HashMap<LoggerUser,List<String>>();
    balancer.balance(Arrays.asList(tservers), loggers, assignmentsOut, 2);
    Map<String,Integer> computeUses = computeUses(tservers, assignmentsOut, 2);
    for (String logger : loggers) {
      assertTrue(computeUses.get(logger) == 2);
    }
  }
}
