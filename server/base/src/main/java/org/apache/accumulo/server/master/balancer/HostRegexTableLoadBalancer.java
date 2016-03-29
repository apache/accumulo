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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TabletMigration;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This balancer will create pools of tablet servers by grouping tablet servers that match a regex into the same pool and calling the balancer set on the table
 * to balance within the set of matching tablet servers. <br>
 * Regex properties for this balancer are specified as:<br>
 * <b>table.custom.balancer.host.regex.&lt;tablename&gt;=&lt;regex&gt;</b><br>
 * Periodically (default 5m) this balancer will check to see if a tablet server is hosting tablets that it should not be according to the regex configuration.
 * If this occurs then the offending tablets will be reassigned. This would cover the case where the configuration is changed and the master is restarted while
 * the tablet servers are up. To change the out of bounds check time period, set the following property:<br>
 * <b>table.custom.balancer.host.regex.oob.period=5m</b><br>
 * Periodically (default 5m) this balancer will regroup the set of current tablet servers into pools based on regexes applied to the tserver host names. This
 * would cover the case of tservers dying or coming online. To change the host pool check time period, set the following property: <br>
 * <b>table.custom.balancer.host.regex.pool.check=5m</b><br>
 * Regex matching can be based on either the host name (default) or host ip address. To set this balancer to match the regular expressions to the tablet server
 * IP address, then set the following property:<br>
 * <b>table.custom.balancer.host.regex.is.ip=true</b>
 *
 */
public class HostRegexTableLoadBalancer extends TableLoadBalancer {

  private static final Logger LOG = LoggerFactory.getLogger(HostRegexTableLoadBalancer.class);
  public static final String HOST_BALANCER_PREFIX = Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "balancer.host.regex.";
  public static final String HOST_BALANCER_OOB_CHECK = Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "balancer.host.regex.oob.period";
  private static final String HOST_BALANCER_OOB_DEFAULT = "5m";
  public static final String HOST_BALANCER_POOL_RECHECK_KEY = Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "balancer.host.regex.pool.check";
  private static final String HOST_BALANCER_POOL_RECHECK_DEFAULT = "5m";
  public static final String HOST_BALANCER_REGEX_USING_IPS = Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "balancer.host.regex.is.ip";
  protected static final String DEFAULT_POOL = "HostTableLoadBalancer.ALL";

  protected long oobCheckMillis = AccumuloConfiguration.getTimeInMillis(HOST_BALANCER_OOB_DEFAULT);
  protected long poolRecheckMillis = AccumuloConfiguration.getTimeInMillis(HOST_BALANCER_POOL_RECHECK_DEFAULT);

  private Map<String,String> tableIdToTableName = null;
  private Map<String,Pattern> poolNameToRegexPattern = null;
  private long lastOOBCheck = System.currentTimeMillis();
  private long lastPoolRecheck = 0;
  private boolean isIpBasedRegex = false;
  private Map<String,SortedMap<TServerInstance,TabletServerStatus>> pools = new HashMap<String,SortedMap<TServerInstance,TabletServerStatus>>();

  /**
   * Group the set of current tservers by pool name. Tservers that don't match a regex are put into a default ppol.
   *
   * @param current
   *          map of current tservers
   * @return current servers grouped by pool name, if not a match it is put into a default pool.
   */
  protected synchronized Map<String,SortedMap<TServerInstance,TabletServerStatus>> splitCurrentByRegex(SortedMap<TServerInstance,TabletServerStatus> current) {
    if ((System.currentTimeMillis() - lastPoolRecheck) > poolRecheckMillis) {
      Map<String,SortedMap<TServerInstance,TabletServerStatus>> newPools = new HashMap<String,SortedMap<TServerInstance,TabletServerStatus>>();
      for (Entry<TServerInstance,TabletServerStatus> e : current.entrySet()) {
        String tableName = getPoolNameForHost(e.getKey().host());
        if (!newPools.containsKey(tableName)) {
          newPools.put(tableName, new TreeMap<TServerInstance,TabletServerStatus>());
        }
        newPools.get(tableName).put(e.getKey(), e.getValue());
      }
      // Ensure that no host is in more than one pool
      // TODO: I'm not sure that I need to check for disjoint as the call to getPoolNameForHost checks for more than one match
      boolean error = false;
      for (SortedMap<TServerInstance,TabletServerStatus> s1 : newPools.values()) {
        for (SortedMap<TServerInstance,TabletServerStatus> s2 : newPools.values()) {
          if (s1 == s2) {
            continue;
          }
          if (!Collections.disjoint(s1.keySet(), s2.keySet())) {
            LOG.error("Pools are not disjoint: {}, there is a problem with your regular expressions. Putting all servers in default pool", newPools);
            error = true;
          }
        }
      }
      if (error) {
        // Put all servers into the default pool
        newPools.clear();
        newPools.put(DEFAULT_POOL, new TreeMap<TServerInstance,TabletServerStatus>());
        for (Entry<TServerInstance,TabletServerStatus> e : current.entrySet()) {
          newPools.get(DEFAULT_POOL).put(e.getKey(), e.getValue());
        }
      }
      pools = newPools;
      this.lastPoolRecheck = System.currentTimeMillis();
    }
    return pools;
  }

  /**
   * Matches host against the regexes and returns the matching pool name
   *
   * @param host
   *          tablet server host
   * @return name of pool. will return default pool if host matches more than one regex
   */
  protected String getPoolNameForHost(String host) {
    String test = host;
    String table = DEFAULT_POOL;
    if (!isIpBasedRegex) {
      try {
        test = getNameFromIp(host);
      } catch (UnknownHostException e1) {
        LOG.error("Unable to determine host name for IP: " + host + ", setting to default pool", e1);
        return table;
      }
    }
    for (Entry<String,Pattern> e : poolNameToRegexPattern.entrySet()) {
      if (e.getValue().matcher(test).matches()) {
        if (!table.equals(DEFAULT_POOL)) {
          LOG.warn("host {} matches more than one regex, assigning to default pool", host);
          return DEFAULT_POOL;
        }
        table = e.getKey();
      }
    }
    return table;
  }

  protected String getNameFromIp(String hostIp) throws UnknownHostException {
    return InetAddress.getByName(hostIp).getHostName();
  }

  /**
   * Matches table name against pool names, returns matching pool name or DEFAULT_POOL.
   *
   * @param tableName
   *          name of table
   * @return tablet server pool name (table name or DEFAULT_POOL)
   */
  protected String getPoolNameForTable(String tableName) {
    if (null == tableName) {
      return DEFAULT_POOL;
    }
    return poolNameToRegexPattern.containsKey(tableName) ? tableName : DEFAULT_POOL;
  }

  /**
   * Parse configuration and extract properties
   *
   * @param conf
   *          server configuration
   */
  protected void parseConfiguration(ServerConfiguration conf) {
    tableIdToTableName = new HashMap<>();
    poolNameToRegexPattern = new HashMap<>();
    for (Entry<String,String> table : getTableOperations().tableIdMap().entrySet()) {
      tableIdToTableName.put(table.getValue(), table.getKey());
      Map<String,String> customProps = conf.getTableConfiguration(table.getValue()).getAllPropertiesWithPrefix(Property.TABLE_ARBITRARY_PROP_PREFIX);
      if (null != customProps && customProps.size() > 0) {
        for (Entry<String,String> customProp : customProps.entrySet()) {
          String tableName = customProp.getKey().substring(HOST_BALANCER_PREFIX.length());
          String regex = customProp.getValue();
          poolNameToRegexPattern.put(tableName, Pattern.compile(regex));
        }
      }
    }
    String oobProperty = conf.getConfiguration().get(HOST_BALANCER_OOB_CHECK);
    if (null != oobProperty) {
      oobCheckMillis = AccumuloConfiguration.getTimeInMillis(oobProperty);
    }
    String poolRecheckProperty = conf.getConfiguration().get(HOST_BALANCER_POOL_RECHECK_KEY);
    if (null != poolRecheckProperty) {
      poolRecheckMillis = AccumuloConfiguration.getTimeInMillis(poolRecheckProperty);
    }
    String ipBased = conf.getConfiguration().get(HOST_BALANCER_REGEX_USING_IPS);
    if (null != ipBased) {
      isIpBasedRegex = Boolean.parseBoolean(ipBased);
    }
  }

  public Map<String,String> getTableIdToTableName() {
    return tableIdToTableName;
  }

  public Map<String,Pattern> getPoolNameToRegexPattern() {
    return poolNameToRegexPattern;
  }

  public long getOobCheckMillis() {
    return oobCheckMillis;
  }

  public long getPoolRecheckMillis() {
    return poolRecheckMillis;
  }

  public boolean isIpBasedRegex() {
    return isIpBasedRegex;
  }

  @Override
  public void init(ServerConfiguration conf) {
    super.init(conf);
    parseConfiguration(conf);
  }

  @Override
  public void getAssignments(SortedMap<TServerInstance,TabletServerStatus> current, Map<KeyExtent,TServerInstance> unassigned,
      Map<KeyExtent,TServerInstance> assignments) {

    if ((System.currentTimeMillis() - this.lastOOBCheck) > this.oobCheckMillis) {
      // Check to see if a tablet is assigned outside the bounds of the pool. If so, migrate it.
      for (Entry<TServerInstance,TabletServerStatus> e : current.entrySet()) {
        String assignedPool = getPoolNameForHost(e.getKey().host());
        for (String pool : poolNameToRegexPattern.keySet()) {
          if (assignedPool.equals(pool) || pool.equals(DEFAULT_POOL)) {
            continue;
          }
          String tid = getTableOperations().tableIdMap().get(pool);
          try {
            List<TabletStats> outOfBoundsTablets = getOnlineTabletsForTable(e.getKey(), tid);
            for (TabletStats ts : outOfBoundsTablets) {
              LOG.info("Tablet {} is currently outside the bounds of the regex, reassigning", ts.toString());
              unassigned.put(new KeyExtent(ts.getExtent()), e.getKey());
            }
          } catch (TException e1) {
            LOG.error("Error in OOB check getting tablets for table {} from server {}", tid, e.getKey().host(), e);
          }
        }
      }
      this.oobCheckMillis = System.currentTimeMillis();
    }

    Map<String,SortedMap<TServerInstance,TabletServerStatus>> pools = splitCurrentByRegex(current);
    // separate the unassigned into tables
    Map<String,Map<KeyExtent,TServerInstance>> groupedUnassigned = new HashMap<String,Map<KeyExtent,TServerInstance>>();
    for (Entry<KeyExtent,TServerInstance> e : unassigned.entrySet()) {
      Map<KeyExtent,TServerInstance> tableUnassigned = groupedUnassigned.get(e.getKey().getTableId());
      if (tableUnassigned == null) {
        tableUnassigned = new HashMap<KeyExtent,TServerInstance>();
        groupedUnassigned.put(e.getKey().getTableId(), tableUnassigned);
      }
      tableUnassigned.put(e.getKey(), e.getValue());
    }
    // Send a view of the current servers to the tables tablet balancer
    for (Entry<String,Map<KeyExtent,TServerInstance>> e : groupedUnassigned.entrySet()) {
      Map<KeyExtent,TServerInstance> newAssignments = new HashMap<KeyExtent,TServerInstance>();
      String tableName = tableIdToTableName.get(e.getKey());
      String poolName = getPoolNameForTable(tableName);
      SortedMap<TServerInstance,TabletServerStatus> currentView = pools.get(poolName);
      getBalancerForTable(e.getKey()).getAssignments(currentView, e.getValue(), newAssignments);
      assignments.putAll(newAssignments);
    }
  }

  @Override
  public long balance(SortedMap<TServerInstance,TabletServerStatus> current, Set<KeyExtent> migrations, List<TabletMigration> migrationsOut) {
    long minBalanceTime = 5 * 1000;
    // Iterate over the tables and balance each of them
    TableOperations t = getTableOperations();
    if (t == null)
      return minBalanceTime;

    if (migrations != null && migrations.size() > 0) {
      LOG.warn("Not balancing tables due to outstanding migrations");
      return minBalanceTime;
    }

    Map<String,SortedMap<TServerInstance,TabletServerStatus>> currentGrouped = splitCurrentByRegex(current);
    for (String s : t.tableIdMap().values()) {
      String tableName = tableIdToTableName.get(s);
      String regexTableName = getPoolNameForTable(tableName);
      SortedMap<TServerInstance,TabletServerStatus> currentView = currentGrouped.get(regexTableName);
      ArrayList<TabletMigration> newMigrations = new ArrayList<TabletMigration>();
      long tableBalanceTime = getBalancerForTable(s).balance(currentView, migrations, newMigrations);
      if (tableBalanceTime < minBalanceTime) {
        minBalanceTime = tableBalanceTime;
      }
      migrationsOut.addAll(newMigrations);
    }

    return minBalanceTime;
  }

}
