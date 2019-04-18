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
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.ConfigurationObserver;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TabletMigration;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

/**
 * This balancer creates groups of tablet servers using user-provided regular expressions over the
 * tablet server hostnames. Then it delegates to the table balancer to balance the tablets within
 * the resulting group of tablet servers. All tablet servers that do not match a regex are grouped
 * into a default group.<br>
 * Regex properties for this balancer are specified as:<br>
 * <b>table.custom.balancer.host.regex.&lt;tablename&gt;=&lt;regex&gt;</b><br>
 * Periodically (default 5m) this balancer will check to see if a tablet server is hosting tablets
 * that it should not be according to the regex configuration. If this occurs then the offending
 * tablets will be reassigned. This would cover the case where the configuration is changed and the
 * master is restarted while the tablet servers are up. To change the out of bounds check time
 * period, set the following property:<br>
 * <b>table.custom.balancer.host.regex.oob.period=5m</b><br>
 * Regex matching can be based on either the host name (default) or host ip address. To set this
 * balancer to match the regular expressions to the tablet server IP address, then set the following
 * property:<br>
 * <b>table.custom.balancer.host.regex.is.ip=true</b><br>
 * It's possible that this balancer may create a lot of migrations. To limit the number of
 * migrations that are created during a balance call, set the following property (default 250):<br>
 * <b>table.custom.balancer.host.regex.concurrent.migrations</b> This balancer can continue
 * balancing even if there are outstanding migrations. To limit the number of outstanding migrations
 * in which this balancer will continue balancing, set the following property (default 0):<br>
 * <b>table.custom.balancer.host.regex.max.outstanding.migrations</b>
 *
 */
public class HostRegexTableLoadBalancer extends TableLoadBalancer implements ConfigurationObserver {

  private static final String PROP_PREFIX = Property.TABLE_ARBITRARY_PROP_PREFIX.getKey();

  private static final Logger LOG = LoggerFactory.getLogger(HostRegexTableLoadBalancer.class);
  public static final String HOST_BALANCER_PREFIX = PROP_PREFIX + "balancer.host.regex.";
  public static final String HOST_BALANCER_OOB_CHECK_KEY = PROP_PREFIX
      + "balancer.host.regex.oob.period";
  private static final String HOST_BALANCER_OOB_DEFAULT = "5m";
  public static final String HOST_BALANCER_REGEX_USING_IPS_KEY = PROP_PREFIX
      + "balancer.host.regex.is.ip";
  public static final String HOST_BALANCER_REGEX_MAX_MIGRATIONS_KEY = PROP_PREFIX
      + "balancer.host.regex.concurrent.migrations";
  private static final int HOST_BALANCER_REGEX_MAX_MIGRATIONS_DEFAULT = 250;
  protected static final String DEFAULT_POOL = "HostTableLoadBalancer.ALL";
  private static final int DEFAULT_OUTSTANDING_MIGRATIONS = 0;
  public static final String HOST_BALANCER_OUTSTANDING_MIGRATIONS_KEY = PROP_PREFIX
      + "balancer.host.regex.max.outstanding.migrations";

  protected long oobCheckMillis = ConfigurationTypeHelper
      .getTimeInMillis(HOST_BALANCER_OOB_DEFAULT);

  private static final long ONE_HOUR = 60 * 60 * 1000;
  private static final Set<KeyExtent> EMPTY_MIGRATIONS = Collections.emptySet();

  private volatile Map<TableId,String> tableIdToTableName = null;
  private volatile Map<String,Pattern> poolNameToRegexPattern = null;
  private volatile long lastOOBCheck = System.currentTimeMillis();
  private volatile boolean isIpBasedRegex = false;
  private Map<String,SortedMap<TServerInstance,TabletServerStatus>> pools = new HashMap<>();
  private volatile int maxTServerMigrations = HOST_BALANCER_REGEX_MAX_MIGRATIONS_DEFAULT;
  private volatile int maxOutstandingMigrations = DEFAULT_OUTSTANDING_MIGRATIONS;
  private final Map<KeyExtent,TabletMigration> migrationsFromLastPass = new HashMap<>();
  private final Map<String,Long> tableToTimeSinceNoMigrations = new HashMap<>();

  /**
   * Group the set of current tservers by pool name. Tservers that don't match a regex are put into
   * a default pool. This could be expensive in the terms of the amount of time to recompute the
   * groups, so HOST_BALANCER_POOL_RECHECK_KEY should be specified in the terms of minutes, not
   * seconds or less.
   *
   * @param current
   *          map of current tservers
   * @return current servers grouped by pool name, if not a match it is put into a default pool.
   */
  // @formatter:off
  protected synchronized Map<String,SortedMap<TServerInstance,TabletServerStatus>>
    splitCurrentByRegex(SortedMap<TServerInstance,TabletServerStatus> current) {
  // @formatter:on
    LOG.debug("Performing pool recheck - regrouping tablet servers based on regular expressions");
    Map<String,SortedMap<TServerInstance,TabletServerStatus>> newPools = new HashMap<>();
    for (Entry<TServerInstance,TabletServerStatus> e : current.entrySet()) {
      List<String> poolNames = getPoolNamesForHost(e.getKey().host());
      for (String pool : poolNames) {
        SortedMap<TServerInstance,TabletServerStatus> np = newPools.get(pool);
        if (np == null) {
          np = new TreeMap<>(current.comparator());
          newPools.put(pool, np);
        }
        np.put(e.getKey(), e.getValue());
      }
    }

    if (newPools.get(DEFAULT_POOL) == null) {
      LOG.warn("Default pool is empty; assigning all tablet servers to the default pool");
      SortedMap<TServerInstance,TabletServerStatus> dp = new TreeMap<>(current.comparator());
      dp.putAll(current);
      newPools.put(DEFAULT_POOL, dp);
    }

    pools = newPools;

    LOG.trace("Pool to TabletServer mapping:");
    if (LOG.isTraceEnabled()) {
      for (Entry<String,SortedMap<TServerInstance,TabletServerStatus>> e : pools.entrySet()) {
        LOG.trace("\tpool: {} -> tservers: {}", e.getKey(), e.getValue().keySet());
      }
    }
    return pools;
  }

  /**
   * Matches host against the regexes and returns the matching pool names
   *
   * @param host
   *          tablet server host
   * @return pool names, will return default pool if host matches more no regex
   */
  protected List<String> getPoolNamesForHost(String host) {
    String test = host;
    if (!isIpBasedRegex) {
      try {
        test = getNameFromIp(host);
      } catch (UnknownHostException e1) {
        LOG.error("Unable to determine host name for IP: " + host + ", setting to default pool",
            e1);
        return Collections.singletonList(DEFAULT_POOL);
      }
    }
    List<String> pools = new ArrayList<>();
    for (Entry<String,Pattern> e : poolNameToRegexPattern.entrySet()) {
      if (e.getValue().matcher(test).matches()) {
        pools.add(e.getKey());
      }
    }
    if (pools.size() == 0) {
      pools.add(DEFAULT_POOL);
    }
    return pools;
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
    if (tableName == null) {
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
    TableOperations t = getTableOperations();
    if (t == null) {
      throw new RuntimeException("Table Operations cannot be null");
    }
    Map<TableId,String> tableIdToTableNameBuilder = new HashMap<>();
    Map<String,Pattern> poolNameToRegexPatternBuilder = new HashMap<>();
    for (Entry<String,String> table : t.tableIdMap().entrySet()) {
      TableId tableId = TableId.of(table.getValue());
      tableIdToTableNameBuilder.put(tableId, table.getKey());
      conf.getTableConfiguration(tableId).addObserver(this);
      Map<String,String> customProps = conf.getTableConfiguration(tableId)
          .getAllPropertiesWithPrefix(Property.TABLE_ARBITRARY_PROP_PREFIX);
      if (customProps != null && customProps.size() > 0) {
        for (Entry<String,String> customProp : customProps.entrySet()) {
          if (customProp.getKey().startsWith(HOST_BALANCER_PREFIX)) {
            if (customProp.getKey().equals(HOST_BALANCER_OOB_CHECK_KEY)
                || customProp.getKey().equals(HOST_BALANCER_REGEX_USING_IPS_KEY)
                || customProp.getKey().equals(HOST_BALANCER_REGEX_MAX_MIGRATIONS_KEY)
                || customProp.getKey().equals(HOST_BALANCER_OUTSTANDING_MIGRATIONS_KEY)) {
              continue;
            }
            String tableName = customProp.getKey().substring(HOST_BALANCER_PREFIX.length());
            String regex = customProp.getValue();
            poolNameToRegexPatternBuilder.put(tableName, Pattern.compile(regex));
          }
        }
      }
    }

    tableIdToTableName = ImmutableMap.copyOf(tableIdToTableNameBuilder);
    poolNameToRegexPattern = ImmutableMap.copyOf(poolNameToRegexPatternBuilder);

    String oobProperty = conf.getSystemConfiguration().get(HOST_BALANCER_OOB_CHECK_KEY);
    if (oobProperty != null) {
      oobCheckMillis = ConfigurationTypeHelper.getTimeInMillis(oobProperty);
    }
    String ipBased = conf.getSystemConfiguration().get(HOST_BALANCER_REGEX_USING_IPS_KEY);
    if (ipBased != null) {
      isIpBasedRegex = Boolean.parseBoolean(ipBased);
    }
    String migrations = conf.getSystemConfiguration().get(HOST_BALANCER_REGEX_MAX_MIGRATIONS_KEY);
    if (migrations != null) {
      maxTServerMigrations = Integer.parseInt(migrations);
    }
    String outstanding = conf.getSystemConfiguration()
        .get(HOST_BALANCER_OUTSTANDING_MIGRATIONS_KEY);
    if (outstanding != null) {
      this.maxOutstandingMigrations = Integer.parseInt(outstanding);
    }
    LOG.info("{}", this);
  }

  @Override
  public String toString() {
    ToStringBuilder buf = new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE);
    buf.append("\nTablet Out Of Bounds Check Interval", this.oobCheckMillis);
    buf.append("\nMax Tablet Server Migrations", this.maxTServerMigrations);
    buf.append("\nRegular Expressions use IPs", this.isIpBasedRegex);
    buf.append("\nPools", this.poolNameToRegexPattern);
    return buf.toString();
  }

  public Map<TableId,String> getTableIdToTableName() {
    return tableIdToTableName;
  }

  public Map<String,Pattern> getPoolNameToRegexPattern() {
    return poolNameToRegexPattern;
  }

  public int getMaxMigrations() {
    return maxTServerMigrations;
  }

  public int getMaxOutstandingMigrations() {
    return maxOutstandingMigrations;
  }

  public long getOobCheckMillis() {
    return oobCheckMillis;
  }

  public boolean isIpBasedRegex() {
    return isIpBasedRegex;
  }

  @Override
  public void init(ServerContext context) {
    super.init(context);
    parseConfiguration(context.getServerConfFactory());
  }

  @Override
  public void getAssignments(SortedMap<TServerInstance,TabletServerStatus> current,
      Map<KeyExtent,TServerInstance> unassigned, Map<KeyExtent,TServerInstance> assignments) {

    Map<String,SortedMap<TServerInstance,TabletServerStatus>> pools = splitCurrentByRegex(current);
    // group the unassigned into tables
    Map<TableId,Map<KeyExtent,TServerInstance>> groupedUnassigned = new HashMap<>();
    for (Entry<KeyExtent,TServerInstance> e : unassigned.entrySet()) {
      Map<KeyExtent,TServerInstance> tableUnassigned = groupedUnassigned
          .get(e.getKey().getTableId());
      if (tableUnassigned == null) {
        tableUnassigned = new HashMap<>();
        groupedUnassigned.put(e.getKey().getTableId(), tableUnassigned);
      }
      tableUnassigned.put(e.getKey(), e.getValue());
    }
    // Send a view of the current servers to the tables tablet balancer
    for (Entry<TableId,Map<KeyExtent,TServerInstance>> e : groupedUnassigned.entrySet()) {
      Map<KeyExtent,TServerInstance> newAssignments = new HashMap<>();
      String tableName = tableIdToTableName.get(e.getKey());
      String poolName = getPoolNameForTable(tableName);
      SortedMap<TServerInstance,TabletServerStatus> currentView = pools.get(poolName);
      if (currentView == null || currentView.size() == 0) {
        LOG.warn("No tablet servers online for table {}, assigning within default pool", tableName);
        currentView = pools.get(DEFAULT_POOL);
        if (currentView == null) {
          LOG.error(
              "No tablet servers exist in the default pool, unable to assign tablets for table {}",
              tableName);
          continue;
        }
      }
      LOG.debug("Sending {} tablets to balancer for table {} for assignment within tservers {}",
          e.getValue().size(), tableName, currentView.keySet());
      getBalancerForTable(e.getKey()).getAssignments(currentView, e.getValue(), newAssignments);
      assignments.putAll(newAssignments);
    }
  }

  @Override
  public long balance(SortedMap<TServerInstance,TabletServerStatus> current,
      Set<KeyExtent> migrations, List<TabletMigration> migrationsOut) {
    long minBalanceTime = 20 * 1000;
    // Iterate over the tables and balance each of them
    TableOperations t = getTableOperations();
    if (t == null)
      return minBalanceTime;

    Map<String,String> tableIdMap = t.tableIdMap();
    long now = System.currentTimeMillis();

    Map<String,SortedMap<TServerInstance,TabletServerStatus>> currentGrouped = splitCurrentByRegex(
        current);
    if ((now - this.lastOOBCheck) > this.oobCheckMillis) {
      try {
        // Check to see if a tablet is assigned outside the bounds of the pool. If so, migrate it.
        for (String table : t.list()) {
          LOG.debug("Checking for out of bounds tablets for table {}", table);
          String tablePoolName = getPoolNameForTable(table);
          for (Entry<TServerInstance,TabletServerStatus> e : current.entrySet()) {
            // pool names are the same as table names, except in the DEFAULT case.
            // If this table is assigned to a pool for this host, then move on.
            List<String> hostPools = getPoolNamesForHost(e.getKey().host());
            if (hostPools.contains(tablePoolName)) {
              continue;
            }
            String tid = tableIdMap.get(table);
            if (tid == null) {
              LOG.warn("Unable to check for out of bounds tablets for table {},"
                  + " it may have been deleted or renamed.", table);
              continue;
            }
            try {
              List<TabletStats> outOfBoundsTablets = getOnlineTabletsForTable(e.getKey(),
                  TableId.of(tid));
              if (outOfBoundsTablets == null) {
                continue;
              }
              Random random = new SecureRandom();
              for (TabletStats ts : outOfBoundsTablets) {
                KeyExtent ke = new KeyExtent(ts.getExtent());
                if (migrations.contains(ke)) {
                  LOG.debug("Migration for out of bounds tablet {} has already been requested", ke);
                  continue;
                }
                String poolName = getPoolNameForTable(table);
                SortedMap<TServerInstance,TabletServerStatus> currentView = currentGrouped
                    .get(poolName);
                if (currentView != null) {
                  int skip = random.nextInt(currentView.size());
                  Iterator<TServerInstance> iter = currentView.keySet().iterator();
                  for (int i = 0; i < skip; i++) {
                    iter.next();
                  }
                  TServerInstance nextTS = iter.next();
                  LOG.info("Tablet {} is currently outside the bounds of the"
                      + " regex, migrating from {} to {}", ke, e.getKey(), nextTS);
                  migrationsOut.add(new TabletMigration(ke, e.getKey(), nextTS));
                  if (migrationsOut.size() >= this.maxTServerMigrations) {
                    break;
                  }
                } else {
                  LOG.warn("No tablet servers online for pool {}, unable to"
                      + " migrate out of bounds tablets", poolName);
                }
              }
            } catch (TException e1) {
              LOG.error("Error in OOB check getting tablets for table {} from server {} {}", tid,
                  e.getKey().host(), e);
            }
          }
        }
      } finally {
        // this could have taken a while...get a new time
        this.lastOOBCheck = System.currentTimeMillis();
      }
    }

    if (migrationsOut.size() > 0) {
      LOG.warn("Not balancing tables due to moving {} out of bounds tablets", migrationsOut.size());
      LOG.info("Migrating out of bounds tablets: {}", migrationsOut);
      return minBalanceTime;
    }

    if (migrations != null && migrations.size() > 0) {
      if (migrations.size() >= maxOutstandingMigrations) {
        LOG.warn("Not balancing tables due to {} outstanding migrations", migrations.size());
        if (LOG.isTraceEnabled()) {
          LOG.trace("Sample up to 10 outstanding migrations: {}", Iterables.limit(migrations, 10));
        }
        return minBalanceTime;
      }

      LOG.debug("Current outstanding migrations of {} being applied", migrations.size());
      if (LOG.isTraceEnabled()) {
        LOG.trace("Sample up to 10 outstanding migrations: {}", Iterables.limit(migrations, 10));
      }
      migrationsFromLastPass.keySet().retainAll(migrations);
      SortedMap<TServerInstance,TabletServerStatus> currentCopy = new TreeMap<>(current);
      Multimap<TServerInstance,String> serverTableIdCopied = HashMultimap.create();
      for (TabletMigration migration : migrationsFromLastPass.values()) {
        TableInfo fromInfo = getTableInfo(currentCopy, serverTableIdCopied,
            migration.tablet.getTableId().toString(), migration.oldServer);
        if (fromInfo != null) {
          fromInfo.setOnlineTablets(fromInfo.getOnlineTablets() - 1);
        }
        TableInfo toInfo = getTableInfo(currentCopy, serverTableIdCopied,
            migration.tablet.getTableId().toString(), migration.newServer);
        if (toInfo != null) {
          toInfo.setOnlineTablets(toInfo.getOnlineTablets() + 1);
        }
      }
      migrations = EMPTY_MIGRATIONS;
    } else {
      migrationsFromLastPass.clear();
    }

    for (String s : tableIdMap.values()) {
      TableId tableId = TableId.of(s);
      String tableName = tableIdToTableName.get(tableId);
      String regexTableName = getPoolNameForTable(tableName);
      SortedMap<TServerInstance,TabletServerStatus> currentView = currentGrouped
          .get(regexTableName);
      if (currentView == null) {
        LOG.warn("Skipping balance for table {} as no tablet servers are online.", tableName);
        continue;
      }
      ArrayList<TabletMigration> newMigrations = new ArrayList<>();
      getBalancerForTable(tableId).balance(currentView, migrations, newMigrations);

      if (newMigrations.isEmpty()) {
        tableToTimeSinceNoMigrations.remove(s);
      } else if (tableToTimeSinceNoMigrations.containsKey(s)) {
        if ((now - tableToTimeSinceNoMigrations.get(s)) > ONE_HOUR) {
          LOG.warn("We have been consistently producing migrations for {}: {}", tableName,
              Iterables.limit(newMigrations, 10));
        }
      } else {
        tableToTimeSinceNoMigrations.put(s, now);
      }

      migrationsOut.addAll(newMigrations);
      if (migrationsOut.size() >= this.maxTServerMigrations) {
        break;
      }
    }

    for (TabletMigration migration : migrationsOut) {
      migrationsFromLastPass.put(migration.tablet, migration);
    }

    LOG.info("Migrating tablets for balance: {}", migrationsOut);
    return minBalanceTime;
  }

  /**
   * Get a mutable table info for the specified table and server
   */
  private TableInfo getTableInfo(SortedMap<TServerInstance,TabletServerStatus> currentCopy,
      Multimap<TServerInstance,String> serverTableIdCopied, String tableId,
      TServerInstance server) {
    TableInfo newInfo = null;
    if (currentCopy.containsKey(server)) {
      Map<String,TableInfo> newTableMap = currentCopy.get(server).getTableMap();
      if (newTableMap != null) {
        newInfo = newTableMap.get(tableId);
        if (newInfo != null) {
          Collection<String> tableIdCopied = serverTableIdCopied.get(server);
          if (tableIdCopied.isEmpty()) {
            newTableMap = new HashMap<>(newTableMap);
            currentCopy.get(server).setTableMap(newTableMap);
          }
          if (!tableIdCopied.contains(tableId)) {
            newInfo = new TableInfo(newInfo);
            newTableMap.put(tableId, newInfo);
            tableIdCopied.add(tableId);
          }
        }
      }
    }
    return newInfo;
  }

  @Override
  public void propertyChanged(String key) {
    parseConfiguration(context.getServerConfFactory());
  }

  @Override
  public void propertiesChanged() {
    parseConfiguration(context.getServerConfFactory());
  }

  @Override
  public void sessionExpired() {}

}
