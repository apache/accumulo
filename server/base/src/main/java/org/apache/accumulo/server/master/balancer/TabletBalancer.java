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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService.Client;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.core.trace.Tracer;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TabletMigration;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

/**
 * This class is responsible for managing the distribution of tablets throughout an Accumulo cluster. In most cases, users will want a balancer implementation
 * which ensures a uniform distribution of tablets, so that no individual tablet server is handling significantly more work than any other.
 *
 * <p>
 * Implementations may wish to store configuration in Accumulo's system configuration using the {@link Property#GENERAL_ARBITRARY_PROP_PREFIX}. They may also
 * benefit from using per-table configuration using {@link Property#TABLE_ARBITRARY_PROP_PREFIX}.
 */
public abstract class TabletBalancer {

  private static final Logger log = LoggerFactory.getLogger(TabletBalancer.class);

  protected AccumuloServerContext context;

  /**
   * Initialize the TabletBalancer. This gives the balancer the opportunity to read the configuration.
   *
   * @deprecated since 2.0.0; use {@link #init(AccumuloServerContext)} instead.
   */
  @Deprecated
  public void init(ServerConfigurationFactory conf) {
    init(new AccumuloServerContext(HdfsZooInstance.getInstance(), conf));
  }

  /**
   * Initialize the TabletBalancer. This gives the balancer the opportunity to read the configuration.
   *
   * @since 2.0.0
   */
  public void init(AccumuloServerContext context) {
    this.context = context;
  }

  /**
   * Assign tablets to tablet servers. This method is called whenever the master finds tablets that are unassigned.
   *
   * @param current
   *          The current table-summary state of all the online tablet servers. Read-only. The TabletServerStatus for each server may be null if the tablet
   *          server has not yet responded to a recent request for status.
   * @param unassigned
   *          A map from unassigned tablet to the last known tablet server. Read-only.
   * @param assignments
   *          A map from tablet to assigned server. Write-only.
   */
  abstract public void getAssignments(SortedMap<TServerInstance,TabletServerStatus> current, Map<KeyExtent,TServerInstance> unassigned,
      Map<KeyExtent,TServerInstance> assignments);

  /**
   * Ask the balancer if any migrations are necessary.
   *
   * If the balancer is going to self-abort due to some environmental constraint (e.g. it requires some minimum number of tservers, or a maximum number of
   * outstanding migrations), it should issue a log message to alert operators. The message should be at WARN normally and at ERROR if the balancer knows that
   * the problem can not self correct. It should not issue these messages more than once a minute.
   *
   * @param current
   *          The current table-summary state of all the online tablet servers. Read-only.
   * @param migrations
   *          the current set of migrations. Read-only.
   * @param migrationsOut
   *          new migrations to perform; should not contain tablets in the current set of migrations. Write-only.
   * @return the time, in milliseconds, to wait before re-balancing.
   *
   *         This method will not be called when there are unassigned tablets.
   */
  public abstract long balance(SortedMap<TServerInstance,TabletServerStatus> current, Set<KeyExtent> migrations, List<TabletMigration> migrationsOut);

  private static final long ONE_SECOND = 1000l;
  private boolean stuck = false;
  private long stuckNotificationTime = -1l;

  protected static final long TIME_BETWEEN_BALANCER_WARNINGS = 60 * ONE_SECOND;

  /**
   * A deferred call descendent TabletBalancers use to log why they can't continue. The call is deferred so that TabletBalancer can limit how often messages
   * happen.
   *
   * Implementations should be reused as much as possible.
   *
   * Be sure to pass in a properly scoped Logger instance so that messages indicate what part of the system is having trouble.
   */
  protected static abstract class BalancerProblem implements Runnable {
    protected final Logger balancerLog;

    public BalancerProblem(Logger logger) {
      balancerLog = logger;
    }
  }

  /**
   * If a TabletBalancer requires active tservers, it should use this problem to indicate when there are none. NoTservers is safe to share with anyone who uses
   * the same Logger. TabletBalancers should have a single static instance.
   */
  protected static class NoTservers extends BalancerProblem {
    public NoTservers(Logger logger) {
      super(logger);
    }

    @Override
    public void run() {
      balancerLog.warn("Not balancing because we don't have any tservers");
    }
  }

  /**
   * If a TabletBalancer only balances when there are no outstanding migrations, it should use this problem to indicate when they exist.
   *
   * Iff a TabletBalancer makes use of the migrations member to provide samples, then OutstandingMigrations is not thread safe.
   */
  protected static class OutstandingMigrations extends BalancerProblem {
    public Set<KeyExtent> migrations = Collections.<KeyExtent> emptySet();

    public OutstandingMigrations(Logger logger) {
      super(logger);
    }

    @Override
    public void run() {
      balancerLog.warn("Not balancing due to {} outstanding migrations.", migrations.size());
      /* TODO ACCUMULO-2938 redact key extents in this output to avoid leaking protected information. */
      balancerLog.debug("Sample up to 10 outstanding migrations: {}", Iterables.limit(migrations, 10));
    }
  }

  /**
   * Warn that a Balancer can't work because of some external restriction. Will not call the provided logging handler more often than
   * TIME_BETWEEN_BALANCER_WARNINGS
   */
  protected void constraintNotMet(BalancerProblem cause) {
    if (!stuck) {
      stuck = true;
      stuckNotificationTime = System.currentTimeMillis();
    } else {
      if ((System.currentTimeMillis() - stuckNotificationTime) > TIME_BETWEEN_BALANCER_WARNINGS) {
        cause.run();
        stuckNotificationTime = System.currentTimeMillis();
      }
    }
  }

  /**
   * Resets logging about problems meeting an external constraint on balancing.
   */
  protected void resetBalancerErrors() {
    stuck = false;
  }

  /**
   * Fetch the tablets for the given table by asking the tablet server. Useful if your balance strategy needs details at the tablet level to decide what tablets
   * to move.
   *
   * @param tserver
   *          The tablet server to ask.
   * @param tableId
   *          The table id
   * @return a list of tablet statistics
   * @throws ThriftSecurityException
   *           tablet server disapproves of your internal System password.
   * @throws TException
   *           any other problem
   */
  public List<TabletStats> getOnlineTabletsForTable(TServerInstance tserver, Table.ID tableId) throws ThriftSecurityException, TException {
    log.debug("Scanning tablet server {} for table {}", tserver, tableId);
    Client client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), tserver.getLocation(), context);
    try {
      return client.getTabletStats(Tracer.traceInfo(), context.rpcCreds(), tableId.canonicalID());
    } catch (TTransportException e) {
      log.error("Unable to connect to {}: ", tserver, e);
    } finally {
      ThriftUtil.returnClient(client);
    }
    return null;
  }

  /**
   * Utility to ensure that the migrations from balance() are consistent:
   * <ul>
   * <li>Tablet objects are not null
   * <li>Source and destination tablet servers are not null and current
   * </ul>
   *
   * @return A list of TabletMigration object that passed sanity checks.
   */
  public static List<TabletMigration> checkMigrationSanity(Set<TServerInstance> current, List<TabletMigration> migrations) {
    List<TabletMigration> result = new ArrayList<>(migrations.size());
    for (TabletMigration m : migrations) {
      if (m.tablet == null) {
        log.warn("Balancer gave back a null tablet {}", m);
        continue;
      }
      if (m.newServer == null) {
        log.warn("Balancer did not set the destination {}", m);
        continue;
      }
      if (m.oldServer == null) {
        log.warn("Balancer did not set the source {}", m);
        continue;
      }
      if (!current.contains(m.oldServer)) {
        log.warn("Balancer wants to move a tablet from a server that is not current: {}", m);
        continue;
      }
      if (!current.contains(m.newServer)) {
        log.warn("Balancer wants to move a tablet to a server that is not current: {}", m);
        continue;
      }
      result.add(m);
    }
    return result;
  }

}
