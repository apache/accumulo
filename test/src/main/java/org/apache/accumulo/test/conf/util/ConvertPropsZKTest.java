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
package org.apache.accumulo.test.conf.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.harness.AccumuloITBase.ZOOKEEPER_TESTING_SERVER;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.PropCacheKey;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.impl.ZooPropStore;
import org.apache.accumulo.server.conf.util.ConfigConverter;
import org.apache.accumulo.test.zookeeper.ZooKeeperTestingServer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(ZOOKEEPER_TESTING_SERVER)
public class ConvertPropsZKTest {

  @TempDir
  private static File tempDir;
  private static final Logger log = LoggerFactory.getLogger(ConvertPropsZooKeeperIT.class);
  private static ZooKeeperTestingServer testZk = null;
  private static ZooKeeper zooKeeper;
  private static ZooReaderWriter zrw;
  private InstanceId instanceId = null;

  @BeforeAll
  public static void setupZk() {

    // using default zookeeper port - we don't have a full configuration
    testZk = new ZooKeeperTestingServer(tempDir);
    zooKeeper = testZk.getZooKeeper();
    zrw = testZk.getZooReaderWriter();
  }

  @AfterAll
  public static void shutdownZK() throws Exception {
    testZk.close();
  }

  /**
   * Generates a list of ZooKeeper nodes captured from a 1.10 instance. The list is ordered so that
   * parent nodes are created before the children. This is used for property conversion testing and
   * other values are irrelevant and left as empty ZooKeeper nodes.
   */
  private static List<NodeData> zooDump(final InstanceId instanceId) {
    String zkRoot = ZooUtil.getRoot(instanceId);
    List<NodeData> names = new ArrayList<>(250);

    names.add(new NodeData(Constants.ZROOT, null));
    names.add(new NodeData(ZooUtil.getRoot(instanceId), null));

    names.add(new NodeData(zkRoot + "/bulk_failed_copyq", null));
    names.add(new NodeData(zkRoot + "/config", null));
    names.add(new NodeData(zkRoot + "/dead", null));
    names.add(new NodeData(zkRoot + "/fate", null));
    names.add(new NodeData(zkRoot + "/gc", null));
    names.add(new NodeData(zkRoot + "/hdfs_reservations", null));
    names.add(new NodeData(zkRoot + "/masters", null));
    names.add(new NodeData(zkRoot + "/monitor", null));
    names.add(new NodeData(zkRoot + "/namespaces", null));
    names.add(new NodeData(zkRoot + "/next_file", null));
    names.add(new NodeData(zkRoot + "/problems", null));
    names.add(new NodeData(zkRoot + "/recovery", null));
    names.add(new NodeData(zkRoot + "/replication", null));
    names.add(new NodeData(zkRoot + "/root_tablet", null));
    names.add(new NodeData(zkRoot + "/table_locks", null));
    names.add(new NodeData(zkRoot + "/tables", null));
    names.add(new NodeData(zkRoot + "/tservers", null));
    names.add(new NodeData(zkRoot + "/users", null));
    names.add(new NodeData(zkRoot + "/wals", null));
    names.add(new NodeData(zkRoot + "/bulk_failed_copyq/locks", null));
    names.add(new NodeData(zkRoot + "/config/master.bulk.retries", "4"));
    names.add(new NodeData(zkRoot + "/config/master.bulk.timeout", "10m"));
    names.add(new NodeData(zkRoot + "/config/table.bloom.enabled", "true"));
    names.add(new NodeData(zkRoot + "/config/master.bulk.rename.threadpool.size", "10"));
    names.add(new NodeData(zkRoot + "/config/master.bulk.threadpool.size", "4"));
    names.add(new NodeData(zkRoot + "/dead/tservers", null));
    names.add(new NodeData(zkRoot + "/gc/lock", null));
    names.add(new NodeData(zkRoot + "/gc/lock/zlock-0000000000", null));
    names.add(new NodeData(zkRoot + "/masters/goal_state", null));
    names.add(new NodeData(zkRoot + "/masters/lock", null));
    names.add(new NodeData(zkRoot + "/masters/repl_coord_addr", null));
    names.add(new NodeData(zkRoot + "/masters/tick", null));
    names.add(new NodeData(zkRoot + "/masters/lock/zlock-0000000000", null));
    names.add(new NodeData(zkRoot + "/monitor/http_addr", null));
    names.add(new NodeData(zkRoot + "/monitor/lock", null));
    names.add(new NodeData(zkRoot + "/monitor/log4j_addr", null));
    names.add(new NodeData(zkRoot + "/monitor/lock/zlock-0000000000", null));
    names.add(new NodeData(zkRoot + "/namespaces/+accumulo", null));
    names.add(new NodeData(zkRoot + "/namespaces/+default", null));
    names.add(new NodeData(zkRoot + "/namespaces/2", null));
    names.add(new NodeData(zkRoot + "/namespaces/3", null));
    names.add(new NodeData(zkRoot + "/namespaces/+accumulo/conf", null));
    names.add(new NodeData(zkRoot + "/namespaces/+accumulo/name", null));
    names.add(new NodeData(zkRoot + "/namespaces/+default/conf", null));
    names.add(new NodeData(zkRoot + "/namespaces/+default/name", null));
    names.add(new NodeData(zkRoot + "/namespaces/2/conf", null));
    names.add(new NodeData(zkRoot + "/namespaces/2/name", null));
    names.add(new NodeData(zkRoot + "/namespaces/2/conf/table.bloom.enabled", "false"));
    names.add(new NodeData(zkRoot + "/namespaces/3/conf", null));
    names.add(new NodeData(zkRoot + "/namespaces/3/name", null));
    names.add(new NodeData(zkRoot + "/recovery/locks", null));
    names.add(new NodeData(zkRoot + "/replication/tservers", null));
    names.add(new NodeData(zkRoot + "/replication/workqueue", null));
    names.add(new NodeData(zkRoot + "/replication/tservers/localhost:11000", null));
    names.add(new NodeData(zkRoot + "/replication/workqueue/locks", null));
    names.add(new NodeData(zkRoot + "/root_tablet/current_logs", null));
    names.add(new NodeData(zkRoot + "/root_tablet/dir", null));
    names.add(new NodeData(zkRoot + "/root_tablet/lastlocation", null));
    names.add(new NodeData(zkRoot + "/root_tablet/location", null));
    names.add(new NodeData(zkRoot + "/root_tablet/walogs", null));
    names.add(new NodeData(zkRoot + "/tables/!0", null));
    names.add(new NodeData(zkRoot + "/tables/+r", null));
    names.add(new NodeData(zkRoot + "/tables/+rep", null));
    names.add(new NodeData(zkRoot + "/tables/1", null));
    names.add(new NodeData(zkRoot + "/tables/4", null));
    names.add(new NodeData(zkRoot + "/tables/5", null));
    names.add(new NodeData(zkRoot + "/tables/6", null));
    names.add(new NodeData(zkRoot + "/tables/7", null));
    names.add(new NodeData(zkRoot + "/tables/!0/compact-cancel-id", null));
    names.add(new NodeData(zkRoot + "/tables/!0/compact-id", null));
    names.add(new NodeData(zkRoot + "/tables/!0/conf", null));
    names.add(new NodeData(zkRoot + "/tables/!0/flush-id", null));
    names.add(new NodeData(zkRoot + "/tables/!0/name", null));
    names.add(new NodeData(zkRoot + "/tables/!0/namespace", null));
    names.add(new NodeData(zkRoot + "/tables/!0/state", null));
    names.add(new NodeData(zkRoot + "/tables/!0/conf/table.failures.ignore", "false"));
    names.add(new NodeData(zkRoot + "/tables/!0/conf/table.cache.index.enable", "true"));
    names.add(new NodeData(zkRoot + "/tables/!0/conf/table.group.server", "file,log,srv,future"));
    names.add(new NodeData(zkRoot + "/tables/!0/conf/table.iterator.scan.replcombiner.opt.columns",
        "stat"));
    names.add(new NodeData(zkRoot + "/tables/!0/conf/table.compaction.major.ratio", "1"));
    names.add(new NodeData(zkRoot + "/tables/!0/conf/table.file.replication", "5"));
    names.add(new NodeData(zkRoot + "/tables/!0/conf/table.iterator.majc.replcombiner",
        "9,org.apache.accumulo.server.replication.StatusCombiner"));
    names.add(new NodeData(zkRoot + "/tables/!0/conf/table.iterator.scan.replcombiner",
        "9,org.apache.accumulo.server.replication.StatusCombiner"));
    names.add(
        new NodeData(zkRoot + "/tables/!0/conf/table.iterator.scan.vers.opt.maxVersions", "1"));
    names.add(new NodeData(zkRoot + "/tables/!0/conf/table.iterator.minc.vers",
        "10,org.apache.accumulo.core.iterators.user.VersioningIterator"));
    names.add(
        new NodeData(zkRoot + "/tables/!0/conf/table.iterator.majc.vers.opt.maxVersions", "1"));
    names.add(new NodeData(zkRoot + "/tables/!0/conf/table.iterator.scan.vers",
        "10,org.apache.accumulo.core.iterators.user.VersioningIterator"));
    names.add(new NodeData(zkRoot + "/tables/!0/conf/table.group.tablet", "~tab,loc"));
    names.add(new NodeData(zkRoot + "/tables/!0/conf/table.groups.enabled", "tablet,server"));
    names.add(new NodeData(zkRoot + "/tables/!0/conf/table.iterator.majc.vers",
        "10,org.apache.accumulo.core.iterators.user.VersioningIterator"));
    names.add(new NodeData(zkRoot + "/tables/!0/conf/table.constraint.1",
        "org.apache.accumulo.server.constraints.MetadataConstraints"));
    names.add(new NodeData(zkRoot + "/tables/!0/conf/table.iterator.minc.replcombiner.opt.columns",
        "stat"));
    names.add(new NodeData(zkRoot + "/tables/!0/conf/table.split.threshold", "64M"));
    names.add(new NodeData(zkRoot + "/tables/!0/conf/table.iterator.majc.bulkLoadFilter",
        "20,org.apache.accumulo.server.iterators.MetadataBulkLoadFilter"));
    names.add(
        new NodeData(zkRoot + "/tables/!0/conf/table.iterator.minc.vers.opt.maxVersions", "1"));
    names.add(new NodeData(zkRoot + "/tables/!0/conf/table.iterator.minc.replcombiner",
        "9,org.apache.accumulo.server.replication.StatusCombiner"));
    names.add(new NodeData(zkRoot + "/tables/!0/conf/table.file.compress.blocksize", "32K"));
    names.add(new NodeData(zkRoot + "/tables/!0/conf/table.durability", "sync"));
    names
        .add(new NodeData(zkRoot + "/tables/!0/conf/table.security.scan.visibility.default", null));
    names.add(new NodeData(zkRoot + "/tables/!0/conf/table.cache.block.enable", "true"));
    names.add(new NodeData(zkRoot + "/tables/!0/conf/table.iterator.majc.replcombiner.opt.columns",
        "stat"));
    names.add(new NodeData(zkRoot + "/tables/+r/compact-cancel-id", null));
    names.add(new NodeData(zkRoot + "/tables/+r/compact-id", null));
    names.add(new NodeData(zkRoot + "/tables/+r/conf", null));
    names.add(new NodeData(zkRoot + "/tables/+r/flush-id", null));
    names.add(new NodeData(zkRoot + "/tables/+r/name", null));
    names.add(new NodeData(zkRoot + "/tables/+r/namespace", null));
    names.add(new NodeData(zkRoot + "/tables/+r/state", null));
    names.add(new NodeData(zkRoot + "/tables/+r/conf/table.group.tablet", "~tab,loc"));
    names.add(new NodeData(zkRoot + "/tables/+r/conf/table.groups.enabled", "tablet,server"));
    names.add(new NodeData(zkRoot + "/tables/+r/conf/table.iterator.majc.vers",
        "10,org.apache.accumulo.core.iterators.user.VersioningIterator"));
    names.add(new NodeData(zkRoot + "/tables/+r/conf/table.constraint.1",
        "org.apache.accumulo.server.constraints.MetadataConstraints"));
    names.add(new NodeData(zkRoot + "/tables/+r/conf/table.split.threshold", "64M"));
    names.add(new NodeData(zkRoot + "/tables/+r/conf/table.failures.ignore", "false"));
    names.add(new NodeData(zkRoot + "/tables/+r/conf/table.iterator.majc.bulkLoadFilter",
        "20,org.apache.accumulo.server.iterators.MetadataBulkLoadFilter"));
    names.add(new NodeData(zkRoot + "/tables/+r/conf/table.cache.index.enable", "true"));
    names.add(
        new NodeData(zkRoot + "/tables/+r/conf/table.iterator.minc.vers.opt.maxVersions", "1"));
    names.add(new NodeData(zkRoot + "/tables/+r/conf/table.group.server", "file,log,srv,future"));
    names.add(new NodeData(zkRoot + "/tables/+r/conf/table.file.compress.blocksize", "32K"));
    names.add(new NodeData(zkRoot + "/tables/+r/conf/table.durability", "sync"));
    names
        .add(new NodeData(zkRoot + "/tables/+r/conf/table.security.scan.visibility.default", null));
    names.add(new NodeData(zkRoot + "/tables/+r/conf/table.compaction.major.ratio", "1"));
    names.add(new NodeData(zkRoot + "/tables/+r/conf/table.file.replication", "5"));
    names.add(
        new NodeData(zkRoot + "/tables/+r/conf/table.iterator.scan.vers.opt.maxVersions", "1"));
    names.add(new NodeData(zkRoot + "/tables/+r/conf/table.iterator.minc.vers",
        "10,org.apache.accumulo.core.iterators.user.VersioningIterator"));
    names.add(
        new NodeData(zkRoot + "/tables/+r/conf/table.iterator.majc.vers.opt.maxVersions", "1"));
    names.add(new NodeData(zkRoot + "/tables/+r/conf/table.cache.block.enable", "true"));
    names.add(new NodeData(zkRoot + "/tables/+r/conf/table.iterator.scan.vers",
        "10,org.apache.accumulo.core.iterators.user.VersioningIterator"));
    names.add(new NodeData(zkRoot + "/tables/+rep/compact-cancel-id", null));
    names.add(new NodeData(zkRoot + "/tables/+rep/compact-id", null));
    names.add(new NodeData(zkRoot + "/tables/+rep/conf", null));
    names.add(new NodeData(zkRoot + "/tables/+rep/flush-id", null));
    names.add(new NodeData(zkRoot + "/tables/+rep/name", null));
    names.add(new NodeData(zkRoot + "/tables/+rep/namespace", null));
    names.add(new NodeData(zkRoot + "/tables/+rep/state", null));
    names.add(new NodeData(zkRoot + "/tables/+rep/conf/table.iterator.minc.statuscombiner",
        "30,org.apache.accumulo.server.replication.StatusCombiner"));
    names.add(new NodeData(zkRoot + "/tables/+rep/conf/table.groups.enabled", "repl,work"));
    names.add(new NodeData(
        zkRoot + "/tables/+rep/conf/table.iterator.majc.statuscombiner.opt.columns", "repl,work"));
    names.add(new NodeData(
        zkRoot + "/tables/+rep/conf/table.iterator.minc.statuscombiner.opt.columns", "repl,work"));
    names.add(new NodeData(zkRoot + "/tables/+rep/conf/table.group.work", "work"));
    names.add(new NodeData(
        zkRoot + "/tables/+rep/conf/table.iterator.scan.statuscombiner.opt.columns", "repl,work"));
    names.add(new NodeData(zkRoot + "/tables/+rep/conf/table.iterator.majc.statuscombiner",
        "30,org.apache.accumulo.server.replication.StatusCombiner"));
    names.add(new NodeData(zkRoot + "/tables/+rep/conf/table.group.repl", "repl"));
    names.add(new NodeData(zkRoot + "/tables/+rep/conf/table.iterator.scan.statuscombiner",
        "30,org.apache.accumulo.server.replication.StatusCombiner"));
    names.add(new NodeData(zkRoot + "/tables/+rep/conf/table.formatter",
        "org.apache.accumulo.server.replication.StatusFormatter"));
    names.add(new NodeData(zkRoot + "/tables/1/compact-cancel-id", null));
    names.add(new NodeData(zkRoot + "/tables/1/compact-id", null));
    names.add(new NodeData(zkRoot + "/tables/1/conf", null));
    names.add(new NodeData(zkRoot + "/tables/1/flush-id", null));
    names.add(new NodeData(zkRoot + "/tables/1/name", null));
    names.add(new NodeData(zkRoot + "/tables/1/namespace", null));
    names.add(new NodeData(zkRoot + "/tables/1/state", null));
    names.add(new NodeData(zkRoot + "/tables/1/conf/table.constraint.1",
        "org.apache.accumulo.core.constraints.DefaultKeySizeConstraint"));
    names.add(new NodeData(zkRoot + "/tables/1/conf/table.formatter",
        "org.apache.accumulo.tracer.TraceFormatter"));
    names.add(new NodeData(zkRoot + "/tables/1/conf/table.iterator.majc.ageoff",
        "10,org.apache.accumulo.core.iterators.user.AgeOffFilter"));
    names.add(
        new NodeData(zkRoot + "/tables/1/conf/table.iterator.majc.ageoff.opt.ttl", "604800000"));
    names.add(new NodeData(zkRoot + "/tables/1/conf/table.iterator.majc.vers",
        "20,org.apache.accumulo.core.iterators.user.VersioningIterator"));
    names
        .add(new NodeData(zkRoot + "/tables/1/conf/table.iterator.majc.vers.opt.maxVersions", "1"));
    names.add(new NodeData(zkRoot + "/tables/1/conf/table.iterator.minc.ageoff",
        "10,org.apache.accumulo.core.iterators.user.AgeOffFilter"));
    names.add(
        new NodeData(zkRoot + "/tables/1/conf/table.iterator.minc.ageoff.opt.ttl", "604800000"));
    names.add(new NodeData(zkRoot + "/tables/1/conf/table.iterator.minc.vers",
        "20,org.apache.accumulo.core.iterators.user.VersioningIterator"));
    names
        .add(new NodeData(zkRoot + "/tables/1/conf/table.iterator.minc.vers.opt.maxVersions", "1"));
    names.add(new NodeData(zkRoot + "/tables/1/conf/table.iterator.scan.ageoff",
        "10,org.apache.accumulo.core.iterators.user.AgeOffFilter"));
    names.add(
        new NodeData(zkRoot + "/tables/1/conf/table.iterator.scan.ageoff.opt.ttl", "604800000"));
    names.add(new NodeData(zkRoot + "/tables/1/conf/table.iterator.scan.vers",
        "20,org.apache.accumulo.core.iterators.user.VersioningIterator"));
    names
        .add(new NodeData(zkRoot + "/tables/1/conf/table.iterator.scan.vers.opt.maxVersions", "1"));
    names.add(new NodeData(zkRoot + "/tables/4/compact-cancel-id", null));
    names.add(new NodeData(zkRoot + "/tables/4/compact-id", null));
    names.add(new NodeData(zkRoot + "/tables/4/conf", null));
    names.add(new NodeData(zkRoot + "/tables/4/flush-id", null));
    names.add(new NodeData(zkRoot + "/tables/4/name", null));
    names.add(new NodeData(zkRoot + "/tables/4/namespace", null));
    names.add(new NodeData(zkRoot + "/tables/4/state", null));
    names.add(new NodeData(zkRoot + "/tables/4/conf/table.iterator.majc.vers",
        "20,org.apache.accumulo.core.iterators.user.VersioningIterator"));
    names.add(new NodeData(zkRoot + "/tables/4/conf/table.constraint.1",
        "org.apache.accumulo.core.constraints.DefaultKeySizeConstraint"));
    names
        .add(new NodeData(zkRoot + "/tables/4/conf/table.iterator.scan.vers.opt.maxVersions", "1"));
    names.add(new NodeData(zkRoot + "/tables/4/conf/table.iterator.minc.vers",
        "/20,org.apache.accumulo.core.iterators.user.VersioningIterator"));
    names
        .add(new NodeData(zkRoot + "/tables/4/conf/table.iterator.majc.vers.opt.maxVersions", "1"));
    names
        .add(new NodeData(zkRoot + "/tables/4/conf/table.iterator.minc.vers.opt.maxVersions", "1"));
    names.add(new NodeData(zkRoot + "/tables/4/conf/table.iterator.scan.vers",
        "20,org.apache.accumulo.core.iterators.user.VersioningIterator"));
    names.add(new NodeData(zkRoot + "/tables/4/conf/table.bloom.enabled", "true"));
    names.add(new NodeData(zkRoot + "/tables/5/compact-cancel-id", null));
    names.add(new NodeData(zkRoot + "/tables/5/compact-id", null));
    names.add(new NodeData(zkRoot + "/tables/5/conf", null));
    names.add(new NodeData(zkRoot + "/tables/5/flush-id", null));
    names.add(new NodeData(zkRoot + "/tables/5/name", null));
    names.add(new NodeData(zkRoot + "/tables/5/namespace", null));
    names.add(new NodeData(zkRoot + "/tables/5/state", null));
    names.add(new NodeData(zkRoot + "/tables/5/conf/table.constraint.1",
        "org.apache.accumulo.core.constraints.DefaultKeySizeConstraint"));
    names.add(new NodeData(zkRoot + "/tables/5/conf/table.iterator.majc.vers",
        "20,org.apache.accumulo.core.iterators.user.VersioningIterator"));
    names
        .add(new NodeData(zkRoot + "/tables/5/conf/table.iterator.majc.vers.opt.maxVersions", "1"));
    names.add(new NodeData(zkRoot + "/tables/5/conf/table.iterator.minc.vers",
        "20,org.apache.accumulo.core.iterators.user.VersioningIterator"));
    names
        .add(new NodeData(zkRoot + "/tables/5/conf/table.iterator.minc.vers.opt.maxVersions", "1"));
    names.add(new NodeData(zkRoot + "/tables/5/conf/table.iterator.scan.vers",
        "20,org.apache.accumulo.core.iterators.user.VersioningIterator"));
    names
        .add(new NodeData(zkRoot + "/tables/5/conf/table.iterator.scan.vers.opt.maxVersions", "1"));
    names.add(new NodeData(zkRoot + "/tables/6/compact-cancel-id", null));
    names.add(new NodeData(zkRoot + "/tables/6/compact-id", null));
    names.add(new NodeData(zkRoot + "/tables/6/conf", null));
    names.add(new NodeData(zkRoot + "/tables/6/flush-id", null));
    names.add(new NodeData(zkRoot + "/tables/6/name", null));
    names.add(new NodeData(zkRoot + "/tables/6/namespace", null));
    names.add(new NodeData(zkRoot + "/tables/6/state", null));
    names.add(new NodeData(zkRoot + "/tables/6/conf/table.bloom.enabled", "true"));
    names.add(new NodeData(zkRoot + "/tables/6/conf/table.constraint.1",
        "org.apache.accumulo.core.constraints.DefaultKeySizeConstraint"));
    names.add(new NodeData(zkRoot + "/tables/6/conf/table.iterator.majc.vers",
        "20,org.apache.accumulo.core.iterators.user.VersioningIterator"));
    names
        .add(new NodeData(zkRoot + "/tables/6/conf/table.iterator.majc.vers.opt.maxVersions", "1"));
    names.add(new NodeData(zkRoot + "/tables/6/conf/table.iterator.minc.vers",
        "20,org.apache.accumulo.core.iterators.user.VersioningIterator"));
    names
        .add(new NodeData(zkRoot + "/tables/6/conf/table.iterator.minc.vers.opt.maxVersions", "1"));
    names.add(new NodeData(zkRoot + "/tables/6/conf/table.iterator.scan.vers",
        "20,org.apache.accumulo.core.iterators.user.VersioningIterator"));
    names
        .add(new NodeData(zkRoot + "/tables/6/conf/table.iterator.scan.vers.opt.maxVersions", "1"));
    names.add(new NodeData(zkRoot + "/tables/7/compact-cancel-id", null));
    names.add(new NodeData(zkRoot + "/tables/7/compact-id", null));
    names.add(new NodeData(zkRoot + "/tables/7/conf", null));
    names.add(new NodeData(zkRoot + "/tables/7/flush-id", null));
    names.add(new NodeData(zkRoot + "/tables/7/name", null));
    names.add(new NodeData(zkRoot + "/tables/7/namespace", null));
    names.add(new NodeData(zkRoot + "/tables/7/state", null));
    names.add(new NodeData(zkRoot + "/tables/7/conf/table.bloom.enabled", null));
    names.add(new NodeData(zkRoot + "/tables/7/conf/table.constraint.1", null));
    names.add(new NodeData(zkRoot + "/tables/7/conf/table.iterator.majc.vers", null));
    names.add(
        new NodeData(zkRoot + "/tables/7/conf/table.iterator.majc.vers.opt.maxVersions", null));
    names.add(new NodeData(zkRoot + "/tables/7/conf/table.iterator.minc.vers", null));
    names.add(
        new NodeData(zkRoot + "/tables/7/conf/table.iterator.minc.vers.opt.maxVersions", null));
    names.add(new NodeData(zkRoot + "/tables/7/conf/table.iterator.scan.vers", null));
    names.add(
        new NodeData(zkRoot + "/tables/7/conf/table.iterator.scan.vers.opt.maxVersions", null));
    names.add(new NodeData(zkRoot + "/tservers/localhost:11000", null));
    names.add(new NodeData(zkRoot + "/tservers/localhost:11000/zlock-0000000000", null));
    names.add(new NodeData(zkRoot + "/users/root", null));
    names.add(new NodeData(zkRoot + "/users/root/Authorizations", null));
    names.add(new NodeData(zkRoot + "/users/root/Namespaces", null));
    names.add(new NodeData(zkRoot + "/users/root/System", null));
    names.add(new NodeData(zkRoot + "/users/root/Tables", null));
    names.add(new NodeData(zkRoot + "/users/root/Namespaces/+accumulo", null));
    names.add(new NodeData(zkRoot + "/users/root/Namespaces/2", null));
    names.add(new NodeData(zkRoot + "/users/root/Namespaces/3", null));
    names.add(new NodeData(zkRoot + "/users/root/Tables/!0", null));
    names.add(new NodeData(zkRoot + "/users/root/Tables/+r", null));
    names.add(new NodeData(zkRoot + "/users/root/Tables/1", null));
    names.add(new NodeData(zkRoot + "/users/root/Tables/4", null));
    names.add(new NodeData(zkRoot + "/users/root/Tables/5", null));
    names.add(new NodeData(zkRoot + "/users/root/Tables/6", null));
    names.add(new NodeData(zkRoot + "/users/root/Tables/7", null));
    names.add(new NodeData(zkRoot + "/wals/localhost:11000[10000c3202e0003]", null));
    names.add(new NodeData(
        zkRoot + "/wals/localhost:11000[10000c3202e0003]/0fef8f3b-d02d-413b-9a27-f1710812b216",
        null));
    names.add(new NodeData(
        zkRoot + "/wals/localhost:11000[10000c3202e0003]/9e410484-e61b-4707-847a-67f96715aa04",
        null));

    return names;
  }

  @BeforeEach
  public void testSetup() {
    instanceId = InstanceId.of(UUID.randomUUID());
  }

  @AfterEach
  public void cleanupZnodes() {
    try {
      ZKUtil.deleteRecursive(zooKeeper, Constants.ZROOT);
    } catch (KeeperException | InterruptedException ex) {
      throw new IllegalStateException("Failed to clean-up test zooKeeper nodes.", ex);
    }
  }

  @Test
  public void init() throws Exception {

    List<NodeData> nodes = zooDump(instanceId);
    for (NodeData node : nodes) {
      zrw.putPersistentData(node.getPath(), node.getData(), ZooUtil.NodeExistsPolicy.SKIP);
    }

    PropStore propStore = new ZooPropStore.Builder(instanceId, zrw, 30_000).build();

    ServerContext context = createMock(ServerContext.class);
    expect(context.getInstanceID()).andReturn(instanceId).anyTimes();
    expect(context.getZooReaderWriter()).andReturn(zrw).anyTimes();
    expect(context.getPropStore()).andReturn(propStore).anyTimes();

    replay(context);

    ConfigConverter.convert(context, true);
    var sysPropKey = propStore.get(PropCacheKey.forSystem(instanceId));

    log.info("SYS: {}", sysPropKey);

    // read converted
    String zkPathTableBase = ZooUtil.getRoot(instanceId) + Constants.ZTABLES;

    List<String> tables = zrw.getChildren(zkPathTableBase);
    for (String table : tables) {
      var vp = propStore.get(PropCacheKey.forTable(instanceId, TableId.of(table)));
      log.info("table:{} props: {}", table, vp);
      assertNotNull(vp);

      String confPath = zkPathTableBase + "/" + table + "/conf";

      List<String> tableChildren = zrw.getChildren(confPath);
      log.info("conf: {} - {}", confPath, tableChildren);

      assertEquals(1, tableChildren.size());
      assertEquals("encoded_props", tableChildren.get(0));
    }

  }

  private static class NodeData {
    private static final byte[] empty = new byte[0];
    private final String path;
    private final String value;

    public NodeData(final String path, final String value) {
      this.path = path;
      this.value = value;
    }

    public String getPath() {
      return path;
    }

    public byte[] getData() {
      if (value == null) {
        return empty;
      }
      return value.getBytes(UTF_8);
    }
  }

}
/*
 * manager - deprecated defaults: default | master.bulk.rename.threadpool.size .. | 20 default |
 * master.bulk.retries ................. | 3 default | master.bulk.threadpool.size ......... | 5
 * default | master.bulk.timeout ................. | 5m
 */
