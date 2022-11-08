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
package org.apache.accumulo.test.conf.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;

/**
 * Provides sampled ZooKeeper properties paths / data from a 1.10 instance for testing.
 */
public class LegacyPropData {

  /**
   * Generates a list of ZooKeeper nodes captured from a 1.10 instance. The list is ordered so that
   * parent nodes are created before the children. This is used for property conversion testing and
   * other values are irrelevant and left as empty ZooKeeper nodes.
   */
  public static List<PropNode> getData(final InstanceId instanceId) {
    String zkRoot = ZooUtil.getRoot(instanceId);
    List<PropNode> names = new ArrayList<>(250);

    names.add(new PropNode(Constants.ZROOT, null));
    names.add(new PropNode(ZooUtil.getRoot(instanceId), null));

    names.add(new PropNode(zkRoot + "/bulk_failed_copyq", null));
    names.add(new PropNode(zkRoot + "/config", null));
    names.add(new PropNode(zkRoot + "/dead", null));
    names.add(new PropNode(zkRoot + "/fate", null));
    names.add(new PropNode(zkRoot + "/gc", null));
    names.add(new PropNode(zkRoot + "/hdfs_reservations", null));
    names.add(new PropNode(zkRoot + "/masters", null));
    names.add(new PropNode(zkRoot + "/monitor", null));
    names.add(new PropNode(zkRoot + "/namespaces", null));
    names.add(new PropNode(zkRoot + "/next_file", null));
    names.add(new PropNode(zkRoot + "/problems", null));
    names.add(new PropNode(zkRoot + "/recovery", null));
    names.add(new PropNode(zkRoot + "/replication", null));
    names.add(new PropNode(zkRoot + "/root_tablet", null));
    names.add(new PropNode(zkRoot + "/table_locks", null));
    names.add(new PropNode(zkRoot + "/tables", null));
    names.add(new PropNode(zkRoot + "/tservers", null));
    names.add(new PropNode(zkRoot + "/users", null));
    names.add(new PropNode(zkRoot + "/wals", null));
    names.add(new PropNode(zkRoot + "/bulk_failed_copyq/locks", null));
    names.add(new PropNode(zkRoot + "/config/master.bulk.retries", "4"));
    names.add(new PropNode(zkRoot + "/config/master.bulk.timeout", "10m"));
    names.add(new PropNode(zkRoot + "/config/table.bloom.enabled", "true"));
    names.add(new PropNode(zkRoot + "/config/master.bulk.rename.threadpool.size", "10"));
    names.add(new PropNode(zkRoot + "/config/master.bulk.threadpool.size", "4"));
    names.add(new PropNode(zkRoot + "/dead/tservers", null));
    names.add(new PropNode(zkRoot + "/gc/lock", null));
    names.add(new PropNode(zkRoot + "/gc/lock/zlock-0000000000", null));
    names.add(new PropNode(zkRoot + "/masters/goal_state", null));
    names.add(new PropNode(zkRoot + "/masters/lock", null));
    names.add(new PropNode(zkRoot + "/masters/repl_coord_addr", null));
    names.add(new PropNode(zkRoot + "/masters/tick", null));
    names.add(new PropNode(zkRoot + "/masters/lock/zlock-0000000000", null));
    names.add(new PropNode(zkRoot + "/monitor/http_addr", null));
    names.add(new PropNode(zkRoot + "/monitor/lock", null));
    names.add(new PropNode(zkRoot + "/monitor/log4j_addr", null));
    names.add(new PropNode(zkRoot + "/monitor/lock/zlock-0000000000", null));
    names.add(new PropNode(zkRoot + "/namespaces/+accumulo", null));
    names.add(new PropNode(zkRoot + "/namespaces/+default", null));
    names.add(new PropNode(zkRoot + "/namespaces/2", null));
    names.add(new PropNode(zkRoot + "/namespaces/3", null));
    names.add(new PropNode(zkRoot + "/namespaces/+accumulo/conf", null));
    names.add(new PropNode(zkRoot + "/namespaces/+accumulo/name", "accumulo"));
    names.add(new PropNode(zkRoot + "/namespaces/+default/conf", null));
    names.add(new PropNode(zkRoot + "/namespaces/+default/name", null));
    names.add(new PropNode(zkRoot + "/namespaces/2/conf", null));
    names.add(new PropNode(zkRoot + "/namespaces/2/name", "ns1"));
    names.add(new PropNode(zkRoot + "/namespaces/2/conf/table.bloom.enabled", "false"));
    names.add(new PropNode(zkRoot + "/namespaces/3/conf", null));
    names.add(new PropNode(zkRoot + "/namespaces/3/name", "ns2"));
    names.add(new PropNode(zkRoot + "/recovery/locks", null));
    names.add(new PropNode(zkRoot + "/replication/tservers", null));
    names.add(new PropNode(zkRoot + "/replication/workqueue", null));
    names.add(new PropNode(zkRoot + "/replication/tservers/localhost:11000", null));
    names.add(new PropNode(zkRoot + "/replication/workqueue/locks", null));
    names.add(new PropNode(zkRoot + "/root_tablet/current_logs", null));
    names.add(new PropNode(zkRoot + "/root_tablet/dir", null));
    names.add(new PropNode(zkRoot + "/root_tablet/lastlocation", null));
    names.add(new PropNode(zkRoot + "/root_tablet/location", null));
    names.add(new PropNode(zkRoot + "/root_tablet/walogs", null));
    names.add(new PropNode(zkRoot + "/tables/!0", null));
    names.add(new PropNode(zkRoot + "/tables/+r", null));
    names.add(new PropNode(zkRoot + "/tables/+rep", null));
    names.add(new PropNode(zkRoot + "/tables/1", null));
    names.add(new PropNode(zkRoot + "/tables/4", null));
    names.add(new PropNode(zkRoot + "/tables/5", null));
    names.add(new PropNode(zkRoot + "/tables/6", null));
    names.add(new PropNode(zkRoot + "/tables/7", null));
    names.add(new PropNode(zkRoot + "/tables/!0/compact-cancel-id", null));
    names.add(new PropNode(zkRoot + "/tables/!0/compact-id", null));
    names.add(new PropNode(zkRoot + "/tables/!0/conf", null));
    names.add(new PropNode(zkRoot + "/tables/!0/flush-id", null));
    names.add(new PropNode(zkRoot + "/tables/!0/name", "metadata"));
    names.add(new PropNode(zkRoot + "/tables/!0/namespace", "+accumulo"));
    names.add(new PropNode(zkRoot + "/tables/!0/state", null));
    names.add(new PropNode(zkRoot + "/tables/!0/conf/table.failures.ignore", "false"));
    names.add(new PropNode(zkRoot + "/tables/!0/conf/table.cache.index.enable", "true"));
    names.add(new PropNode(zkRoot + "/tables/!0/conf/table.group.server", "file,log,srv,future"));
    names.add(new PropNode(zkRoot + "/tables/!0/conf/table.iterator.scan.replcombiner.opt.columns",
        "stat"));
    names.add(new PropNode(zkRoot + "/tables/!0/conf/table.compaction.major.ratio", "1"));
    names.add(new PropNode(zkRoot + "/tables/!0/conf/table.file.replication", "5"));
    names.add(new PropNode(zkRoot + "/tables/!0/conf/table.iterator.majc.replcombiner",
        "9,org.apache.accumulo.server.replication.StatusCombiner"));
    names.add(new PropNode(zkRoot + "/tables/!0/conf/table.iterator.scan.replcombiner",
        "9,org.apache.accumulo.server.replication.StatusCombiner"));
    names.add(
        new PropNode(zkRoot + "/tables/!0/conf/table.iterator.scan.vers.opt.maxVersions", "1"));
    names.add(new PropNode(zkRoot + "/tables/!0/conf/table.iterator.minc.vers",
        "10,org.apache.accumulo.core.iterators.user.VersioningIterator"));
    names.add(
        new PropNode(zkRoot + "/tables/!0/conf/table.iterator.majc.vers.opt.maxVersions", "1"));
    names.add(new PropNode(zkRoot + "/tables/!0/conf/table.iterator.scan.vers",
        "10,org.apache.accumulo.core.iterators.user.VersioningIterator"));
    names.add(new PropNode(zkRoot + "/tables/!0/conf/table.group.tablet", "~tab,loc"));
    names.add(new PropNode(zkRoot + "/tables/!0/conf/table.groups.enabled", "tablet,server"));
    names.add(new PropNode(zkRoot + "/tables/!0/conf/table.iterator.majc.vers",
        "10,org.apache.accumulo.core.iterators.user.VersioningIterator"));
    names.add(new PropNode(zkRoot + "/tables/!0/conf/table.constraint.1",
        "org.apache.accumulo.server.constraints.MetadataConstraints"));
    names.add(new PropNode(zkRoot + "/tables/!0/conf/table.iterator.minc.replcombiner.opt.columns",
        "stat"));
    names.add(new PropNode(zkRoot + "/tables/!0/conf/table.split.threshold", "64M"));
    names.add(new PropNode(zkRoot + "/tables/!0/conf/table.iterator.majc.bulkLoadFilter",
        "20,org.apache.accumulo.server.iterators.MetadataBulkLoadFilter"));
    names.add(
        new PropNode(zkRoot + "/tables/!0/conf/table.iterator.minc.vers.opt.maxVersions", "1"));
    names.add(new PropNode(zkRoot + "/tables/!0/conf/table.iterator.minc.replcombiner",
        "9,org.apache.accumulo.server.replication.StatusCombiner"));
    names.add(new PropNode(zkRoot + "/tables/!0/conf/table.file.compress.blocksize", "32K"));
    names.add(new PropNode(zkRoot + "/tables/!0/conf/table.durability", "sync"));
    names
        .add(new PropNode(zkRoot + "/tables/!0/conf/table.security.scan.visibility.default", null));
    names.add(new PropNode(zkRoot + "/tables/!0/conf/table.cache.block.enable", "true"));
    names.add(new PropNode(zkRoot + "/tables/!0/conf/table.iterator.majc.replcombiner.opt.columns",
        "stat"));
    names.add(new PropNode(zkRoot + "/tables/+r/compact-cancel-id", null));
    names.add(new PropNode(zkRoot + "/tables/+r/compact-id", null));
    names.add(new PropNode(zkRoot + "/tables/+r/conf", null));
    names.add(new PropNode(zkRoot + "/tables/+r/flush-id", null));
    names.add(new PropNode(zkRoot + "/tables/+r/name", "root"));
    names.add(new PropNode(zkRoot + "/tables/+r/namespace", "+accumulo"));
    names.add(new PropNode(zkRoot + "/tables/+r/state", null));
    names.add(new PropNode(zkRoot + "/tables/+r/conf/table.group.tablet", "~tab,loc"));
    names.add(new PropNode(zkRoot + "/tables/+r/conf/table.groups.enabled", "tablet,server"));
    names.add(new PropNode(zkRoot + "/tables/+r/conf/table.iterator.majc.vers",
        "10,org.apache.accumulo.core.iterators.user.VersioningIterator"));
    names.add(new PropNode(zkRoot + "/tables/+r/conf/table.constraint.1",
        "org.apache.accumulo.server.constraints.MetadataConstraints"));
    names.add(new PropNode(zkRoot + "/tables/+r/conf/table.split.threshold", "64M"));
    names.add(new PropNode(zkRoot + "/tables/+r/conf/table.failures.ignore", "false"));
    names.add(new PropNode(zkRoot + "/tables/+r/conf/table.iterator.majc.bulkLoadFilter",
        "20,org.apache.accumulo.server.iterators.MetadataBulkLoadFilter"));
    names.add(new PropNode(zkRoot + "/tables/+r/conf/table.cache.index.enable", "true"));
    names.add(
        new PropNode(zkRoot + "/tables/+r/conf/table.iterator.minc.vers.opt.maxVersions", "1"));
    names.add(new PropNode(zkRoot + "/tables/+r/conf/table.group.server", "file,log,srv,future"));
    names.add(new PropNode(zkRoot + "/tables/+r/conf/table.file.compress.blocksize", "32K"));
    names.add(new PropNode(zkRoot + "/tables/+r/conf/table.durability", "sync"));
    names
        .add(new PropNode(zkRoot + "/tables/+r/conf/table.security.scan.visibility.default", null));
    names.add(new PropNode(zkRoot + "/tables/+r/conf/table.compaction.major.ratio", "1"));
    names.add(new PropNode(zkRoot + "/tables/+r/conf/table.file.replication", "5"));
    names.add(
        new PropNode(zkRoot + "/tables/+r/conf/table.iterator.scan.vers.opt.maxVersions", "1"));
    names.add(new PropNode(zkRoot + "/tables/+r/conf/table.iterator.minc.vers",
        "10,org.apache.accumulo.core.iterators.user.VersioningIterator"));
    names.add(
        new PropNode(zkRoot + "/tables/+r/conf/table.iterator.majc.vers.opt.maxVersions", "1"));
    names.add(new PropNode(zkRoot + "/tables/+r/conf/table.cache.block.enable", "true"));
    names.add(new PropNode(zkRoot + "/tables/+r/conf/table.iterator.scan.vers",
        "10,org.apache.accumulo.core.iterators.user.VersioningIterator"));
    names.add(new PropNode(zkRoot + "/tables/+rep/compact-cancel-id", null));
    names.add(new PropNode(zkRoot + "/tables/+rep/compact-id", null));
    names.add(new PropNode(zkRoot + "/tables/+rep/conf", null));
    names.add(new PropNode(zkRoot + "/tables/+rep/flush-id", null));
    names.add(new PropNode(zkRoot + "/tables/+rep/name", null));
    names.add(new PropNode(zkRoot + "/tables/+rep/namespace", null));
    names.add(new PropNode(zkRoot + "/tables/+rep/state", null));
    names.add(new PropNode(zkRoot + "/tables/+rep/conf/table.iterator.minc.statuscombiner",
        "30,org.apache.accumulo.server.replication.StatusCombiner"));
    names.add(new PropNode(zkRoot + "/tables/+rep/conf/table.groups.enabled", "repl,work"));
    names.add(new PropNode(
        zkRoot + "/tables/+rep/conf/table.iterator.majc.statuscombiner.opt.columns", "repl,work"));
    names.add(new PropNode(
        zkRoot + "/tables/+rep/conf/table.iterator.minc.statuscombiner.opt.columns", "repl,work"));
    names.add(new PropNode(zkRoot + "/tables/+rep/conf/table.group.work", "work"));
    names.add(new PropNode(
        zkRoot + "/tables/+rep/conf/table.iterator.scan.statuscombiner.opt.columns", "repl,work"));
    names.add(new PropNode(zkRoot + "/tables/+rep/conf/table.iterator.majc.statuscombiner",
        "30,org.apache.accumulo.server.replication.StatusCombiner"));
    names.add(new PropNode(zkRoot + "/tables/+rep/conf/table.group.repl", "repl"));
    names.add(new PropNode(zkRoot + "/tables/+rep/conf/table.iterator.scan.statuscombiner",
        "30,org.apache.accumulo.server.replication.StatusCombiner"));
    names.add(new PropNode(zkRoot + "/tables/+rep/conf/table.formatter",
        "org.apache.accumulo.server.replication.StatusFormatter"));
    names.add(new PropNode(zkRoot + "/tables/1/compact-cancel-id", null));
    names.add(new PropNode(zkRoot + "/tables/1/compact-id", null));
    names.add(new PropNode(zkRoot + "/tables/1/conf", null));
    names.add(new PropNode(zkRoot + "/tables/1/flush-id", null));
    names.add(new PropNode(zkRoot + "/tables/1/name", "trace"));
    names.add(new PropNode(zkRoot + "/tables/1/namespace", "+default"));
    names.add(new PropNode(zkRoot + "/tables/1/state", null));
    names.add(new PropNode(zkRoot + "/tables/1/conf/table.constraint.1",
        "org.apache.accumulo.core.constraints.DefaultKeySizeConstraint"));
    names.add(new PropNode(zkRoot + "/tables/1/conf/table.formatter",
        "org.apache.accumulo.tracer.TraceFormatter"));
    names.add(new PropNode(zkRoot + "/tables/1/conf/table.iterator.majc.ageoff",
        "10,org.apache.accumulo.core.iterators.user.AgeOffFilter"));
    names.add(
        new PropNode(zkRoot + "/tables/1/conf/table.iterator.majc.ageoff.opt.ttl", "604800000"));
    names.add(new PropNode(zkRoot + "/tables/1/conf/table.iterator.majc.vers",
        "20,org.apache.accumulo.core.iterators.user.VersioningIterator"));
    names
        .add(new PropNode(zkRoot + "/tables/1/conf/table.iterator.majc.vers.opt.maxVersions", "1"));
    names.add(new PropNode(zkRoot + "/tables/1/conf/table.iterator.minc.ageoff",
        "10,org.apache.accumulo.core.iterators.user.AgeOffFilter"));
    names.add(
        new PropNode(zkRoot + "/tables/1/conf/table.iterator.minc.ageoff.opt.ttl", "604800000"));
    names.add(new PropNode(zkRoot + "/tables/1/conf/table.iterator.minc.vers",
        "20,org.apache.accumulo.core.iterators.user.VersioningIterator"));
    names
        .add(new PropNode(zkRoot + "/tables/1/conf/table.iterator.minc.vers.opt.maxVersions", "1"));
    names.add(new PropNode(zkRoot + "/tables/1/conf/table.iterator.scan.ageoff",
        "10,org.apache.accumulo.core.iterators.user.AgeOffFilter"));
    names.add(
        new PropNode(zkRoot + "/tables/1/conf/table.iterator.scan.ageoff.opt.ttl", "604800000"));
    names.add(new PropNode(zkRoot + "/tables/1/conf/table.iterator.scan.vers",
        "20,org.apache.accumulo.core.iterators.user.VersioningIterator"));
    names
        .add(new PropNode(zkRoot + "/tables/1/conf/table.iterator.scan.vers.opt.maxVersions", "1"));
    names.add(new PropNode(zkRoot + "/tables/4/compact-cancel-id", null));
    names.add(new PropNode(zkRoot + "/tables/4/compact-id", null));
    names.add(new PropNode(zkRoot + "/tables/4/conf", null));
    names.add(new PropNode(zkRoot + "/tables/4/flush-id", null));
    names.add(new PropNode(zkRoot + "/tables/4/name", "tbl1"));
    names.add(new PropNode(zkRoot + "/tables/4/namespace", "2"));
    names.add(new PropNode(zkRoot + "/tables/4/state", null));
    names.add(new PropNode(zkRoot + "/tables/4/conf/table.iterator.majc.vers",
        "20,org.apache.accumulo.core.iterators.user.VersioningIterator"));
    names.add(new PropNode(zkRoot + "/tables/4/conf/table.constraint.1",
        "org.apache.accumulo.core.constraints.DefaultKeySizeConstraint"));
    names
        .add(new PropNode(zkRoot + "/tables/4/conf/table.iterator.scan.vers.opt.maxVersions", "1"));
    names.add(new PropNode(zkRoot + "/tables/4/conf/table.iterator.minc.vers",
        "/20,org.apache.accumulo.core.iterators.user.VersioningIterator"));
    names
        .add(new PropNode(zkRoot + "/tables/4/conf/table.iterator.majc.vers.opt.maxVersions", "1"));
    names
        .add(new PropNode(zkRoot + "/tables/4/conf/table.iterator.minc.vers.opt.maxVersions", "1"));
    names.add(new PropNode(zkRoot + "/tables/4/conf/table.iterator.scan.vers",
        "20,org.apache.accumulo.core.iterators.user.VersioningIterator"));
    names.add(new PropNode(zkRoot + "/tables/4/conf/table.bloom.enabled", "true"));
    names.add(new PropNode(zkRoot + "/tables/5/compact-cancel-id", null));
    names.add(new PropNode(zkRoot + "/tables/5/compact-id", null));
    names.add(new PropNode(zkRoot + "/tables/5/conf", null));
    names.add(new PropNode(zkRoot + "/tables/5/flush-id", null));
    names.add(new PropNode(zkRoot + "/tables/5/name", "tbl2"));
    names.add(new PropNode(zkRoot + "/tables/5/namespace", "2"));
    names.add(new PropNode(zkRoot + "/tables/5/state", null));
    names.add(new PropNode(zkRoot + "/tables/5/conf/table.constraint.1",
        "org.apache.accumulo.core.constraints.DefaultKeySizeConstraint"));
    names.add(new PropNode(zkRoot + "/tables/5/conf/table.iterator.majc.vers",
        "20,org.apache.accumulo.core.iterators.user.VersioningIterator"));
    names
        .add(new PropNode(zkRoot + "/tables/5/conf/table.iterator.majc.vers.opt.maxVersions", "1"));
    names.add(new PropNode(zkRoot + "/tables/5/conf/table.iterator.minc.vers",
        "20,org.apache.accumulo.core.iterators.user.VersioningIterator"));
    names
        .add(new PropNode(zkRoot + "/tables/5/conf/table.iterator.minc.vers.opt.maxVersions", "1"));
    names.add(new PropNode(zkRoot + "/tables/5/conf/table.iterator.scan.vers",
        "20,org.apache.accumulo.core.iterators.user.VersioningIterator"));
    names
        .add(new PropNode(zkRoot + "/tables/5/conf/table.iterator.scan.vers.opt.maxVersions", "1"));
    names.add(new PropNode(zkRoot + "/tables/6/compact-cancel-id", null));
    names.add(new PropNode(zkRoot + "/tables/6/compact-id", null));
    names.add(new PropNode(zkRoot + "/tables/6/conf", null));
    names.add(new PropNode(zkRoot + "/tables/6/flush-id", null));
    names.add(new PropNode(zkRoot + "/tables/6/name", "tbl3"));
    names.add(new PropNode(zkRoot + "/tables/6/namespace", "+default"));
    names.add(new PropNode(zkRoot + "/tables/6/state", null));
    names.add(new PropNode(zkRoot + "/tables/6/conf/table.bloom.enabled", "true"));
    names.add(new PropNode(zkRoot + "/tables/6/conf/table.constraint.1",
        "org.apache.accumulo.core.constraints.DefaultKeySizeConstraint"));
    names.add(new PropNode(zkRoot + "/tables/6/conf/table.iterator.majc.vers",
        "20,org.apache.accumulo.core.iterators.user.VersioningIterator"));
    names
        .add(new PropNode(zkRoot + "/tables/6/conf/table.iterator.majc.vers.opt.maxVersions", "1"));
    names.add(new PropNode(zkRoot + "/tables/6/conf/table.iterator.minc.vers",
        "20,org.apache.accumulo.core.iterators.user.VersioningIterator"));
    names
        .add(new PropNode(zkRoot + "/tables/6/conf/table.iterator.minc.vers.opt.maxVersions", "1"));
    names.add(new PropNode(zkRoot + "/tables/6/conf/table.iterator.scan.vers",
        "20,org.apache.accumulo.core.iterators.user.VersioningIterator"));
    names
        .add(new PropNode(zkRoot + "/tables/6/conf/table.iterator.scan.vers.opt.maxVersions", "1"));
    names.add(new PropNode(zkRoot + "/tables/7/compact-cancel-id", null));
    names.add(new PropNode(zkRoot + "/tables/7/compact-id", null));
    names.add(new PropNode(zkRoot + "/tables/7/conf", null));
    names.add(new PropNode(zkRoot + "/tables/7/flush-id", null));
    names.add(new PropNode(zkRoot + "/tables/7/name", "tbl4"));
    names.add(new PropNode(zkRoot + "/tables/7/namespace", "2"));
    names.add(new PropNode(zkRoot + "/tables/7/state", null));
    names.add(new PropNode(zkRoot + "/tables/7/conf/table.bloom.enabled", null));
    names.add(new PropNode(zkRoot + "/tables/7/conf/table.constraint.1", null));
    names.add(new PropNode(zkRoot + "/tables/7/conf/table.iterator.majc.vers", null));
    names.add(
        new PropNode(zkRoot + "/tables/7/conf/table.iterator.majc.vers.opt.maxVersions", null));
    names.add(new PropNode(zkRoot + "/tables/7/conf/table.iterator.minc.vers", null));
    names.add(
        new PropNode(zkRoot + "/tables/7/conf/table.iterator.minc.vers.opt.maxVersions", null));
    names.add(new PropNode(zkRoot + "/tables/7/conf/table.iterator.scan.vers", null));
    names.add(
        new PropNode(zkRoot + "/tables/7/conf/table.iterator.scan.vers.opt.maxVersions", null));
    names.add(new PropNode(zkRoot + "/tservers/localhost:11000", null));
    names.add(new PropNode(zkRoot + "/tservers/localhost:11000/zlock-0000000000", null));
    names.add(new PropNode(zkRoot + "/users/root", null));
    names.add(new PropNode(zkRoot + "/users/root/Authorizations", null));
    names.add(new PropNode(zkRoot + "/users/root/Namespaces", null));
    names.add(new PropNode(zkRoot + "/users/root/System", null));
    names.add(new PropNode(zkRoot + "/users/root/Tables", null));
    names.add(new PropNode(zkRoot + "/users/root/Namespaces/+accumulo", null));
    names.add(new PropNode(zkRoot + "/users/root/Namespaces/2", null));
    names.add(new PropNode(zkRoot + "/users/root/Namespaces/3", null));
    names.add(new PropNode(zkRoot + "/users/root/Tables/!0", null));
    names.add(new PropNode(zkRoot + "/users/root/Tables/+r", null));
    names.add(new PropNode(zkRoot + "/users/root/Tables/1", null));
    names.add(new PropNode(zkRoot + "/users/root/Tables/4", null));
    names.add(new PropNode(zkRoot + "/users/root/Tables/5", null));
    names.add(new PropNode(zkRoot + "/users/root/Tables/6", null));
    names.add(new PropNode(zkRoot + "/users/root/Tables/7", null));
    names.add(new PropNode(zkRoot + "/wals/localhost:11000[10000c3202e0003]", null));
    names.add(new PropNode(
        zkRoot + "/wals/localhost:11000[10000c3202e0003]/0fef8f3b-d02d-413b-9a27-f1710812b216",
        null));
    names.add(new PropNode(
        zkRoot + "/wals/localhost:11000[10000c3202e0003]/9e410484-e61b-4707-847a-67f96715aa04",
        null));

    return names;
  }

  public static class PropNode {
    private static final byte[] empty = new byte[0];
    private final String path;
    private final String value;

    public PropNode(final String path, final String value) {
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
