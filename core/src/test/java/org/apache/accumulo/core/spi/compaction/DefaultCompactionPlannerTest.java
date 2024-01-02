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
package org.apache.accumulo.core.spi.compaction;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.common.ServiceEnvironment.Configuration;
import org.apache.accumulo.core.spi.compaction.CompactionPlan.Builder;
import org.apache.accumulo.core.spi.compaction.CompactionPlanner.InitParameters;
import org.apache.accumulo.core.util.ConfigurationImpl;
import org.apache.accumulo.core.util.compaction.CompactionExecutorIdImpl;
import org.apache.accumulo.core.util.compaction.CompactionPlanImpl;
import org.apache.accumulo.core.util.compaction.CompactionPlannerInitParams;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

import com.google.gson.JsonParseException;

public class DefaultCompactionPlannerTest {

  private static <T> T getOnlyElement(Collection<T> c) {
    return c.stream().collect(onlyElement());
  }

  private static final Configuration defaultConf =
      new ConfigurationImpl(DefaultConfiguration.getInstance());
  private static final CompactionServiceId csid = CompactionServiceId.of("cs1");
  private static final String prefix = Property.COMPACTION_SERVICE_PREFIX.getKey();

  @Test
  public void testFindFilesToCompact() throws Exception {

    testFFtC(createCFs("F4", "1M", "F5", "1M", "F6", "1M"),
        createCFs("F1", "100M", "F2", "100M", "F3", "100M", "F4", "1M", "F5", "1M", "F6", "1M"),
        2.0);

    testFFtC(createCFs("F1", "100M", "F2", "100M", "F3", "100M", "F4", "1M"), 2.0);

    testFFtC(
        createCFs("F1", "100M", "F2", "99M", "F3", "33M", "F4", "33M", "F5", "33M", "F6", "33M"),
        2.0);
    testFFtC(
        createCFs("F1", "100M", "F2", "99M", "F3", "33M", "F4", "33M", "F5", "33M", "F6", "33M"),
        3.0);

    testFFtC(createCFs("F3", "10M", "F4", "10M", "F5", "10M", "F6", "10M"),
        createCFs("F1", "50M", "F2", "49M", "F3", "10M", "F4", "10M", "F5", "10M", "F6", "10M"),
        2.0);

    testFFtC(createCFs("F3", "10M", "F4", "10M", "F5", "10M", "F6", "10M"),
        createCFs("F1", "50M", "F2", "49M", "F3", "10M", "F4", "10M", "F5", "10M", "F6", "10M"),
        3.0);

    testFFtC(createCFs("S1", "1M", "S2", "1M", "S3", "1M", "S4", "1M"),
        createCFs("B1", "100M", "B2", "100M", "B3", "100M", "B4", "100M", "M1", "10M", "M2", "10M",
            "M3", "10M", "M4", "10M", "S1", "1M", "S2", "1M", "S3", "1M", "S4", "1M"),
        3.0);
    testFFtC(createCFs("M1", "10M", "M2", "10M", "M3", "10M", "M4", "10M", "C1", "4M"),
        createCFs("B1", "100M", "B2", "100M", "B3", "100M", "B4", "100M", "M1", "10M", "M2", "10M",
            "M3", "10M", "M4", "10M", "C1", "4M"),
        3.0);
    testFFtC(createCFs("B1", "100M", "B2", "100M", "B3", "100M", "B4", "100M", "C2", "44M"),
        createCFs("B1", "100M", "B2", "100M", "B3", "100M", "B4", "100M", "C2", "44M"), 3.0);
    testFFtC(createCFs(), createCFs("C3", "444M"), 3.0);

    testFFtC(createCFs(), createCFs("A1", "17M", "S1", "11M", "S2", "11M", "S3", "11M"), 3.0);
    testFFtC(createCFs("A1", "16M", "S1", "11M", "S2", "11M", "S3", "11M"), 3.0);

    testFFtC(
        createCFs("A1", "1M", "A2", "1M", "A3", "1M", "A4", "1M", "A5", "3M", "A6", "3M", "A7",
            "5M", "A8", "5M"),
        createCFs("A1", "1M", "A2", "1M", "A3", "1M", "A4", "1M", "A5", "3M", "A6", "3M", "A7",
            "5M", "A8", "5M", "A9", "100M", "A10", "100M", "A11", "100M", "A12", "500M"),
        3.0);

    testFFtC(
        createCFs("F1", "100M", "F2", "99M", "F3", "33M", "F4", "33M", "F5", "33M", "F6", "33M"),
        3.0);

    testFFtC(createCFs("F3", "10M", "F4", "9M", "F5", "8M", "F6", "7M"),
        createCFs("F1", "12M", "F2", "11M", "F3", "10M", "F4", "9M", "F5", "8M", "F6", "7M"), 3.0,
        4);

    testFFtC(createCFs("F3", "4M", "F4", "8M", "F5", "9M", "F6", "10M"),
        createCFs("F1", "1M", "F2", "2M", "F3", "4M", "F4", "8M", "F5", "9M", "F6", "10M"), 3.0, 4);

    testFFtC(createCFs(),
        createCFs("F1", "1M", "F2", "2M", "F3", "4M", "F4", "8M", "F5", "16M", "F6", "32M"), 3.0,
        4);

    testFFtC(createCFs(), createCFs("F1", "200M", "F2", "200M", "F3", "200M", "F4", "200M", "F5",
        "200M", "F6", "200M"), 3.0, 4, 100_000_000L);

    testFFtC(createCFs("F2", "2M", "F3", "30M", "F4", "30M", "F5", "30M"),
        createCFs("F1", "1M", "F2", "2M", "F3", "30M", "F4", "30M", "F5", "30M", "F6", "30M"), 3.0,
        4, 100_000_000L);

    testFFtC(createCFs("F1", "1M", "F2", "2M", "F3", "30M", "F4", "30M", "F5", "30M"),
        createCFs("F1", "1M", "F2", "2M", "F3", "30M", "F4", "30M", "F5", "30M", "F6", "30M"), 3.0,
        8, 100_000_000L);

    testFFtC(createCFs("F1", "1M", "F2", "2M", "F3", "30M", "F4", "30M", "F5", "30M", "F6", "30M"),
        createCFs("F1", "1M", "F2", "2M", "F3", "30M", "F4", "30M", "F5", "30M", "F6", "30M"), 3.0,
        8, 200_000_000L);

  }

  @Test
  public void testRunningCompaction() throws Exception {
    String executors = "[{'name':'small','type': 'internal','maxSize':'32M','numThreads':1},"
        + "{'name':'medium','type': 'internal','maxSize':'128M','numThreads':2},"
        + "{'name':'large','type': 'internal','maxSize':'512M','numThreads':3},"
        + "{'name':'huge','type': 'internal','numThreads':4}]";

    var planner = createPlanner(defaultConf, executors);

    var all = createCFs("F1", "3M", "F2", "3M", "F3", "11M", "F4", "12M", "F5", "13M");
    var candidates = createCFs("F3", "11M", "F4", "12M", "F5", "13M");
    var compacting =
        Set.of(createJob(CompactionKind.SYSTEM, all, createCFs("F1", "3M", "F2", "3M")));
    var params = createPlanningParams(all, candidates, compacting, 2, CompactionKind.SYSTEM);
    var plan = planner.makePlan(params);

    // The result of the running compaction could be included in a future compaction, so the planner
    // should wait.
    assertTrue(plan.getJobs().isEmpty());

    all = createCFs("F1", "30M", "F2", "30M", "F3", "11M", "F4", "12M", "F5", "13M");
    candidates = createCFs("F3", "11M", "F4", "12M", "F5", "13M");
    compacting = Set.of(createJob(CompactionKind.SYSTEM, all, createCFs("F1", "30M", "F2", "30M")));
    params = createPlanningParams(all, candidates, compacting, 2, CompactionKind.SYSTEM);
    plan = planner.makePlan(params);

    // The result of the running compaction would not be included in future compactions, so the
    // planner should compact.
    var job = getOnlyElement(plan.getJobs());
    assertEquals(candidates, job.getFiles());
    assertEquals(CompactionExecutorIdImpl.internalId(csid, "medium"), job.getExecutor());
  }

  @Test
  public void testUserCompaction() throws Exception {
    ConfigurationCopy aconf = new ConfigurationCopy(DefaultConfiguration.getInstance());
    aconf.set(prefix + "cs1.planner.opts.maxOpen", "15");
    ConfigurationImpl config = new ConfigurationImpl(aconf);

    String executors = "[{'name':'small','type': 'internal','maxSize':'32M','numThreads':1},"
        + "{'name':'medium','type': 'internal','maxSize':'128M','numThreads':2},"
        + "{'name':'large','type': 'internal','maxSize':'512M','numThreads':3},"
        + "{'name':'huge','type': 'internal','numThreads':4}]";

    var planner = createPlanner(config, executors);
    var all = createCFs("F1", "3M", "F2", "3M", "F3", "11M", "F4", "12M", "F5", "13M");
    var candidates = createCFs("F3", "11M", "F4", "12M", "F5", "13M");
    var compacting =
        Set.of(createJob(CompactionKind.SYSTEM, all, createCFs("F1", "3M", "F2", "3M")));
    var params = createPlanningParams(all, candidates, compacting, 2, CompactionKind.USER);
    var plan = planner.makePlan(params);

    // a running non-user compaction should not prevent a user compaction
    var job = getOnlyElement(plan.getJobs());
    assertEquals(candidates, job.getFiles());
    assertEquals(CompactionExecutorIdImpl.internalId(csid, "medium"), job.getExecutor());

    // should only run one user compaction at a time
    compacting = Set.of(createJob(CompactionKind.USER, all, createCFs("F1", "3M", "F2", "3M")));
    params = createPlanningParams(all, candidates, compacting, 2, CompactionKind.USER);
    plan = planner.makePlan(params);
    assertTrue(plan.getJobs().isEmpty());

    // 17 files that do not meet the compaction ratio, when max files to compact is 15 should do 3
    // files then 15
    all = createCFs("F1", "1M", "F2", "2M", "F3", "4M", "F4", "8M", "F5", "16M", "F6", "32M", "F7",
        "64M", "F8", "128M", "F9", "256M", "FA", "512M", "FB", "1G", "FC", "2G", "FD", "4G", "FE",
        "8G", "FF", "16G", "FG", "32G", "FH", "64G");
    compacting = Set.of();
    params = createPlanningParams(all, all, compacting, 2, CompactionKind.USER);
    plan = planner.makePlan(params);
    job = getOnlyElement(plan.getJobs());
    assertEquals(createCFs("F1", "1M", "F2", "2M", "F3", "4M"), job.getFiles());
    assertEquals(CompactionExecutorIdImpl.internalId(csid, "small"), job.getExecutor());

    // should compact all 15
    all = createCFs("FI", "7M", "F4", "8M", "F5", "16M", "F6", "32M", "F7", "64M", "F8", "128M",
        "F9", "256M", "FA", "512M", "FB", "1G", "FC", "2G", "FD", "4G", "FE", "8G", "FF", "16G",
        "FG", "32G", "FH", "64G");
    params = createPlanningParams(all, all, compacting, 2, CompactionKind.USER);
    plan = planner.makePlan(params);
    job = getOnlyElement(plan.getJobs());
    assertEquals(all, job.getFiles());
    assertEquals(CompactionExecutorIdImpl.internalId(csid, "huge"), job.getExecutor());

    // For user compaction, can compact a subset that meets the compaction ratio if there is also a
    // larger set of files the meets the compaction ratio
    all = createCFs("F1", "3M", "F2", "4M", "F3", "5M", "F4", "6M", "F5", "50M", "F6", "51M", "F7",
        "52M");
    params = createPlanningParams(all, all, compacting, 2, CompactionKind.USER);
    plan = planner.makePlan(params);
    job = getOnlyElement(plan.getJobs());
    assertEquals(createCFs("F1", "3M", "F2", "4M", "F3", "5M", "F4", "6M"), job.getFiles());
    assertEquals(CompactionExecutorIdImpl.internalId(csid, "small"), job.getExecutor());

    // There is a subset of small files that meets the compaction ratio, but the larger set does not
    // so compact everything to avoid doing more than logarithmic work
    all = createCFs("F1", "3M", "F2", "4M", "F3", "5M", "F4", "6M", "F5", "50M");
    params = createPlanningParams(all, all, compacting, 2, CompactionKind.USER);
    plan = planner.makePlan(params);
    job = getOnlyElement(plan.getJobs());
    assertEquals(all, job.getFiles());
    assertEquals(CompactionExecutorIdImpl.internalId(csid, "medium"), job.getExecutor());

  }

  @Test
  public void testMaxSize() throws Exception {
    String executors = "[{'name':'small','type': 'internal','maxSize':'32M','numThreads':1},"
        + "{'name':'medium','type': 'internal','maxSize':'128M','numThreads':2},"
        + "{'name':'large','type': 'internal','maxSize':'512M','numThreads':3}]";

    var planner = createPlanner(defaultConf, executors);
    var all = createCFs("F1", "128M", "F2", "129M", "F3", "130M", "F4", "131M", "F5", "132M");
    var params = createPlanningParams(all, all, Set.of(), 2, CompactionKind.SYSTEM);
    var plan = planner.makePlan(params);

    // should only compact files less than max size
    var job = getOnlyElement(plan.getJobs());
    assertEquals(createCFs("F1", "128M", "F2", "129M", "F3", "130M"), job.getFiles());
    assertEquals(CompactionExecutorIdImpl.internalId(csid, "large"), job.getExecutor());

    // user compaction can exceed the max size
    params = createPlanningParams(all, all, Set.of(), 2, CompactionKind.USER);
    plan = planner.makePlan(params);
    job = getOnlyElement(plan.getJobs());
    assertEquals(all, job.getFiles());
    assertEquals(CompactionExecutorIdImpl.internalId(csid, "large"), job.getExecutor());
  }

  @Test
  public void testMultipleCompactions() {
    // This test validates that when a tablet has many files that multiple compaction jobs can be
    // issued at the same time.
    String executors = "[{'name':'small','type': 'internal','maxSize':'32M','numThreads':1},"
        + "{'name':'medium','type': 'internal','maxSize':'128M','numThreads':2},"
        + "{'name':'large','type': 'internal','maxSize':'512M','numThreads':3}]";

    for (var kind : List.of(CompactionKind.USER, CompactionKind.SYSTEM)) {
      var planner = createPlanner(defaultConf, executors);
      var all = IntStream.range(0, 990).mapToObj(i -> createCF("F" + i, 1000)).collect(toSet());
      // simulate 10 larger files, these should not compact at the same time as the smaller files.
      // Its more optimal to wait for all of the smaller files to compact and them compact the
      // output of compacting the smaller files with the larger files.
      IntStream.range(990, 1000).mapToObj(i -> createCF("C" + i, 20000)).forEach(all::add);
      var params = createPlanningParams(all, all, Set.of(), 2, kind);
      var plan = planner.makePlan(params);

      // There are 990 smaller files to compact. Should produce 66 jobs of 15 smaller files each.
      assertEquals(66, plan.getJobs().size());
      Set<CompactableFile> filesSeen = new HashSet<>();
      plan.getJobs().forEach(job -> {
        assertEquals(15, job.getFiles().size());
        assertEquals(kind, job.getKind());
        assertEquals(CompactionExecutorIdImpl.internalId(csid, "small"), job.getExecutor());
        // ensure the files across all of the jobs are disjoint
        job.getFiles().forEach(cf -> assertTrue(filesSeen.add(cf)));
      });

      // Ensure all of the smaller files are scheduled for compaction. Should not see any of the
      // larger files.
      assertEquals(IntStream.range(0, 990).mapToObj(i -> createCF("F" + i, 1000)).collect(toSet()),
          filesSeen);
    }
  }

  @Test
  public void testMultipleCompactionsAndLargeCompactionRatio() {

    String executors = "[{'name':'small','type': 'internal','maxSize':'32M','numThreads':1},"
        + "{'name':'medium','type': 'internal','maxSize':'128M','numThreads':2},"
        + "{'name':'large','type': 'internal','maxSize':'512M','numThreads':3}]";
    var planner = createPlanner(defaultConf, executors);
    var all = IntStream.range(0, 65).mapToObj(i -> createCF("F" + i, i + 1)).collect(toSet());
    // This compaction ratio would not cause a system compaction, how a user compaction must compact
    // all of the files so it should generate some compactions.
    var params = createPlanningParams(all, all, Set.of(), 100, CompactionKind.USER);
    var plan = planner.makePlan(params);

    assertEquals(3, plan.getJobs().size());

    var iterator = plan.getJobs().iterator();
    var job1 = iterator.next();
    var job2 = iterator.next();
    var job3 = iterator.next();
    assertTrue(Collections.disjoint(job1.getFiles(), job2.getFiles()));
    assertTrue(Collections.disjoint(job1.getFiles(), job3.getFiles()));
    assertTrue(Collections.disjoint(job2.getFiles(), job3.getFiles()));

    for (var job : plan.getJobs()) {
      assertEquals(15, job.getFiles().size());
      assertEquals(CompactionKind.USER, job.getKind());
      assertTrue(all.containsAll(job.getFiles()));
      // Should select three sets of files that are from the smallest 45 files.
      assertTrue(job.getFiles().stream().mapToLong(CompactableFile::getEstimatedSize).sum()
          <= IntStream.range(1, 46).sum());
    }
  }

  @Test
  public void testMultipleCompactionsAndRunningCompactions() {
    // This test validates that when a tablet has many files that multiple compaction jobs can be
    // issued at the same time even if there are running compaction as long everything meets the
    // compaction ratio.
    String executors = "[{'name':'small','type': 'internal','maxSize':'32M','numThreads':1},"
        + "{'name':'medium','type': 'internal','maxSize':'128M','numThreads':2},"
        + "{'name':'large','type': 'internal','maxSize':'512M','numThreads':3}]";
    for (var kind : List.of(CompactionKind.USER, CompactionKind.SYSTEM)) {
      var planner = createPlanner(defaultConf, executors);
      var all = IntStream.range(0, 990).mapToObj(i -> createCF("F" + i, 1000)).collect(toSet());
      // simulate 10 larger files, these should not compact at the same time as the smaller files.
      // Its more optimal to wait for all of the smaller files to compact and them compact the
      // output of compacting the smaller files with the larger files.
      IntStream.range(990, 1000).mapToObj(i -> createCF("C" + i, 20000)).forEach(all::add);
      // 30 files are compacting, so they will not be in the candidate set.
      var candidates =
          IntStream.range(30, 990).mapToObj(i -> createCF("F" + i, 1000)).collect(toSet());
      // create two jobs covering the first 30 files
      var job1 = createJob(kind, all,
          IntStream.range(0, 15).mapToObj(i -> createCF("F" + i, 1000)).collect(toSet()));
      var job2 = createJob(kind, all,
          IntStream.range(15, 30).mapToObj(i -> createCF("F" + i, 1000)).collect(toSet()));
      var params = createPlanningParams(all, candidates, Set.of(job1, job2), 2, kind);
      var plan = planner.makePlan(params);

      // There are 990 smaller files to compact. Should produce 66 jobs of 15 smaller files each.
      assertEquals(64, plan.getJobs().size());
      Set<CompactableFile> filesSeen = new HashSet<>();
      plan.getJobs().forEach(job -> {
        assertEquals(15, job.getFiles().size());
        assertEquals(kind, job.getKind());
        assertEquals(CompactionExecutorIdImpl.internalId(csid, "small"), job.getExecutor());
        // ensure the files across all of the jobs are disjoint
        job.getFiles().forEach(cf -> assertTrue(filesSeen.add(cf)));
      });

      // Ensure all of the smaller files are scheduled for compaction. Should not see any of the
      // larger files.
      assertEquals(IntStream.range(30, 990).mapToObj(i -> createCF("F" + i, 1000)).collect(toSet()),
          filesSeen);
    }
  }

  @Test
  public void testUserCompactionDoesNotWaitOnSystemCompaction() {
    // this test ensure user compactions do not wait on system compactions to complete

    String executors = "[{'name':'small','type': 'internal','maxSize':'32M','numThreads':1},"
        + "{'name':'medium','type': 'internal','maxSize':'128M','numThreads':2},"
        + "{'name':'large','type': 'internal','maxSize':'512M','numThreads':3}]";
    var planner = createPlanner(defaultConf, executors);
    var all = createCFs("F1", "1M", "F2", "1M", "F3", "1M", "F4", "3M", "F5", "3M", "F6", "3M",
        "F7", "20M");
    var candidates = createCFs("F4", "3M", "F5", "3M", "F6", "3M", "F7", "20M");
    var compacting = Set
        .of(createJob(CompactionKind.SYSTEM, all, createCFs("F1", "1M", "F2", "1M", "F3", "1M")));
    var params = createPlanningParams(all, candidates, compacting, 2, CompactionKind.SYSTEM);
    var plan = planner.makePlan(params);
    // The planning of the system compaction should find its most optimal to wait on the running
    // system compaction and emit zero jobs.
    assertEquals(0, plan.getJobs().size());

    params = createPlanningParams(all, candidates, compacting, 2, CompactionKind.USER);
    plan = planner.makePlan(params);
    // The planning of user compaction should not take the running system compaction into
    // consideration and should create a compaction job.
    assertEquals(1, plan.getJobs().size());
    assertEquals(createCFs("F4", "3M", "F5", "3M", "F6", "3M", "F7", "20M"),
        getOnlyElement(plan.getJobs()).getFiles());

    // Reverse the situation and turn the running compaction into a user compaction
    compacting =
        Set.of(createJob(CompactionKind.USER, all, createCFs("F1", "1M", "F2", "1M", "F3", "1M")));
    params = createPlanningParams(all, candidates, compacting, 2, CompactionKind.SYSTEM);
    plan = planner.makePlan(params);
    // The planning of a system compaction should not take the running user compaction into account
    // and should emit a job
    assertEquals(1, plan.getJobs().size());
    assertEquals(createCFs("F4", "3M", "F5", "3M", "F6", "3M"),
        getOnlyElement(plan.getJobs()).getFiles());

    params = createPlanningParams(all, candidates, compacting, 2, CompactionKind.USER);
    plan = planner.makePlan(params);
    // The planning of the user compaction should decide the most optimal thing to do is to wait on
    // the running user compaction and should not emit any jobs.
    assertEquals(0, plan.getJobs().size());
  }

  @Test
  public void testQueueCreation() throws Exception {
    DefaultCompactionPlanner planner = new DefaultCompactionPlanner();

    String queues = "[{\"name\": \"small\", \"maxSize\":\"32M\"},{\"name\":\"midsize\"}]";
    planner.init(getInitParamQueues(defaultConf, queues));

    var all = createCFs("F1", "1M", "F2", "1M", "F3", "1M", "F4", "1M");
    var params = createPlanningParams(all, all, Set.of(), 2, CompactionKind.SYSTEM);
    var plan = planner.makePlan(params);

    var job = getOnlyElement(plan.getJobs());
    assertEquals(all, job.getFiles());
    assertEquals(CompactionExecutorIdImpl.externalId("small"), job.getExecutor());

    all = createCFs("F1", "100M", "F2", "100M", "F3", "100M", "F4", "100M");
    params = createPlanningParams(all, all, Set.of(), 2, CompactionKind.SYSTEM);
    plan = planner.makePlan(params);

    job = getOnlyElement(plan.getJobs());
    assertEquals(all, job.getFiles());
    assertEquals(CompactionExecutorIdImpl.externalId("midsize"), job.getExecutor());
  }

  /**
   * Tests that additional fields in the JSON objects cause errors to be thrown.
   */
  @Test
  public void testErrorAdditionalConfigFields() {
    DefaultCompactionPlanner QueuePlanner = new DefaultCompactionPlanner();

    String queues =
        "[{\"name\":\"smallQueue\", \"maxSize\":\"32M\"}, {\"name\":\"largeQueue\", \"type\":\"internal\", \"foo\":\"bar\", \"queue\":\"broken\"}]";

    final InitParameters queueParams = getInitParamQueues(defaultConf, queues);
    assertNotNull(queueParams);
    var e = assertThrows(JsonParseException.class, () -> QueuePlanner.init(queueParams),
        "Failed to throw error");
    assertTrue(e.getMessage().contains("[type, foo, queue]"),
        "Error message didn't contain '[type, foo, queue]'");

    String executors = "[{'name':'small','type': 'internal','maxSize':'32M','numThreads':1},"
        + "{'name':'medium','type': 'internal','maxSize':'128M','numThreads':2, 'foo':'bar'},"
        + "{'name':'large','type': 'internal','numThreads':1, 'unexpectedField':'foo'}]";

    final InitParameters execParams = getInitParams(defaultConf, executors);
    assertNotNull(execParams);

    DefaultCompactionPlanner ExecPlanner = new DefaultCompactionPlanner();
    var err = assertThrows(JsonParseException.class, () -> ExecPlanner.init(execParams),
        "Failed to throw error");
    assertTrue(err.getMessage().contains("Invalid fields: [foo]"),
        "Error message didn't contain '[foo]'");
  }

  /**
   * Tests internal type executor with no numThreads set throws error
   */
  @Test
  public void testErrorInternalTypeNoNumThreads() {
    DefaultCompactionPlanner planner = new DefaultCompactionPlanner();
    String executors = "[{'name':'small','type':'internal','maxSize':'32M'},"
        + "{'name':'medium','type':'internal','maxSize':'128M','numThreads':2},"
        + "{'name':'large','type':'internal','maxSize':'512M','numThreads':3}]";

    var e = assertThrows(NullPointerException.class,
        () -> planner.init(getInitParams(defaultConf, executors)), "Failed to throw error");
    assertTrue(e.getMessage().contains("numThreads"), "Error message didn't contain numThreads");
  }

  /**
   * Test external type executor with numThreads set throws error.
   */
  @Test
  public void testErrorExternalTypeNumThreads() {
    DefaultCompactionPlanner planner = new DefaultCompactionPlanner();
    String executors = "[{'name':'small','type':'internal','maxSize':'32M', 'numThreads':1},"
        + "{'name':'medium','type':'internal','maxSize':'128M','numThreads':2},"
        + "{'name':'large','type':'external','maxSize':'512M','numThreads':3}]";

    var e = assertThrows(IllegalArgumentException.class,
        () -> planner.init(getInitParams(defaultConf, executors)), "Failed to throw error");
    assertTrue(e.getMessage().contains("numThreads"), "Error message didn't contain numThreads");
  }

  /**
   * Tests external type executor missing group throws error
   */
  @Test
  public void testErrorExternalNoQueue() {
    DefaultCompactionPlanner planner = new DefaultCompactionPlanner();
    String executors = "[{'name':'small','type':'internal','maxSize':'32M', 'numThreads':1},"
        + "{'name':'medium','type':'internal','maxSize':'128M','numThreads':2},"
        + "{'name':'large','type':'external','maxSize':'512M'}]";

    var e = assertThrows(NullPointerException.class,
        () -> planner.init(getInitParams(defaultConf, executors)), "Failed to throw error");
    assertTrue(e.getMessage().contains("group"), "Error message didn't contain group");
  }

  /**
   * Tests queue with missing name throws error
   */
  @Test
  public void testErrorQueueNoName() {
    DefaultCompactionPlanner planner = new DefaultCompactionPlanner();
    String queues = "[{\"name\":\"smallQueue\", \"maxSize\":\"32M\"}, {\"maxSize\":\"120M\"}]";

    final InitParameters params = getInitParamQueues(defaultConf, queues);
    assertNotNull(params);

    var e = assertThrows(NullPointerException.class, () -> planner.init(params),
        "Failed to throw error");
    assertEquals(e.getMessage(), "'name' must be specified", "Error message didn't contain 'name'");
  }

  /**
   * Tests not having executors or queues throws errors
   */
  @Test
  public void testErrorNoExecutors() {
    DefaultCompactionPlanner planner = new DefaultCompactionPlanner();
    var execParams = getInitParams(defaultConf, "");
    assertNotNull(execParams);

    var e = assertThrows(IllegalStateException.class, () -> planner.init(execParams),
        "Failed to throw error");
    assertEquals("No defined executors or queues for this planner", e.getMessage(),
        "Error message was not equal");

    var params = getInitParamQueues(defaultConf, "");
    assertNotNull(params);

    var e2 = assertThrows(IllegalStateException.class, () -> planner.init(params),
        "Failed to throw error");
    assertEquals("No defined executors or queues for this planner", e2.getMessage(),
        "Error message was not equal");
  }

  /**
   * Tests executors can only have one without a max size.
   */
  @Test
  public void testErrorOnlyOneMaxSize() {
    DefaultCompactionPlanner planner = new DefaultCompactionPlanner();
    String executors = "[{'name':'small','type':'internal','maxSize':'32M', 'numThreads':1},"
        + "{'name':'medium','type':'internal','numThreads':2},"
        + "{'name':'large','type':'internal','numThreads':'3'}]";
    var e = assertThrows(IllegalArgumentException.class,
        () -> planner.init(getInitParams(defaultConf, executors)), "Failed to throw error");
    assertTrue(e.getMessage().contains("maxSize"), "Error message didn't contain maxSize");
  }

  /**
   * Tests executors can only have one without a max size.
   */
  @Test
  public void testErrorDuplicateMaxSize() {
    DefaultCompactionPlanner planner = new DefaultCompactionPlanner();
    String executors = "[{'name':'small','type': 'internal','maxSize':'32M','numThreads':1},"
        + "{'name':'medium','type': 'internal','maxSize':'128M','numThreads':2},"
        + "{'name':'large','type': 'internal','maxSize':'128M','numThreads':3}]";

    var e = assertThrows(IllegalArgumentException.class,
        () -> planner.init(getInitParams(defaultConf, executors)), "Failed to throw error");
    assertTrue(e.getMessage().contains("maxSize"), "Error message didn't contain maxSize");
  }

  private CompactionJob createJob(CompactionKind kind, Set<CompactableFile> all,
      Set<CompactableFile> files) {
    return new CompactionPlanImpl.BuilderImpl(kind, all, all)
        .addJob((short) all.size(), CompactionExecutorIdImpl.internalId(csid, "small"), files)
        .build().getJobs().iterator().next();
  }

  private static CompactableFile createCF(String name, long size) {
    try {
      return CompactableFile
          .create(new URI("hdfs://fake/accumulo/tables/1/t-0000000z/" + name + ".rf"), size, 0);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  private static Set<CompactableFile> createCFs(String... namesSizePairs) {
    Set<CompactableFile> files = new HashSet<>();

    for (int i = 0; i < namesSizePairs.length; i += 2) {
      String name = namesSizePairs[i];
      long size = ConfigurationTypeHelper.getFixedMemoryAsBytes(namesSizePairs[i + 1]);
      files.add(createCF(name, size));
    }

    return files;
  }

  private static void testFFtC(Set<CompactableFile> expected, double ratio) {
    testFFtC(expected, expected, ratio, 100);
  }

  private static void testFFtC(Set<CompactableFile> expected, Set<CompactableFile> files,
      double ratio) {
    testFFtC(expected, files, ratio, 100);
  }

  private static void testFFtC(Set<CompactableFile> expected, Set<CompactableFile> files,
      double ratio, int maxFiles) {
    testFFtC(expected, files, ratio, maxFiles, Long.MAX_VALUE);
  }

  private static void testFFtC(Set<CompactableFile> expected, Set<CompactableFile> files,
      double ratio, int maxFiles, long maxSize) {
    var result = DefaultCompactionPlanner.findDataFilesToCompact(files, ratio, maxFiles, maxSize);
    var expectedNames = expected.stream().map(CompactableFile::getUri).map(URI::getPath)
        .map(path -> path.split("/")).map(t -> t[t.length - 1]).collect(toSet());
    var resultNames = result.stream().map(CompactableFile::getUri).map(URI::getPath)
        .map(path -> path.split("/")).map(t -> t[t.length - 1]).collect(toSet());
    assertEquals(expectedNames, resultNames);
  }

  private static CompactionPlanner.PlanningParameters createPlanningParams(Set<CompactableFile> all,
      Set<CompactableFile> candidates, Set<CompactionJob> compacting, double ratio,
      CompactionKind kind) {
    return new CompactionPlanner.PlanningParameters() {

      @Override
      public TableId getTableId() {
        return TableId.of("42");
      }

      @Override
      public ServiceEnvironment getServiceEnvironment() {
        throw new UnsupportedOperationException();
      }

      @Override
      public Collection<CompactionJob> getRunningCompactions() {
        return compacting;
      }

      @Override
      public double getRatio() {
        return ratio;
      }

      @Override
      public CompactionKind getKind() {
        return kind;
      }

      @Override
      public Map<String,String> getExecutionHints() {
        return Map.of();
      }

      @Override
      public Collection<CompactableFile> getCandidates() {
        return candidates;
      }

      @Override
      public Collection<CompactableFile> getAll() {
        return all;
      }

      @Override
      public Builder createPlanBuilder() {
        return new CompactionPlanImpl.BuilderImpl(kind, all, candidates);
      }
    };
  }

  private static CompactionPlanner.InitParameters getInitParamQueues(Configuration conf,
      String queues) {

    Map<String,String> options = new HashMap<>();
    options.put("queues", queues.replaceAll("'", "\""));
    options.put("maxOpen", "15");

    ServiceEnvironment senv = EasyMock.createMock(ServiceEnvironment.class);
    EasyMock.expect(senv.getConfiguration()).andReturn(conf).anyTimes();
    EasyMock.replay(senv);

    return new CompactionPlannerInitParams(csid, prefix, options, senv);
  }

  private static CompactionPlanner.InitParameters getInitParams(Configuration conf,
      String executors) {

    Map<String,String> options = new HashMap<>();
    options.put("executors", executors.replaceAll("'", "\""));
    options.put("maxOpen", "15");

    ServiceEnvironment senv = EasyMock.createMock(ServiceEnvironment.class);
    EasyMock.expect(senv.getConfiguration()).andReturn(conf).anyTimes();
    EasyMock.replay(senv);

    return new CompactionPlannerInitParams(csid, prefix, options, senv);
  }

  private static DefaultCompactionPlanner createPlanner(Configuration conf, String executors) {
    DefaultCompactionPlanner planner = new DefaultCompactionPlanner();
    var initParams = getInitParams(conf, executors);

    planner.init(initParams);
    return planner;
  }
}
