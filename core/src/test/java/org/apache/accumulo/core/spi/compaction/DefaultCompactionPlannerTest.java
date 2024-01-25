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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.common.ServiceEnvironment.Configuration;
import org.apache.accumulo.core.spi.compaction.CompactionPlan.Builder;
import org.apache.accumulo.core.util.ConfigurationImpl;
import org.apache.accumulo.core.util.compaction.CompactionExecutorIdImpl;
import org.apache.accumulo.core.util.compaction.CompactionJobImpl;
import org.apache.accumulo.core.util.compaction.CompactionPlanImpl;
import org.apache.accumulo.core.util.compaction.CompactionPlannerInitParams;
import org.apache.accumulo.core.util.compaction.CompactionServicesConfig;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class DefaultCompactionPlannerTest {

  private static <T> T getOnlyElement(Collection<T> c) {
    return c.stream().collect(onlyElement());
  }

  private static final Configuration defaultConf =
      new ConfigurationImpl(DefaultConfiguration.getInstance());
  private static final CompactionServiceId csid = CompactionServiceId.of("cs1");

  private static final Logger log = LoggerFactory.getLogger(DefaultCompactionPlannerTest.class);

  @Test
  public void testFindFilesToCompact() {

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
  public void testRunningCompaction() {
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

  /**
   * Tests that the maxOpen property overrides the deprecated open.max property with the default
   * service
   */
  @Test
  @SuppressWarnings("removal")
  public void testOverrideMaxOpenDefaultService() {
    Map<String,String> overrides = new HashMap<>();
    // Set old property and use that for max open files.
    overrides.put(Property.TSERV_MAJC_THREAD_MAXOPEN.getKey(), "17");
    SiteConfiguration aconf = SiteConfiguration.empty().withOverrides(overrides).build();
    ConfigurationImpl config = new ConfigurationImpl(aconf);

    ServiceEnvironment senv = EasyMock.createMock(ServiceEnvironment.class);
    EasyMock.expect(senv.getConfiguration()).andReturn(config).anyTimes();
    EasyMock.replay(senv);

    // Use the CompactionServicesConfig to create options based on default property values
    var compactionServices = new CompactionServicesConfig(aconf, log::warn);
    var options = compactionServices.getOptions().get("default");

    var initParams =
        new CompactionPlannerInitParams(CompactionServiceId.of("default"), options, senv);

    var planner = new DefaultCompactionPlanner();
    planner.init(initParams);

    var all = createCFs("F1", "10M", "F2", "11M", "F3", "12M", "F4", "13M", "F5", "14M", "F6",
        "15M", "F7", "16M", "F8", "17M", "F9", "18M", "FA", "19M", "FB", "20M", "FC", "21M", "FD",
        "22M", "FE", "23M", "FF", "24M", "FG", "25M", "FH", "26M");
    Set<CompactionJob> compacting = Set.of();
    var params = createPlanningParams(all, all, compacting, 2, CompactionKind.USER);
    var plan = planner.makePlan(params);
    var job = getOnlyElement(plan.getJobs());
    assertEquals(all, job.getFiles());
    assertEquals(CompactionExecutorIdImpl.internalId(CompactionServiceId.of("default"), "large"),
        job.getExecutor());

    overrides.put(Property.TSERV_MAJC_THREAD_MAXOPEN.getKey(), "5");
    aconf = SiteConfiguration.empty().withOverrides(overrides).build();
    config = new ConfigurationImpl(aconf);
    senv = EasyMock.createMock(ServiceEnvironment.class);
    EasyMock.expect(senv.getConfiguration()).andReturn(config).anyTimes();
    EasyMock.replay(senv);

    // Create new initParams so executor IDs can be reused
    initParams = new CompactionPlannerInitParams(CompactionServiceId.of("default"), options, senv);
    planner = new DefaultCompactionPlanner();
    planner.init(initParams);

    params = createPlanningParams(all, all, compacting, 2, CompactionKind.USER);
    plan = planner.makePlan(params);
    job = getOnlyElement(plan.getJobs());
    assertEquals(createCFs("F1", "10M", "F2", "11M", "F3", "12M", "F4", "13M", "F5", "14M"),
        job.getFiles());
    assertEquals(CompactionExecutorIdImpl.internalId(CompactionServiceId.of("default"), "medium"),
        job.getExecutor());
  }

  /**
   * Tests that the maxOpen property overrides the deprecated open.max property
   */
  @Test
  @SuppressWarnings("removal")
  public void testOverrideMaxOpen() {
    Map<String,String> overrides = new HashMap<>();
    // Set old property and use that for max open files.
    overrides.put(Property.TSERV_MAJC_THREAD_MAXOPEN.getKey(), "17");
    SiteConfiguration aconf = SiteConfiguration.empty().withOverrides(overrides).build();
    ConfigurationImpl config = new ConfigurationImpl(aconf);

    String executors = "[{'name':'small','type': 'internal','maxSize':'32M','numThreads':1},"
        + "{'name':'medium','type': 'internal','maxSize':'128M','numThreads':2},"
        + "{'name':'large','type': 'internal','maxSize':'512M','numThreads':3},"
        + "{'name':'huge','type': 'internal','numThreads':4}]";

    var planner = createPlanner(config, executors);
    var all = createCFs("F1", "1M", "F2", "2M", "F3", "4M", "F4", "8M", "F5", "16M", "F6", "32M",
        "F7", "64M", "F8", "128M", "F9", "256M", "FA", "512M", "FB", "1G", "FC", "2G", "FD", "4G",
        "FE", "8G", "FF", "16G", "FG", "32G", "FH", "64G");
    Set<CompactionJob> compacting = Set.of();
    var params = createPlanningParams(all, all, compacting, 2, CompactionKind.USER);
    var plan = planner.makePlan(params);
    var job = getOnlyElement(plan.getJobs());
    assertEquals(all, job.getFiles());
    assertEquals(CompactionExecutorIdImpl.internalId(csid, "huge"), job.getExecutor());

    // Set new property that overrides the old property.
    overrides.put(Property.TSERV_COMPACTION_SERVICE_PREFIX.getKey() + "cs1.planner.opts.maxOpen",
        "15");
    aconf = SiteConfiguration.empty().withOverrides(overrides).build();
    config = new ConfigurationImpl(aconf);
    planner = createPlanner(config, executors);
    params = createPlanningParams(all, all, compacting, 2, CompactionKind.USER);
    plan = planner.makePlan(params);

    // 17 files that do not meet the compaction ratio. When max files to compact is 15,
    // the plan should do 3 files then 15
    job = getOnlyElement(plan.getJobs());
    assertEquals(createCFs("F1", "1M", "F2", "2M", "F3", "4M"), job.getFiles());
    assertEquals(CompactionExecutorIdImpl.internalId(csid, "small"), job.getExecutor());

    overrides.put(Property.TSERV_COMPACTION_SERVICE_PREFIX.getKey() + "cs1.planner.opts.maxOpen",
        "5");
    aconf = SiteConfiguration.empty().withOverrides(overrides).build();
    // 17 files that do not meet the compaction ratio. When max files to compact is 5 should do 5,
    // files then another 5, then the final 5.
    config = new ConfigurationImpl(aconf);
    planner = createPlanner(config, executors);
    params = createPlanningParams(all, all, compacting, 2, CompactionKind.USER);
    plan = planner.makePlan(params);
    job = getOnlyElement(plan.getJobs());
    assertEquals(createCFs("F4", "8M", "F3", "4M", "F2", "2M", "F1", "1M", "F5", "16M"),
        job.getFiles());
    assertEquals(CompactionExecutorIdImpl.internalId(csid, "small"), job.getExecutor());
  }

  @Test
  public void testUserCompaction() {
    ConfigurationCopy aconf = new ConfigurationCopy(DefaultConfiguration.getInstance());
    aconf.set(Property.TSERV_COMPACTION_SERVICE_PREFIX.getKey() + "cs1.planner.opts.maxOpen", "15");
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
  public void testMaxSize() {
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
   * Tests external type executor missing queue throws error
   */
  @Test
  public void testErrorExternalNoQueue() {
    DefaultCompactionPlanner planner = new DefaultCompactionPlanner();
    String executors = "[{'name':'small','type':'internal','maxSize':'32M', 'numThreads':1},"
        + "{'name':'medium','type':'internal','maxSize':'128M','numThreads':2},"
        + "{'name':'large','type':'external','maxSize':'512M'}]";

    var e = assertThrows(NullPointerException.class,
        () -> planner.init(getInitParams(defaultConf, executors)), "Failed to throw error");
    assertTrue(e.getMessage().contains("queue"), "Error message didn't contain queue");
  }

  /**
   * Tests executors can only have one without a max size.
   */
  @Test
  public void testErrorOnlyOneMaxSize() {
    DefaultCompactionPlanner planner = new DefaultCompactionPlanner();
    String executors = "[{'name':'small','type':'internal','maxSize':'32M', 'numThreads':1},"
        + "{'name':'medium','type':'internal','numThreads':2},"
        + "{'name':'large','type':'external','queue':'q1'}]";

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
    String executors = "[{'name':'small','type':'internal','maxSize':'32M', 'numThreads':1},"
        + "{'name':'medium','type':'internal','maxSize':'128M','numThreads':2},"
        + "{'name':'large','type':'external','maxSize':'128M','queue':'q1'}]";

    var e = assertThrows(IllegalArgumentException.class,
        () -> planner.init(getInitParams(defaultConf, executors)), "Failed to throw error");
    assertTrue(e.getMessage().contains("maxSize"), "Error message didn't contain maxSize");
  }

  // Test cases where a tablet has more than table.file.max files, but no files were found using the
  // compaction ratio. The planner should try to find the highest ratio that will result in a
  // compaction.
  @Test
  public void testMaxTabletFiles() {
    String executors = "[{'name':'small','type': 'internal','maxSize':'32M','numThreads':1},"
        + "{'name':'medium','type': 'internal','maxSize':'128M','numThreads':2},"
        + "{'name':'large','type': 'internal','numThreads':3}]";

    Map<String,String> overrides = new HashMap<>();
    overrides.put(Property.TSERV_COMPACTION_SERVICE_PREFIX.getKey() + "cs1.planner.opts.maxOpen",
        "10");
    overrides.put(Property.TABLE_FILE_MAX.getKey(), "7");
    var conf = new ConfigurationImpl(SiteConfiguration.empty().withOverrides(overrides).build());

    // For this case need to compact three files and the highest ratio that achieves that is 1.8
    var planner = createPlanner(conf, executors);
    var all = createCFs(1000, 1.1, 1.9, 1.8, 1.6, 1.3, 1.4, 1.3, 1.2, 1.1);
    var params = createPlanningParams(all, all, Set.of(), 3, CompactionKind.SYSTEM, conf);
    var plan = planner.makePlan(params);
    var job = getOnlyElement(plan.getJobs());
    assertEquals(createCFs(1000, 1.1, 1.9, 1.8), job.getFiles());

    // For this case need to compact two files and the highest ratio that achieves that is 2.9
    all = createCFs(1000, 2, 2.9, 2.8, 2.7, 2.6, 2.5, 2.4, 2.3);
    params = createPlanningParams(all, all, Set.of(), 3, CompactionKind.SYSTEM, conf);
    plan = planner.makePlan(params);
    job = getOnlyElement(plan.getJobs());
    assertEquals(createCFs(1000, 2, 2.9), job.getFiles());

    all =
        createCFs(1000, 1.1, 2.89, 2.85, 2.7, 2.3, 2.9, 2.8, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2);
    params = createPlanningParams(all, all, Set.of(), 3, CompactionKind.SYSTEM, conf);
    plan = planner.makePlan(params);
    job = getOnlyElement(plan.getJobs());
    assertEquals(createCFs(1000, 1.1, 2.89, 2.85, 2.7, 2.3, 2.9), job.getFiles());

    all = createCFs(1000, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 1.1);
    params = createPlanningParams(all, all, Set.of(), 3, CompactionKind.SYSTEM, conf);
    plan = planner.makePlan(params);
    job = getOnlyElement(plan.getJobs());
    assertEquals(createCFs(1000, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9), job.getFiles());

    // In this case the tablet can not be brought below the max files limit in a single compaction,
    // so it should find the highest ratio to compact
    for (var ratio : List.of(1.9, 2.0, 3.0, 4.0)) {
      all = createCFs(1000, 1.9, 1.8, 1.7, 1.6, 1.5, 1.4, 1.5, 1.2, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1,
          1.1, 1.1);
      params = createPlanningParams(all, all, Set.of(), ratio, CompactionKind.SYSTEM, conf);
      plan = planner.makePlan(params);
      job = getOnlyElement(plan.getJobs());
      assertEquals(createCFs(1000, 1.9), job.getFiles());
    }

    // In this case the tablet can be brought below the max limit in single compaction, so it should
    // find this
    all =
        createCFs(1000, 1.9, 1.8, 1.7, 1.6, 1.5, 1.4, 1.5, 1.2, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1);
    params = createPlanningParams(all, all, Set.of(), 3, CompactionKind.SYSTEM, conf);
    plan = planner.makePlan(params);
    job = getOnlyElement(plan.getJobs());
    assertEquals(createCFs(1000, 1.9, 1.8, 1.7, 1.6, 1.5, 1.4, 1.5, 1.2, 1.1), job.getFiles());

    // each file is 10x the size of the file smaller than it
    all = createCFs(10, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1);
    params = createPlanningParams(all, all, Set.of(), 3, CompactionKind.SYSTEM, conf);
    plan = planner.makePlan(params);
    job = getOnlyElement(plan.getJobs());
    assertEquals(createCFs(10, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1), job.getFiles());

    // test with some files growing 20x, ensure those are not included
    for (var ratio : List.of(1.9, 2.0, 3.0, 4.0)) {
      all = createCFs(10, 1.05, 1.05, 1.25, 1.75, 1.25, 1.05, 1.05, 1.05);
      params = createPlanningParams(all, all, Set.of(), ratio, CompactionKind.SYSTEM, conf);
      plan = planner.makePlan(params);
      job = getOnlyElement(plan.getJobs());
      assertEquals(createCFs(10, 1.05, 1.05, 1.25, 1.75), job.getFiles());
    }

  }

  @Test
  public void testMaxTabletFilesNoCompaction() {
    String executors = "[{'name':'small','type': 'internal','maxSize':'32M','numThreads':1},"
        + "{'name':'medium','type': 'internal','maxSize':'128M','numThreads':2},"
        + "{'name':'large','type': 'internal','maxSize':'512M','numThreads':3}]";

    Map<String,String> overrides = new HashMap<>();
    overrides.put(Property.TSERV_COMPACTION_SERVICE_PREFIX.getKey() + "cs1.planner.opts.maxOpen",
        "10");
    overrides.put(Property.TABLE_FILE_MAX.getKey(), "7");
    var conf = new ConfigurationImpl(SiteConfiguration.empty().withOverrides(overrides).build());

    // ensure that when a compaction would be over the max size limit that it is not planned
    var planner = createPlanner(conf, executors);
    var all = createCFs(1_000_000_000, 2, 2, 2, 2, 2, 2, 2);
    var params = createPlanningParams(all, all, Set.of(), 3, CompactionKind.SYSTEM, conf);
    var plan = planner.makePlan(params);

    assertTrue(plan.getJobs().isEmpty());

    // ensure when a compaction is running and we are over files max but below the compaction ratio
    // that a compaction is not planned
    all = createCFs(1_000, 2, 2, 2, 2, 2, 2, 2);
    var job = new CompactionJobImpl((short) 1, CompactionExecutorIdImpl.externalId("ee1"),
        createCFs("F1", "1000"), CompactionKind.SYSTEM, Optional.of(false));
    params = createPlanningParams(all, all, Set.of(job), 3, CompactionKind.SYSTEM, conf);
    plan = planner.makePlan(params);

    assertTrue(plan.getJobs().isEmpty());

    // a really bad situation, each file is 20 times the size of its smaller file. The algorithm
    // does not search that for ratios that low.
    all = createCFs(10, 1.05, 1.05, 1.05, 1.05, 1.05, 1.05, 1.05, 1.05);
    params = createPlanningParams(all, all, Set.of(), 3, CompactionKind.SYSTEM, conf);
    plan = planner.makePlan(params);
    assertTrue(plan.getJobs().isEmpty());
  }

  // Test to ensure that plugin falls back from TABLE_FILE_MAX to TSERV_SCAN_MAX_OPENFILES
  @Test
  public void testMaxTableFilesFallback() {
    String executors = "[{'name':'small','type': 'internal','maxSize':'32M','numThreads':1},"
        + "{'name':'medium','type': 'internal','maxSize':'128M','numThreads':2},"
        + "{'name':'large','type': 'internal','numThreads':3}]";

    Map<String,String> overrides = new HashMap<>();
    overrides.put(Property.TSERV_COMPACTION_SERVICE_PREFIX.getKey() + "cs1.planner.opts.maxOpen",
        "10");
    overrides.put(Property.TABLE_FILE_MAX.getKey(), "0");
    overrides.put(Property.TSERV_SCAN_MAX_OPENFILES.getKey(), "5");
    var conf = new ConfigurationImpl(SiteConfiguration.empty().withOverrides(overrides).build());

    var planner = createPlanner(conf, executors);
    var all = createCFs(1000, 1.9, 1.8, 1.7, 1.6, 1.5, 1.4, 1.3, 1.2, 1.1);
    var params = createPlanningParams(all, all, Set.of(), 3, CompactionKind.SYSTEM, conf);
    var plan = planner.makePlan(params);
    var job = getOnlyElement(plan.getJobs());
    assertEquals(createCFs(1000, 1.9, 1.8, 1.7, 1.6, 1.5, 1.4), job.getFiles());
  }

  private CompactionJob createJob(CompactionKind kind, Set<CompactableFile> all,
      Set<CompactableFile> files) {
    return new CompactionPlanImpl.BuilderImpl(kind, all, all)
        .addJob((short) all.size(), CompactionExecutorIdImpl.internalId(csid, "small"), files)
        .build().getJobs().iterator().next();
  }

  // Create a set of files whose sizes would require certain compaction ratios to compact
  private Set<CompactableFile> createCFs(int initialSize, double... desiredRatios) {
    List<String> pairs = new ArrayList<>();
    pairs.add("F1");
    pairs.add(initialSize + "");

    double previousFileSizes = initialSize;

    int i = 2;
    for (double desiredRatio : desiredRatios) {
      Preconditions.checkArgument(desiredRatio > 1.0);
      Preconditions.checkArgument(desiredRatio <= i);

      /*
       * The compaction ratio formula is fileSize * ratio < fileSize + previousFileSizes. Solved the
       * following equation to compute a file size given a desired ratio.
       *
       * fileSize * ratio = fileSize + previousFileSizes
       *
       * fileSize * ratio - fileSize = previousFileSizes
       *
       * fileSize * (ratio - 1) = previousFileSizes
       *
       * fileSize = previousFileSizes / (ratio - 1)
       */

      double fileSize = previousFileSizes / (desiredRatio - 1);
      pairs.add("F" + i + "_" + desiredRatio);
      pairs.add(Math.round(fileSize) + "");

      previousFileSizes += fileSize;
      i++;
    }

    return createCFs(pairs.toArray(new String[0]));
  }

  private static Set<CompactableFile> createCFs(String... namesSizePairs) {
    Set<CompactableFile> files = new HashSet<>();

    for (int i = 0; i < namesSizePairs.length; i += 2) {
      String name = namesSizePairs[i];
      long size = ConfigurationTypeHelper.getFixedMemoryAsBytes(namesSizePairs[i + 1]);
      try {
        files.add(CompactableFile
            .create(new URI("hdfs://fake/accumulo/tables/1/t-0000000z/" + name + ".rf"), size, 0));
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
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
        .map(path -> path.split("/")).map(t -> t[t.length - 1]).collect(Collectors.toSet());
    var resultNames = result.stream().map(CompactableFile::getUri).map(URI::getPath)
        .map(path -> path.split("/")).map(t -> t[t.length - 1]).collect(Collectors.toSet());
    assertEquals(expectedNames, resultNames);
  }

  private static CompactionPlanner.PlanningParameters createPlanningParams(Set<CompactableFile> all,
      Set<CompactableFile> candidates, Set<CompactionJob> compacting, double ratio,
      CompactionKind kind) {
    return createPlanningParams(all, candidates, compacting, ratio, kind, defaultConf);
  }

  private static CompactionPlanner.PlanningParameters createPlanningParams(Set<CompactableFile> all,
      Set<CompactableFile> candidates, Set<CompactionJob> compacting, double ratio,
      CompactionKind kind, Configuration conf) {
    return new CompactionPlanner.PlanningParameters() {

      @Override
      public TableId getTableId() {
        return TableId.of("42");
      }

      @Override
      public ServiceEnvironment getServiceEnvironment() {
        ServiceEnvironment senv = EasyMock.createMock(ServiceEnvironment.class);
        EasyMock.expect(senv.getConfiguration()).andReturn(conf).anyTimes();
        EasyMock.expect(senv.getConfiguration(TableId.of("42"))).andReturn(conf).anyTimes();
        EasyMock.replay(senv);
        return senv;
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

  private static CompactionPlanner.InitParameters getInitParams(Configuration conf,
      String executors) {

    String maxOpen =
        conf.get(Property.TSERV_COMPACTION_SERVICE_PREFIX.getKey() + "cs1.planner.opts.maxOpen");
    Map<String,String> options = new HashMap<>();
    options.put("executors", executors.replaceAll("'", "\""));

    if (maxOpen != null) {
      options.put("maxOpen", maxOpen);
    }

    ServiceEnvironment senv = EasyMock.createMock(ServiceEnvironment.class);
    EasyMock.expect(senv.getConfiguration()).andReturn(conf).anyTimes();
    EasyMock.replay(senv);

    return new CompactionPlannerInitParams(csid, options, senv);
  }

  private static DefaultCompactionPlanner createPlanner(Configuration conf, String executors) {
    DefaultCompactionPlanner planner = new DefaultCompactionPlanner();
    var initParams = getInitParams(conf, executors);

    planner.init(initParams);
    return planner;
  }
}
