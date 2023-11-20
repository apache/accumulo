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
package org.apache.accumulo.core.util.compaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner;
import org.junit.jupiter.api.Test;

@SuppressWarnings("deprecation")
public class CompactionServicesConfigTest {

  private final Property oldPrefix = Property.TSERV_COMPACTION_SERVICE_PREFIX;
  private final Property newPrefix = Property.COMPACTION_SERVICE_PREFIX;

  @Test
  public void testCompactionProps() {
    ConfigurationCopy conf = new ConfigurationCopy();

    conf.set("compaction.major.service.default.planner", DefaultCompactionPlanner.class.getName());
    conf.set("compaction.major.service.default.planner.opts.maxOpen", "10");
    conf.set("compaction.major.service.default.planner.opts.executors",
        "[{'name':'small','type':'internal','maxSize':'32M','numThreads':2},{'name':'medium','type':'internal','maxSize':'128M','numThreads':2},{'name':'large','type':'internal','numThreads':2}]");

    conf.set(oldPrefix.getKey() + "default.planner.opts.ignoredProp", "1");
    conf.set(newPrefix.getKey() + "default.planner.opts.validProp", "1");
    conf.set(oldPrefix.getKey() + "default.planner.opts.validProp", "a");

    var compactionConfig = new CompactionServicesConfig(conf);
    assertTrue(compactionConfig.getOptions().get("default").containsKey("validProp"));
    assertEquals("1", compactionConfig.getOptions().get("default").get("validProp"));
    assertNull(compactionConfig.getOptions().get("default").get("ignoredProp"));
  }

  @Test
  public void testDuplicateCompactionPlannerDefs() {
    ConfigurationCopy conf = new ConfigurationCopy();

    String planner = DefaultCompactionPlanner.class.getName();
    String oldPlanner = "OldPlanner";

    conf.set(newPrefix.getKey() + "default.planner", planner);
    conf.set(oldPrefix.getKey() + "default.planner", oldPlanner);

    conf.set(oldPrefix.getKey() + "old.planner", oldPlanner);

    var compactionConfig = new CompactionServicesConfig(conf);

assertEquals(Map.of("default",planner,"old",oldPlanner), compactionConfig.getPlanners());
  }

  @Test
  public void testCompactionPlannerOldDef() {
    ConfigurationCopy conf = new ConfigurationCopy();

    conf.set(oldPrefix.getKey() + "cs1.planner", DefaultCompactionPlanner.class.getName());
    conf.set(oldPrefix.getKey() + "cs1.planner.opts.maxOpen", "10");
    conf.set(oldPrefix.getKey() + "cs1.planner.opts.executors",
        "[{'name':'small','type':'internal','maxSize':'32M','numThreads':2},{'name':'medium','type':'internal','maxSize':'128M','numThreads':2},{'name':'large','type':'internal','numThreads':2}]");
    conf.set(oldPrefix.getKey() + "cs1.planner.opts.foo", "1");
    conf.set(newPrefix.getKey() + "cs1.planner.opts.bar", "2");

    var compactionConfig = new CompactionServicesConfig(conf);
    assertTrue(compactionConfig.getOptions().get("cs1").containsKey("foo"));
    assertEquals("1", compactionConfig.getOptions().get("cs1").get("foo"));

    assertTrue(compactionConfig.getOptions().get("cs1").containsKey("bar"));
    assertEquals("2", compactionConfig.getOptions().get("cs1").get("bar"));
  }

  @Test
  public void testCompactionRateLimits() {
    ConfigurationCopy conf = new ConfigurationCopy();
    CompactionServicesConfig compactionConfig;

    conf.set(oldPrefix.getKey() + "cs1.planner", DefaultCompactionPlanner.class.getName());
    conf.set(oldPrefix.getKey() + "cs1.rate.limit", "2M");
    compactionConfig = new CompactionServicesConfig(conf);
    assertEquals(2097152, compactionConfig.getRateLimits().get("cs1"));

    // Test newPrefix property override
    conf.set(newPrefix.getKey() + "cs1.rate.limit", "4M");
    compactionConfig = new CompactionServicesConfig(conf);
    assertEquals(4194304, compactionConfig.getRateLimits().get("cs1"));
  }
}
