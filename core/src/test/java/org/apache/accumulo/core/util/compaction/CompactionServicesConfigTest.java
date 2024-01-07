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

import java.util.Map;

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner;
import org.junit.jupiter.api.Test;

public class CompactionServicesConfigTest {

  private final Property prefix = Property.COMPACTION_SERVICE_PREFIX;

  @Test
  public void testCompactionProps() {
    ConfigurationCopy conf = new ConfigurationCopy();

    conf.set(prefix.getKey() + "default.planner", DefaultCompactionPlanner.class.getName());
    conf.set(prefix.getKey() + "default.planner.opts.maxOpen", "10");
    conf.set(prefix.getKey() + "default.planner.opts.groups",
        "[{'name':'small','maxSize':'32M'},{'name':'medium','maxSize':'128M'},{'name':'large'}]");

    conf.set(prefix.getKey() + "default.planner.opts.validProp", "1");

    var compactionConfig = new CompactionServicesConfig(conf);
    assertEquals(Map.of("maxOpen", "10", "groups",
        "[{'name':'small','maxSize':'32M'},{'name':'medium','maxSize':'128M'},{'name':'large'}]",
        "validProp", "1"), compactionConfig.getOptions().get("default"));
  }

  @Test
  public void testCompactionRateLimits() {
    ConfigurationCopy conf = new ConfigurationCopy();
    CompactionServicesConfig compactionConfig;

    conf.set(prefix.getKey() + "cs1.planner", DefaultCompactionPlanner.class.getName());
    conf.set(prefix.getKey() + "cs1.rate.limit", "2M");
    compactionConfig = new CompactionServicesConfig(conf);
    assertEquals(2097152, compactionConfig.getRateLimits().get("cs1"));
  }
}
