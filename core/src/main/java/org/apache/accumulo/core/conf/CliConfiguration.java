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

package org.apache.accumulo.core.conf;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CliConfiguration {

  private static final Logger log = LoggerFactory.getLogger(CliConfiguration.class);
  private static volatile Map<String,String> config = new HashMap<>();

  /**
   * Sets CliConfiguration with map of configuration. Additional calls will overwrite existing properties and values.
   *
   * @param conf
   *          Map of configuration
   */
  public static void set(Map<String,String> conf) {
    Objects.requireNonNull(conf);
    config = conf;
  }

  public static void print() {
    log.info("The following configuration was set on the command line:");
    for (Map.Entry<String,String> entry : config.entrySet()) {
      String key = entry.getKey();
      log.info(key + " = " + (Property.isSensitive(key) ? "<hidden>" : entry.getValue()));
    }
  }

  public static String get(Property property) {
    return config.get(property.getKey());
  }

  public static void getProperties(Map<String,String> props, Predicate<String> filter) {
    for (Map.Entry<String,String> entry : config.entrySet()) {
      if (filter.test(entry.getKey())) {
        props.put(entry.getKey(), entry.getValue());
      }
    }
  }
}
