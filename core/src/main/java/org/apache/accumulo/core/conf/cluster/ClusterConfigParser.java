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
package org.apache.accumulo.core.conf.cluster;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

public class ClusterConfigParser {

  public static Map<String,String> parseConfiguration(String configFile) throws IOException {
    Map<String,String> results = new HashMap<>();
    try (InputStream fis = Files.newInputStream(Paths.get(configFile), StandardOpenOption.READ)) {
      Yaml y = new Yaml();
      Map<String,Object> config = y.load(fis);
      config.forEach((k, v) -> flatten("", k, v, results));
    }
    return results;
  }

  private static String addTheDot(String key) {
    return (key.endsWith(".")) ? "" : ".";
  }

  @SuppressWarnings("unchecked")
  private static void flatten(String parentKey, String key, Object value,
      Map<String,String> results) {
    String parent = (parentKey == null || parentKey == "") ? "" : parentKey + addTheDot(parentKey);
    if (value instanceof String) {
      results.put(parent + key, (String) value);
      return;
    } else if (value instanceof List) {
      ((List<?>) value).forEach(l -> {
        if (l instanceof String) {
          // remove the [] at the ends of toString()
          String val = value.toString();
          results.put(parent + key, val.substring(1, val.length() - 1).replace(", ", ","));
          return;
        } else {
          flatten(parent, key, l, results);
        }
      });
    } else if (value instanceof Map) {
      ((Map<String,Object>) value).forEach((k, v) -> flatten(parent + key, k, v, results));
    } else {
      throw new RuntimeException("Unhandled object type: " + value.getClass());
    }
  }

  public static void main(String[] args) throws IOException {
    if (args == null || args.length != 1) {
      System.err.println("Usage: ClusterConfigParser <configFile>");
      System.exit(1);
    }
    parseConfiguration(args[0]).forEach((k, v) -> System.out.println(k + ":" + v));
  }

}
