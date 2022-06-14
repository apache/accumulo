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
package org.apache.accumulo.harness.conf;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.accumulo.harness.AccumuloClusterHarness.ClusterType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Base class for extracting configuration values from Java Properties
 */
public abstract class AccumuloClusterPropertyConfiguration implements AccumuloClusterConfiguration {
  private static final Logger log =
      LoggerFactory.getLogger(AccumuloClusterPropertyConfiguration.class);

  public static final String ACCUMULO_IT_PROPERTIES_FILE = "accumulo.it.properties";
  public static final String ACCUMULO_CLUSTER_TYPE_KEY = "accumulo.it.cluster.type";

  public static final String ACCUMULO_MINI_PREFIX = "accumulo.it.cluster.mini.";
  public static final String ACCUMULO_STANDALONE_PREFIX = "accumulo.it.cluster.standalone.";

  public static final String ACCUMULO_CLUSTER_CLIENT_CONF_KEY = "accumulo.it.cluster.clientconf";

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "path provided by test")
  public static AccumuloClusterPropertyConfiguration get() {

    String clusterTypeValue = null, clientConf = null;
    String propertyFile = System.getProperty(ACCUMULO_IT_PROPERTIES_FILE);

    if (propertyFile != null) {
      // Check for properties provided in a file
      File f = new File(propertyFile);
      Properties fileProperties = new Properties();
      try (FileReader reader = new FileReader(f, UTF_8)) {
        fileProperties.load(reader);
        clusterTypeValue = fileProperties.getProperty(ACCUMULO_CLUSTER_TYPE_KEY);
        clientConf = fileProperties.getProperty(ACCUMULO_CLUSTER_CLIENT_CONF_KEY);
      } catch (IOException e) {
        throw new RuntimeException("Could not read properties from file: " + propertyFile, e);
      }
    } else {
      log.debug("No properties file found in {}", ACCUMULO_IT_PROPERTIES_FILE);
    }

    if (clusterTypeValue == null) {
      clusterTypeValue = System.getProperty(ACCUMULO_CLUSTER_TYPE_KEY);
    }

    if (clientConf == null) {
      clientConf = System.getProperty(ACCUMULO_CLUSTER_CLIENT_CONF_KEY);
    }

    ClusterType type;
    if (clusterTypeValue == null) {
      type = ClusterType.MINI;
    } else {
      type = ClusterType.valueOf(clusterTypeValue);
    }

    log.info("Using {} cluster type from system properties", type);

    switch (type) {
      case MINI:
        // we'll let no client conf pass through and expect that the caller will set it after MAC is
        // started
        return new AccumuloMiniClusterConfiguration();
      case STANDALONE:
        if (clientConf == null) {
          throw new RuntimeException(
              "Expected client configuration to be provided: " + ACCUMULO_CLUSTER_CLIENT_CONF_KEY);
        }
        File clientConfFile = new File(clientConf);
        if (!clientConfFile.exists() || !clientConfFile.isFile()) {
          throw new RuntimeException(
              "Client configuration should be a normal file: " + clientConfFile);
        }
        return new StandaloneAccumuloClusterConfiguration(clientConfFile);
      default:
        throw new RuntimeException(
            "Clusters other than MiniAccumuloCluster are not yet implemented");
    }
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "path provided by test")
  public Map<String,String> getConfiguration(ClusterType type) {
    requireNonNull(type);

    String prefix;
    switch (type) {
      case MINI:
        prefix = ACCUMULO_MINI_PREFIX;
        break;
      case STANDALONE:
        prefix = ACCUMULO_STANDALONE_PREFIX;
        break;
      default:
        throw new IllegalArgumentException("Unknown ClusterType: " + type);
    }

    Map<String,String> configuration = new HashMap<>();

    String propertyFile = System.getProperty(ACCUMULO_IT_PROPERTIES_FILE);

    // Check for properties provided in a file
    if (propertyFile != null) {
      File f = new File(propertyFile);
      if (f.exists() && f.isFile() && f.canRead()) {
        Properties fileProperties = new Properties();
        FileReader reader = null;
        try {
          reader = new FileReader(f, UTF_8);
        } catch (IOException e) {
          log.warn("Could not read properties from specified file: {}", propertyFile, e);
        }

        if (reader != null) {
          try {
            fileProperties.load(reader);
            loadFromProperties(prefix, fileProperties, configuration);
          } catch (IOException e) {
            log.warn("Could not load properties from specified file: {}", propertyFile, e);
          } finally {
            try {
              reader.close();
            } catch (IOException e) {
              log.warn("Could not close reader", e);
            }
          }
        }
      }
    }

    // Load any properties specified directly in the system properties
    loadFromProperties(prefix, System.getProperties(), configuration);

    return configuration;
  }

  protected void loadFromProperties(String desiredPrefix, Properties properties,
      Map<String,String> configuration) {
    for (Entry<Object,Object> entry : properties.entrySet()) {
      if (!(entry.getKey() instanceof String)) {
        continue;
      }

      String key = (String) entry.getKey();
      if (key.startsWith(desiredPrefix)) {
        configuration.put(key, (String) entry.getValue());
      }
    }
  }
}
