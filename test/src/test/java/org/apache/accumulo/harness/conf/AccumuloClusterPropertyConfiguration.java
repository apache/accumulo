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
package org.apache.accumulo.harness.conf;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.accumulo.harness.AccumuloClusterIT.ClusterType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Base class for extracting configuration values from Java Properties
 */
public abstract class AccumuloClusterPropertyConfiguration implements AccumuloClusterConfiguration {
  private static final Logger log = LoggerFactory.getLogger(AccumuloClusterPropertyConfiguration.class);

  public static final String ACCUMULO_IT_PROPERTIES_FILE = "accumulo.it.properties";

  public static final String ACCUMULO_CLUSTER_TYPE_KEY = "accumulo.it.cluster.type";

  public static final String ACCUMULO_MINI_PREFIX = "accumulo.it.cluster.mini.";
  public static final String ACCUMULO_STANDALONE_PREFIX = "accumulo.it.cluster.standalone.";

  protected ClusterType clusterType;

  public static AccumuloClusterPropertyConfiguration get() {
    Properties systemProperties = System.getProperties();

    String clusterTypeValue = null;
    String propertyFile = systemProperties.getProperty(ACCUMULO_IT_PROPERTIES_FILE);

    if (null != propertyFile) {
      // Check for properties provided in a file
      File f = new File(propertyFile);
      if (f.exists() && f.isFile() && f.canRead()) {
        Properties fileProperties = new Properties();
        FileReader reader = null;
        try {
          reader = new FileReader(f);
        } catch (FileNotFoundException e) {
          log.warn("Could not read properties from specified file: {}", propertyFile, e);
        }

        if (null != reader) {
          try {
            fileProperties.load(reader);
          } catch (IOException e) {
            log.warn("Could not load properties from specified file: {}", propertyFile, e);
          } finally {
            try {
              reader.close();
            } catch (IOException e) {
              log.warn("Could not close reader", e);
            }
          }

          clusterTypeValue = fileProperties.getProperty(ACCUMULO_CLUSTER_TYPE_KEY);
        }
      } else {
        log.debug("Property file ({}) is not a readable file", propertyFile);
      }
    } else {
      log.debug("No properties file found in {}", ACCUMULO_IT_PROPERTIES_FILE);
    }

    if (null == clusterTypeValue) {
      clusterTypeValue = systemProperties.getProperty(ACCUMULO_CLUSTER_TYPE_KEY);
    }

    ClusterType type;
    if (null == clusterTypeValue) {
      type = ClusterType.MINI;
    } else {
      type = ClusterType.valueOf(clusterTypeValue);
    }

    log.info("Using {} cluster type from system properties", type);

    switch (type) {
      case MINI:
        return new AccumuloMiniClusterConfiguration();
      case STANDALONE:
        return new StandaloneAccumuloClusterConfiguration();
      default:
        throw new RuntimeException("Clusters other than MiniAccumuloCluster are not yet implemented");
    }
  }

  public Map<String,String> getConfiguration(ClusterType type) {
    Preconditions.checkNotNull(type);

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

    Map<String,String> configuration = new HashMap<String,String>();

    Properties systemProperties = System.getProperties();

    String propertyFile = systemProperties.getProperty(ACCUMULO_IT_PROPERTIES_FILE);

    // Check for properties provided in a file
    if (null != propertyFile) {
      File f = new File(propertyFile);
      if (f.exists() && f.isFile() && f.canRead()) {
        Properties fileProperties = new Properties();
        FileReader reader = null;
        try {
          reader = new FileReader(f);
        } catch (FileNotFoundException e) {
          log.warn("Could not read properties from specified file: {}", propertyFile, e);
        }

        if (null != reader) {
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
    loadFromProperties(prefix, systemProperties, configuration);

    return configuration;
  }

  protected void loadFromProperties(String desiredPrefix, Properties properties, Map<String,String> configuration) {
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
