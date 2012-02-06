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
package org.apache.accumulo.examples.wikisearch.ingest;

import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.SimpleAnalyzer;

public class WikipediaConfiguration {
  public final static String INSTANCE_NAME = "wikipedia.accumulo.instance_name";
  public final static String USER = "wikipedia.accumulo.user";
  public final static String PASSWORD = "wikipedia.accumulo.password";
  public final static String TABLE_NAME = "wikipedia.accumulo.table";
  
  public final static String ZOOKEEPERS = "wikipedia.accumulo.zookeepers";
  
  public final static String NAMESPACES_FILENAME = "wikipedia.namespaces.filename";
  public final static String LANGUAGES_FILENAME = "wikipedia.languages.filename";
  public final static String WORKING_DIRECTORY = "wikipedia.ingest.working";
  
  public final static String ANALYZER = "wikipedia.index.analyzer";
  
  public final static String NUM_PARTITIONS = "wikipedia.ingest.partitions";

  public final static String NUM_GROUPS = "wikipedia.ingest.groups";

  
  public static String getUser(Configuration conf) {
    return conf.get(USER);
  };
  
  public static byte[] getPassword(Configuration conf) {
    String pass = conf.get(PASSWORD);
    if (pass == null) {
      return null;
    }
    return pass.getBytes();
  }
  
  public static String getTableName(Configuration conf) {
    String tablename = conf.get(TABLE_NAME);
    if (tablename == null) {
      throw new RuntimeException("No data table name specified in " + TABLE_NAME);
    }
    return tablename;
  }
  
  public static String getInstanceName(Configuration conf) {
    return conf.get(INSTANCE_NAME);
  }
  
  public static String getZookeepers(Configuration conf) {
    String zookeepers = conf.get(ZOOKEEPERS);
    if (zookeepers == null) {
      throw new RuntimeException("No zookeepers specified in " + ZOOKEEPERS);
    }
    return zookeepers;
  }
  
  public static Path getNamespacesFile(Configuration conf) {
    String filename = conf.get(NAMESPACES_FILENAME, new Path(getWorkingDirectory(conf), "namespaces.dat").toString());
    return new Path(filename);
  }
  
  public static Path getLanguagesFile(Configuration conf) {
    String filename = conf.get(LANGUAGES_FILENAME, new Path(getWorkingDirectory(conf), "languages.txt").toString());
    return new Path(filename);
  }
  
  public static Path getWorkingDirectory(Configuration conf) {
    String filename = conf.get(WORKING_DIRECTORY);
    return new Path(filename);
  }
  
  public static Analyzer getAnalyzer(Configuration conf) throws IOException {
    Class<? extends Analyzer> analyzerClass = conf.getClass(ANALYZER, SimpleAnalyzer.class, Analyzer.class);
    return ReflectionUtils.newInstance(analyzerClass, conf);
  }
  
  public static Connector getConnector(Configuration conf) throws AccumuloException, AccumuloSecurityException {
    return getInstance(conf).getConnector(getUser(conf), getPassword(conf));
  }
  
  public static Instance getInstance(Configuration conf) {
    return new ZooKeeperInstance(getInstanceName(conf), getZookeepers(conf));
  }
  
  public static int getNumPartitions(Configuration conf) {
    return conf.getInt(NUM_PARTITIONS, 25);
  }
  
  public static int getNumGroups(Configuration conf) {
    return conf.getInt(NUM_GROUPS, 1);
  }
  
  /**
   * Helper method to get properties from Hadoop configuration
   * 
   * @param <T>
   * @param conf
   * @param propertyName
   * @param resultClass
   * @throws IllegalArgumentException
   *           if property is not defined, null, or empty. Or if resultClass is not handled.
   * @return value of property
   */
  @SuppressWarnings("unchecked")
  public static <T> T isNull(Configuration conf, String propertyName, Class<T> resultClass) {
    String p = conf.get(propertyName);
    if (StringUtils.isEmpty(p))
      throw new IllegalArgumentException(propertyName + " must be specified");
    
    if (resultClass.equals(String.class))
      return (T) p;
    else if (resultClass.equals(String[].class))
      return (T) conf.getStrings(propertyName);
    else if (resultClass.equals(Boolean.class))
      return (T) Boolean.valueOf(p);
    else if (resultClass.equals(Long.class))
      return (T) Long.valueOf(p);
    else if (resultClass.equals(Integer.class))
      return (T) Integer.valueOf(p);
    else if (resultClass.equals(Float.class))
      return (T) Float.valueOf(p);
    else if (resultClass.equals(Double.class))
      return (T) Double.valueOf(p);
    else
      throw new IllegalArgumentException(resultClass.getSimpleName() + " is unhandled.");
    
  }
  
}
