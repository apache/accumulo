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
package org.apache.accumulo.server.test.randomwalk;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Properties;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.log4j.Logger;

public class State {
  
  private static final Logger log = Logger.getLogger(State.class);
  private HashMap<String,Object> stateMap = new HashMap<String,Object>();
  private Properties props;
  private int numVisits = 0;
  private int maxVisits = Integer.MAX_VALUE;
  
  private MultiTableBatchWriter mtbw = null;
  private Connector connector = null;
  private Instance instance = null;
  
  State(Properties props) {
    this.props = props;
  }
  
  public void setMaxVisits(int num) {
    maxVisits = num;
  }
  
  public void visitedNode() throws Exception {
    numVisits++;
    if (numVisits > maxVisits) {
      log.debug("Visited max number (" + maxVisits + ") of nodes");
      throw new Exception("Visited max number (" + maxVisits + ") of nodes");
    }
  }
  
  public String getPid() {
    return ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
  }
  
  public void set(String key, Object value) {
    stateMap.put(key, value);
  }
  
  public Object get(String key) {
    if (stateMap.containsKey(key) == false) {
      throw new RuntimeException("State does not contain " + key);
    }
    return stateMap.get(key);
  }
  
  public String getString(String key) {
    return (String) stateMap.get(key);
  }
  
  public Integer getInteger(String key) {
    return (Integer) stateMap.get(key);
  }
  
  public String getProperty(String key) {
    return props.getProperty(key);
  }
  
  public Connector getConnector() throws Exception {
    if (connector == null) {
      String instance = props.getProperty("INSTANCE");
      String zookeepers = props.getProperty("ZOOKEEPERS");
      String username = props.getProperty("USERNAME");
      String password = props.getProperty("PASSWORD");
      connector = new ZooKeeperInstance(instance, zookeepers).getConnector(username, password.getBytes());
    }
    return connector;
  }
  
  public Instance getInstance() {
    if (instance == null) {
      instance = HdfsZooInstance.getInstance();
    }
    return instance;
  }
  
  public MultiTableBatchWriter getMultiTableBatchWriter() {
    if (mtbw == null) {
      long maxMem = Long.parseLong(props.getProperty("MAX_MEM"));
      int maxLatency = Integer.parseInt(props.getProperty("MAX_LATENCY"));
      int numThreads = Integer.parseInt(props.getProperty("NUM_THREADS"));
      mtbw = connector.createMultiTableBatchWriter(maxMem, maxLatency, numThreads);
    }
    return mtbw;
  }
  
  public String getMapReduceJars() {
    
    String acuHome = System.getenv("ACCUMULO_HOME");
    String zkHome = System.getenv("ZOOKEEPER_HOME");
    
    if (acuHome == null || zkHome == null) {
      throw new RuntimeException("ACCUMULO or ZOOKEEPER home not set!");
    }
    
    String retval = null;
    
    File zkLib = new File(zkHome);
    String[] files = zkLib.list();
    for (int i = 0; i < files.length; i++) {
      String f = files[i];
      if (f.matches("^zookeeper-.+jar$")) {
        if (retval == null) {
          retval = String.format("%s/%s", zkLib.getAbsolutePath(), f);
        } else {
          retval += String.format(",%s/%s", zkLib.getAbsolutePath(), f);
        }
      }
    }
    
    File libdir = new File(acuHome + "/lib");
    files = libdir.list();
    for (int i = 0; i < files.length; i++) {
      String f = files[i];
      if (f.matches("^accumulo-core-.+jar$") || f.matches("^accumulo-server-.+jar$") || f.matches("^cloudtrace-.+jar$") || f.matches("^libthrift-.+jar$")) {
        if (retval == null) {
          retval = String.format("%s/%s", libdir.getAbsolutePath(), f);
        } else {
          retval += String.format(",%s/%s", libdir.getAbsolutePath(), f);
        }
      }
    }
    
    return retval;
  }
}
