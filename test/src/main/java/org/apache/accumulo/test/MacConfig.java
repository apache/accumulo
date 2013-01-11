/**
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
package org.apache.accumulo.test;

import java.io.File;
import java.util.Collections;
import java.util.Map;

/**
 * Holds configuration for {@link MiniAccumuloCluster}. Required configurations must be passed to constructor and all other configurations are optional.
 */

public class MacConfig {
  
  private File dir = null;
  private String rootPassword = null;
  private Map<String,String> siteConfig = Collections.emptyMap();
  private int numTservers = 2;
  
  /**
   * @param dir
   *          A local directory where mini accumulo cluster will store all of its data. This directory should be empty or non-existant.
   * @param rootPassword
   *          The initial password for the Accumulo root user
   */

  public MacConfig(File dir, String rootPassword) {
    this.dir = dir;
    this.rootPassword = rootPassword;
  }
  
  public File getDir() {
    return dir;
  }
  
  public String getRootPassword() {
    return rootPassword;
  }
  
  public int getNumTservers() {
    return numTservers;
  }

  /**
   * Calling this method is optional. If not set, it defaults to two.
   * 
   * @param numTservers
   *          the number of tablet servers that mini accumulo cluster should start
   * @return
   */
  
  public MacConfig setNumTservers(int numTservers) {
    if (numTservers < 1)
      throw new IllegalArgumentException("Must have at least one tablet server");
    this.numTservers = numTservers;
    return this;
  }

  public Map<String,String> getSiteConfig() {
    return siteConfig;
  }

  
  /**
   * Calling this method is optional. If not set, it defautls to an empty map.
   * 
   * @param siteConfig
   *          key/values that you normally put in accumulo-site.xml can be put here
   * @return
   */
  
  public MacConfig setSiteConfig(Map<String,String> siteConfig) {
    this.siteConfig = siteConfig;
    return this;
  }
}