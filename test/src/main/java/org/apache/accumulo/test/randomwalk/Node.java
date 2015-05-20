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
package org.apache.accumulo.test.randomwalk;

import java.io.File;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * Represents a point in graph of RandomFramework
 */
public abstract class Node {

  protected final Logger log = Logger.getLogger(this.getClass());
  long progress = System.currentTimeMillis();

  /**
   * Visits node
   *
   * @param state
   *          Random walk state passed between nodes
   * @param env
   *          test environment
   */
  public abstract void visit(State state, Environment env, Properties props) throws Exception;

  @Override
  public boolean equals(Object o) {
    if (o == null)
      return false;
    return toString().equals(o.toString());
  }

  @Override
  public String toString() {
    return this.getClass().getName();
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }

  synchronized public void makingProgress() {
    progress = System.currentTimeMillis();
  }

  synchronized public long lastProgress() {
    return progress;
  }

  protected String getMapReduceJars() {

    String acuHome = System.getenv("ACCUMULO_HOME");
    String zkHome = System.getenv("ZOOKEEPER_HOME");

    if (acuHome == null || zkHome == null) {
      throw new RuntimeException("ACCUMULO or ZOOKEEPER home not set!");
    }

    String retval = null;

    File zkLib = new File(zkHome);
    String[] files = zkLib.list();
    if (files != null) {
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
    }

    File libdir = new File(acuHome + "/lib");
    for (String jar : "accumulo-core accumulo-server-base accumulo-fate accumulo-trace libthrift htrace-core".split(" ")) {
      retval += String.format(",%s/%s.jar", libdir.getAbsolutePath(), jar);
    }

    return retval;
  }
}
