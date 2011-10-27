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
package org.apache.accumulo.server.util;

import java.util.List;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.zookeeper.IZooReaderWriter;
import org.apache.accumulo.server.zookeeper.ZooLock;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;

public class ZooZap {
  
  static boolean verbose = false;
  
  /**
   * @param args
   */
  private static void message(String msg) {
    if (verbose)
      System.out.println(msg);
  }
  
  public static void main(String[] args) {
    
    boolean zapMaster = false;
    boolean zapTservers = false;
    boolean zapLoggers = false;
    boolean zapTracers = false;
    
    if (args.length == 0) {
      printUsage();
      return;
    }
    
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-tservers")) {
        zapTservers = true;
      } else if (args[i].equals("-master")) {
        zapMaster = true;
      } else if (args[i].equals("-loggers")) {
        zapLoggers = true;
      } else if (args[i].equals("-tracers")) {
        zapTracers = true;
      } else if (args[i].equals("-verbose")) {
        verbose = true;
      } else {
        printUsage();
        return;
      }
    }
    
    IZooReaderWriter zoo = ZooReaderWriter.getInstance();
    String iid = HdfsZooInstance.getInstance().getInstanceID();
    
    if (zapMaster) {
      String masterLockPath = Constants.ZROOT + "/" + iid + Constants.ZMASTER_LOCK;
      
      zapDirectory(zoo, masterLockPath);
    }
    
    if (zapTservers) {
      String tserversPath = Constants.ZROOT + "/" + iid + Constants.ZTSERVERS;
      try {
        List<String> children = zoo.getChildren(tserversPath);
        for (String child : children) {
          message("Deleting " + tserversPath + "/" + child + " from zookeeper");
          
          if (zapMaster)
            ZooReaderWriter.getInstance().recursiveDelete(tserversPath + "/" + child, NodeMissingPolicy.SKIP);
          else {
            String path = tserversPath + "/" + child;
            if (zoo.getChildren(path).size() > 0) {
              if (!ZooLock.deleteLock(path, "tserver")) {
                message("Did not delete " + tserversPath + "/" + child);
              }
            }
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    
    if (zapLoggers) {
      String loggersPath = Constants.ZROOT + "/" + iid + Constants.ZLOGGERS;
      zapDirectory(zoo, loggersPath);
    }
    
    if (zapTracers) {
      String loggersPath = Constants.ZROOT + "/" + iid + Constants.ZTRACERS;
      zapDirectory(zoo, loggersPath);
    }
    
  }
  
  private static void zapDirectory(IZooReaderWriter zoo, String loggersPath) {
    try {
      List<String> children = zoo.getChildren(loggersPath);
      for (String child : children) {
        message("Deleting " + loggersPath + "/" + child + " from zookeeper");
        zoo.recursiveDelete(loggersPath + "/" + child, NodeMissingPolicy.SKIP);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  private static void printUsage() {
    System.err.println("Usage : " + ZooZap.class.getName() + " [-verbose] [-tservers] [-master] [-loggers]");
  }
  
}
