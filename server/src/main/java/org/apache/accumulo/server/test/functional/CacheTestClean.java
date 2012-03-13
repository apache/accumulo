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
package org.apache.accumulo.server.test.functional;

import java.io.File;

import org.apache.accumulo.core.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.zookeeper.IZooReaderWriter;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;

public class CacheTestClean {
  
  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    String rootDir = args[0];
    File reportDir = new File(args[1]);
    
    IZooReaderWriter zoo = ZooReaderWriter.getInstance();
    
    if (zoo.exists(rootDir)) {
      zoo.recursiveDelete(rootDir, NodeMissingPolicy.FAIL);
    }
    
    if (!reportDir.exists()) {
      reportDir.mkdir();
    } else {
      File[] files = reportDir.listFiles();
      if (files.length != 0)
        throw new Exception("dir " + reportDir + " is not empty");
    }
    
  }
  
}
