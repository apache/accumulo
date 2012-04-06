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

import java.util.Arrays;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class TableDiskUsage {
  
  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    FileSystem fs = FileSystem.get(new Configuration());
    
    Instance instance = HdfsZooInstance.getInstance();
    ServerConfiguration conf = new ServerConfiguration(instance);
    Connector conn = instance.getConnector("root", "secret");
    
    org.apache.accumulo.core.util.TableDiskUsage.printDiskUsage(conf.getConfiguration(), Arrays.asList(args), fs, conn);
  }
  
}
