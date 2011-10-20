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
package org.apache.accumulo.server.test.scalability;

import java.io.FileInputStream;
import java.util.Properties;
import java.net.InetAddress;

import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.server.test.scalability.ScaleTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Run {
  
  public static void main(String[] args) throws Exception {
    
    final String sitePath = "/tmp/scale-site.conf";
    final String testPath = "/tmp/scale-test.conf";
    
    // parse command line
    if (args.length != 3) {
      throw new IllegalArgumentException("usage : Run <testId> <action> <numTabletServers>");
    }
    String testId = args[0];
    String action = args[1];
    int numTabletServers = Integer.parseInt(args[2]);
    
    Configuration conf = CachedConfiguration.getInstance();
    FileSystem fs;
    fs = FileSystem.get(conf);
    
    fs.copyToLocalFile(new Path("/accumulo-scale/conf/site.conf"), new Path(sitePath));
    fs.copyToLocalFile(new Path(String.format("/accumulo-scale/conf/%s.conf", testId)), new Path(testPath));
    
    // load configuration file properties
    Properties scaleProps = new Properties();
    Properties testProps = new Properties();
    try {
      scaleProps.load(new FileInputStream(sitePath));
      testProps.load(new FileInputStream(testPath));
    } catch (Exception e) {
      System.out.println("Problem loading config file");
      e.printStackTrace();
    }
    
    ScaleTest test = (ScaleTest) Class.forName(String.format("accumulo.server.test.scalability.%s", testId)).newInstance();
    
    test.init(scaleProps, testProps, numTabletServers);
    
    if (action.equalsIgnoreCase("setup")) {
      test.setup();
    } else if (action.equalsIgnoreCase("client")) {
      InetAddress addr = InetAddress.getLocalHost();
      String host = addr.getHostName();
      fs.createNewFile(new Path("/accumulo-scale/clients/" + host));
      test.client();
      fs.copyFromLocalFile(new Path("/tmp/scale.out"), new Path("/accumulo-scale/results/" + host));
    } else if (action.equalsIgnoreCase("teardown")) {
      test.teardown();
    }
  }
  
}
