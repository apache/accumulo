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
package org.apache.accumulo.test.scalability;

import java.io.FileInputStream;
import java.net.InetAddress;
import java.util.Properties;

import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.beust.jcommander.Parameter;

public class Run {

  static class Opts extends Help {
    @Parameter(names = "--testId", required = true)
    String testId;
    @Parameter(names = "--action", required = true, description = "one of 'setup', 'teardown' or 'client'")
    String action;
    @Parameter(names = "--count", description = "number of tablet servers", required = true)
    int numTabletServers;
  }

  public static void main(String[] args) throws Exception {

    final String sitePath = "/tmp/scale-site.conf";
    final String testPath = "/tmp/scale-test.conf";
    Opts opts = new Opts();
    opts.parseArgs(Run.class.getName(), args);

    Configuration conf = CachedConfiguration.getInstance();
    FileSystem fs;
    fs = FileSystem.get(conf);

    fs.copyToLocalFile(new Path("/accumulo-scale/conf/site.conf"), new Path(sitePath));
    fs.copyToLocalFile(new Path(String.format("/accumulo-scale/conf/%s.conf", opts.testId)), new Path(testPath));

    // load configuration file properties
    Properties scaleProps = new Properties();
    Properties testProps = new Properties();
    try {
      FileInputStream fis = new FileInputStream(sitePath);
      try {
        scaleProps.load(fis);
      } finally {
        fis.close();
      }
      fis = new FileInputStream(testPath);
      testProps.load(fis);
    } catch (Exception e) {
      System.out.println("Problem loading config file");
      e.printStackTrace();
    }

    ScaleTest test = (ScaleTest) Class.forName(String.format("org.apache.accumulo.test.scalability.%s", opts.testId)).newInstance();

    test.init(scaleProps, testProps, opts.numTabletServers);

    if (opts.action.equalsIgnoreCase("setup")) {
      test.setup();
    } else if (opts.action.equalsIgnoreCase("client")) {
      InetAddress addr = InetAddress.getLocalHost();
      String host = addr.getHostName();
      fs.createNewFile(new Path("/accumulo-scale/clients/" + host));
      test.client();
      fs.copyFromLocalFile(new Path("/tmp/scale.out"), new Path("/accumulo-scale/results/" + host));
    } else if (opts.action.equalsIgnoreCase("teardown")) {
      test.teardown();
    }
  }

}
