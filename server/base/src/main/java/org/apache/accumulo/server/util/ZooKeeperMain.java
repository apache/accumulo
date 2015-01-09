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

import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.beust.jcommander.Parameter;

public class ZooKeeperMain {

  static class Opts extends Help {

    @Parameter(names = {"-z", "--keepers"}, description = "Comma separated list of zookeeper hosts (host:port,host:port)")
    String servers = null;

    @Parameter(names = {"-t", "--timeout"}, description = "timeout, in seconds to timeout the zookeeper connection")
    long timeout = 30;
  }

  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(ZooKeeperMain.class.getName(), args);
    FileSystem fs = VolumeManagerImpl.get().getDefaultVolume().getFileSystem();
    String baseDir = ServerConstants.getBaseUris()[0];
    System.out.println("Using " + fs.makeQualified(new Path(baseDir + "/instance_id")) + " to lookup accumulo instance");
    Instance instance = HdfsZooInstance.getInstance();
    if (opts.servers == null) {
      opts.servers = instance.getZooKeepers();
    }
    System.out.println("The accumulo instance id is " + instance.getInstanceID());
    if (!opts.servers.contains("/"))
      opts.servers += "/accumulo/" + instance.getInstanceID();
    org.apache.zookeeper.ZooKeeperMain.main(new String[] {"-server", opts.servers, "-timeout", "" + (opts.timeout * 1000)});
  }
}
