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
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.cli.ClientOpts;
import org.apache.accumulo.server.zookeeper.ZooLock;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class ZooZap {

  private static void message(String msg, Opts opts) {
    if (opts.verbose)
      System.out.println(msg);
  }

  static class Opts extends ClientOpts {
    @Parameter(names = "-master", description = "remove master locks")
    boolean zapMaster = false;
    @Parameter(names = "-tservers", description = "remove tablet server locks")
    boolean zapTservers = false;
    @Parameter(names = "-tracers", description = "remove tracer locks")
    boolean zapTracers = false;
    @Parameter(names = "-verbose", description = "print out messages about progress")
    boolean verbose = false;
  }

  public static void main(String[] args) {
    Opts opts = new Opts();
    opts.parseArgs(ZooZap.class.getName(), args);

    if (!opts.zapMaster && !opts.zapTservers && !opts.zapTracers) {
      new JCommander(opts).usage();
      return;
    }

    String iid = opts.getInstance().getInstanceID();
    IZooReaderWriter zoo = ZooReaderWriter.getInstance();

    if (opts.zapMaster) {
      String masterLockPath = Constants.ZROOT + "/" + iid + Constants.ZMASTER_LOCK;

      zapDirectory(zoo, masterLockPath, opts);
    }

    if (opts.zapTservers) {
      String tserversPath = Constants.ZROOT + "/" + iid + Constants.ZTSERVERS;
      try {
        List<String> children = zoo.getChildren(tserversPath);
        for (String child : children) {
          message("Deleting " + tserversPath + "/" + child + " from zookeeper", opts);

          if (opts.zapMaster)
            ZooReaderWriter.getInstance().recursiveDelete(tserversPath + "/" + child, NodeMissingPolicy.SKIP);
          else {
            String path = tserversPath + "/" + child;
            if (zoo.getChildren(path).size() > 0) {
              if (!ZooLock.deleteLock(path, "tserver")) {
                message("Did not delete " + tserversPath + "/" + child, opts);
              }
            }
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    if (opts.zapTracers) {
      String path = Constants.ZROOT + "/" + iid + Constants.ZTRACERS;
      zapDirectory(zoo, path, opts);
    }

  }

  private static void zapDirectory(IZooReaderWriter zoo, String path, Opts opts) {
    try {
      List<String> children = zoo.getChildren(path);
      for (String child : children) {
        message("Deleting " + path + "/" + child + " from zookeeper", opts);
        zoo.recursiveDelete(path + "/" + child, NodeMissingPolicy.SKIP);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
