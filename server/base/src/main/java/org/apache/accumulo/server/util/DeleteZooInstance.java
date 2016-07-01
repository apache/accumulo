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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.zookeeper.KeeperException;

import com.beust.jcommander.Parameter;

public class DeleteZooInstance {

  static class Opts extends Help {
    @Parameter(names = {"-i", "--instance"}, description = "the instance name or id to delete")
    String instance;
  }

  static void deleteRetry(IZooReaderWriter zk, String path) throws Exception {
    for (int i = 0; i < 10; i++) {
      try {
        zk.recursiveDelete(path, NodeMissingPolicy.SKIP);
        return;
      } catch (KeeperException.NotEmptyException ex) {
        // ignored
      } catch (Exception ex) {
        throw ex;
      }
    }
  }

  /**
   * @param args
   *          : the name or UUID of the instance to be deleted
   */
  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(DeleteZooInstance.class.getName(), args);

    IZooReaderWriter zk = ZooReaderWriter.getInstance();
    // try instance name:
    Set<String> instances = new HashSet<>(zk.getChildren(Constants.ZROOT + Constants.ZINSTANCES));
    Set<String> uuids = new HashSet<>(zk.getChildren(Constants.ZROOT));
    uuids.remove("instances");
    if (instances.contains(opts.instance)) {
      String path = Constants.ZROOT + Constants.ZINSTANCES + "/" + opts.instance;
      byte[] data = zk.getData(path, null);
      deleteRetry(zk, path);
      deleteRetry(zk, Constants.ZROOT + "/" + new String(data, UTF_8));
    } else if (uuids.contains(opts.instance)) {
      // look for the real instance name
      for (String instance : instances) {
        String path = Constants.ZROOT + Constants.ZINSTANCES + "/" + instance;
        byte[] data = zk.getData(path, null);
        if (opts.instance.equals(new String(data, UTF_8)))
          deleteRetry(zk, path);
      }
      deleteRetry(zk, Constants.ZROOT + "/" + opts.instance);
    }
  }

}
