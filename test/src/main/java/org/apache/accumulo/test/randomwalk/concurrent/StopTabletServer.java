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
package org.apache.accumulo.test.randomwalk.concurrent;

import static com.google.common.base.Charsets.UTF_8;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooReader;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

public class StopTabletServer extends Test {

  Set<TServerInstance> getTServers(Instance instance) throws KeeperException, InterruptedException {
    Set<TServerInstance> result = new HashSet<TServerInstance>();
    ZooReader rdr = new ZooReader(instance.getZooKeepers(), instance.getZooKeepersSessionTimeOut());
    String base = ZooUtil.getRoot(instance) + Constants.ZTSERVERS;
    for (String child : rdr.getChildren(base)) {
      try {
        List<String> children = rdr.getChildren(base + "/" + child);
        if (children.size() > 0) {
          Collections.sort(children);
          Stat stat = new Stat();
          byte[] data = rdr.getData(base + "/" + child + "/" + children.get(0), stat);
          if (!"master".equals(new String(data, UTF_8))) {
            result.add(new TServerInstance(AddressUtil.parseAddress(child, false), stat.getEphemeralOwner()));
          }
        }
      } catch (KeeperException.NoNodeException ex) {
        // someone beat us too it
      }
    }
    return result;
  }

  @Override
  public void visit(State state, Properties props) throws Exception {

    Instance instance = state.getInstance();

    List<TServerInstance> currentServers = new ArrayList<TServerInstance>(getTServers(instance));
    Collections.shuffle(currentServers);
    Runtime runtime = Runtime.getRuntime();
    if (currentServers.size() > 1) {
      TServerInstance victim = currentServers.get(0);
      log.info("Stopping " + victim.hostPort());
      Process exec = runtime.exec(new String[] {System.getenv("ACCUMULO_HOME") + "/bin/accumulo", "admin", "stop", victim.hostPort()});
      if (exec.waitFor() != 0)
        throw new RuntimeException("admin stop returned a non-zero response: " + exec.exitValue());
      Set<TServerInstance> set = getTServers(instance);
      if (set.contains(victim))
        throw new RuntimeException("Failed to stop " + victim);
    }
  }

}
