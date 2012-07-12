package org.apache.accumulo.server.test.randomwalk.concurrent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooReader;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;
import org.apache.accumulo.server.util.AddressUtil;
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
          if (!"master".equals(new String(data))) {
            result.add(new TServerInstance(AddressUtil.parseAddress(child, Property.TSERV_CLIENTPORT), stat.getEphemeralOwner()));
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
