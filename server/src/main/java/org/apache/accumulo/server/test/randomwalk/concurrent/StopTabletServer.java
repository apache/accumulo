package org.apache.accumulo.server.test.randomwalk.concurrent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.server.master.LiveTServerSet;
import org.apache.accumulo.server.master.LiveTServerSet.Listener;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;

public class StopTabletServer extends Test {
  
  @Override
  public void visit(State state, Properties props) throws Exception {
    
    LiveTServerSet set = new LiveTServerSet(state.getInstance(), DefaultConfiguration.getDefaultConfiguration(), new Listener() {
      @Override
      public void update(LiveTServerSet current, Set<TServerInstance> deleted, Set<TServerInstance> added) {
        log.info("Tablet server set changed: " + deleted + " deleted and " + added + " added");
      }
    });
    List<TServerInstance> currentServers = new ArrayList<TServerInstance>(set.getCurrentServers());
    Collections.shuffle(currentServers);
    Runtime runtime = Runtime.getRuntime();
    if (currentServers.size() > 1) {
      TServerInstance victim = currentServers.get(0);
      log.info("Stopping " + victim.hostPort());
      runtime.exec(new String[] {System.getenv("ACCUMULO_HOME") + "/bin/accumulo", "admin", "stop", victim.hostPort()});
      if (set.getCurrentServers().contains(victim))
        throw new RuntimeException("Failed to stop " + victim);
    }
  }
  
}
