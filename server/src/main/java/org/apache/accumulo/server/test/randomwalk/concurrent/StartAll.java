package org.apache.accumulo.server.test.randomwalk.concurrent;

import java.util.Properties;

import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;

public class StartAll extends Test {
  
  @Override
  public void visit(State state, Properties props) throws Exception {
    log.info("Starting all servers");
    Process exec = Runtime.getRuntime().exec(new String[]{System.getenv().get("ACCUMULO_HOME") + "/bin/start-all.sh"});
    exec.waitFor();
  }
  
}
