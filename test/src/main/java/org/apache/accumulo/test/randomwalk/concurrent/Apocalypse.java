package org.apache.accumulo.test.randomwalk.concurrent;

import java.util.Properties;

import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;

public class Apocalypse extends Test {
  
  @Override
  public void visit(State state, Properties props) throws Exception {
    Process exec = Runtime.getRuntime().exec(new String[] {System.getenv("ACCUMULO_HOME") + "/test/system/randomwalk/bin/apocalypse.sh"});
    if (exec.waitFor() != 0)
      throw new RuntimeException("apocalypse.sh returned a non-zero response: " + exec.exitValue());
  }
  
}
