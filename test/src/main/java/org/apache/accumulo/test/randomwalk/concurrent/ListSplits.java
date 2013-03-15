package org.apache.accumulo.test.randomwalk.concurrent;

import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;
import org.apache.hadoop.io.Text;

public class ListSplits extends Test {
  
  @Override
  public void visit(State state, Properties props) throws Exception {
    Connector conn = state.getConnector();
    
    Random rand = (Random) state.get("rand");
    
    @SuppressWarnings("unchecked")
    List<String> tableNames = (List<String>) state.get("tables");
    
    String tableName = tableNames.get(rand.nextInt(tableNames.size()));
    
    try {
      Collection<Text> splits = conn.tableOperations().listSplits(tableName);
      log.debug("Table " + tableName + " had " + splits.size() + " splits");
    } catch (TableNotFoundException e) {
      log.debug("listSplits " + tableName + " failed, doesnt exist");
    } catch (AccumuloSecurityException ase) {
      log.debug("listSplits " + tableName + " failed, " + ase.getMessage());
    }
  }
}
