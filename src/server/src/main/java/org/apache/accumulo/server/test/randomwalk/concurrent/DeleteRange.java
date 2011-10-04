package org.apache.accumulo.server.test.randomwalk.concurrent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;
import org.apache.hadoop.io.Text;


public class DeleteRange extends Test {

    @Override
    public void visit(State state, Properties props) throws Exception {
        Connector conn = state.getConnector();
        
        Random rand = (Random) state.get("rand");
        
        @SuppressWarnings("unchecked")
        List<String> tableNames = (List<String>) state.get("tables");
        
        String tableName = tableNames.get(rand.nextInt(tableNames.size()));

        List<Text> range = new ArrayList<Text>();
        do {
            range.add(new Text(String.format("%016x", Math.abs(rand.nextLong()))));
            range.add(new Text(String.format("%016x", Math.abs(rand.nextLong()))));
        } while (range.get(0).equals(range.get(1)));
        Collections.sort(range);
        if (rand.nextInt(20) == 0)
            range.set(0, null);
        if (rand.nextInt(20) == 0)
            range.set(1, null);
        
        try{
            conn.tableOperations().deleteRows(tableName, range.get(0), range.get(1));
            log.debug("deleted rows (" + range.get(0) + " -> " + range.get(1) + "] in "+tableName);
        }catch(TableNotFoundException tne){
            log.debug("deleted rows "+tableName+" failed, doesnt exist");
        }catch(TableOfflineException toe){
            log.debug("deleted rows "+tableName+" failed, offline");
        }
    }
}
