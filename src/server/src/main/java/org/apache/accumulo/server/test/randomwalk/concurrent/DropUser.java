package org.apache.accumulo.server.test.randomwalk.concurrent;

import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;


public class DropUser extends Test {
    @Override
    public void visit(State state, Properties props) throws Exception {
        Connector conn = state.getConnector();
        
        Random rand = (Random) state.get("rand");
        
        @SuppressWarnings("unchecked")
        List<String> userNames = (List<String>) state.get("users");
        
        String userName = userNames.get(rand.nextInt(userNames.size()));
        
        try {
        	log.debug("Dropping user "+userName);
            conn.securityOperations().dropUser(userName);
        } catch (AccumuloSecurityException ex) {
            log.debug("Unable to drop " + ex.getCause());
        } 
    }
}
