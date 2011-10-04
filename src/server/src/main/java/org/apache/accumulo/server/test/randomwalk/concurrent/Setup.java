package org.apache.accumulo.server.test.randomwalk.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;


public class Setup extends Test {

	@Override
	public void visit(State state, Properties props) throws Exception {
		Random rand = new Random();
        state.set("rand", rand);

		int numTables = Integer.parseInt(props.getProperty("numTables", "5"));
        log.debug("numTables = "+numTables);
        List<String> tables = new ArrayList<String>();
		for(int i = 0; i < numTables; i++)
			tables.add(String.format("ctt_%03d", i));
		state.set("tables", tables);
        
        int numUsers = Integer.parseInt(props.getProperty("numUsers", "5"));
        log.debug("numUsers = "+numUsers);
        List<String> users = new ArrayList<String>();
        for(int i = 0; i < numUsers; i++)
            users.add(String.format("user%03d", i));
		state.set("users", users);
	}

}
