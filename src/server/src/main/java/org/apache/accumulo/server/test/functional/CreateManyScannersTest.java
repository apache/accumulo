package org.apache.accumulo.server.test.functional;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;



public class CreateManyScannersTest extends FunctionalTest {

	@Override
	public void cleanup() throws Exception {
		
	}

	@Override
	public Map<String, String> getInitialConfig() {
		return Collections.emptyMap();
	}

	@Override
	public List<TableSetup> getTablesToCreate() {
		return Collections.singletonList(new TableSetup("mscant"));
	}

	@Override
	public void run() throws Exception {
	    Connector connector = getConnector();
		for(int i = 0; i < 100000; i++){
			connector.createScanner("mscant", Constants.NO_AUTHS);
		}
	}

}
