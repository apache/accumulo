package org.apache.accumulo.server.test.randomwalk.sequential;

import java.util.Properties;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;
import org.apache.hadoop.io.Text;


public class Write extends Test {

	@Override
	public void visit(State state, Properties props) throws Exception {
		
		BatchWriter bw = state.getMultiTableBatchWriter().getBatchWriter(state.getString("seqTableName"));
		
		state.set("numWrites", state.getInteger("numWrites")+1);
		
		Integer totalWrites = state.getInteger("totalWrites")+1;
		if ((totalWrites % 10000) == 0) {
			log.debug("Total writes: "+totalWrites);
		}
		state.set("totalWrites", totalWrites);
		
		Mutation m = new Mutation(new Text(String.format("%010d", totalWrites)));
		m.put(new Text("cf"), new Text("cq"), new Value(new String("val").getBytes()));
		bw.addMutation(m);
	}
}
