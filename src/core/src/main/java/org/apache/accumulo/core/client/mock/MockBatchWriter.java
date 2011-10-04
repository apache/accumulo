package org.apache.accumulo.core.client.mock;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;

public class MockBatchWriter implements BatchWriter {
    
    final String tablename;
    final MockAccumulo acu;
    
    MockBatchWriter(MockAccumulo acu, String tablename) {
        this.acu = acu;
        this.tablename = tablename;
    }

    @Override
    public void addMutation(Mutation m) throws MutationsRejectedException {
        acu.addMutation(tablename, m);
    }

    @Override
    public void addMutations(Iterable<Mutation> iterable)
            throws MutationsRejectedException {
        for (Mutation m : iterable) {
            acu.addMutation(tablename, m);
        }
    }

    @Override
    public void flush() throws MutationsRejectedException {
    }

    @Override
    public void close() throws MutationsRejectedException {
    }

}
