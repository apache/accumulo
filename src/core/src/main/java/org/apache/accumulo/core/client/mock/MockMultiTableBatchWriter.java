package org.apache.accumulo.core.client.mock;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;


public class MockMultiTableBatchWriter implements MultiTableBatchWriter {
	MockAccumulo acu = null;
	Map<String, MockBatchWriter> bws = null;

	public MockMultiTableBatchWriter(MockAccumulo acu) {
		this.acu = acu;
		bws = new HashMap<String, MockBatchWriter>();
	}

	@Override
	public BatchWriter getBatchWriter(String table) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		if (!bws.containsKey(table)) {
			bws.put(table, new MockBatchWriter(acu, table));
		}
		return bws.get(table);
	}

	@Override
	public void flush() throws MutationsRejectedException {
	}

	@Override
	public void close() throws MutationsRejectedException {
	}

	@Override
	public boolean isClosed() {
		throw new UnsupportedOperationException();
	}
}