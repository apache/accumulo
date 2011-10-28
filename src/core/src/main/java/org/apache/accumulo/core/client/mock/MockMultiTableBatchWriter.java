package org.apache.accumulo.core.client.mock;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;

public class MockMultiTableBatchWriter implements MultiTableBatchWriter {
  
  final MockAccumulo ma;
  
  MockMultiTableBatchWriter(MockAccumulo ma) {
    this.ma = ma;
  }

  @Override
  public BatchWriter getBatchWriter(String table) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    return new MockBatchWriter(ma, table);
  }
  
  @Override
  public void flush() throws MutationsRejectedException {
  }
  
  @Override
  public void close() throws MutationsRejectedException {
  }
  
  @Override
  public boolean isClosed() {
    return false;
  }
  
}
