package org.apache.accumulo.core.file;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.iterators.InterruptibleIterator;


public interface FileSKVIterator extends InterruptibleIterator {
	public Key getFirstKey() throws IOException;
	public Key getLastKey() throws IOException;
	public DataInputStream getMetaStore(String name) throws IOException, NoSuchMetaStoreException;
	public void closeDeepCopies() throws IOException;
	public void close() throws IOException;
}
