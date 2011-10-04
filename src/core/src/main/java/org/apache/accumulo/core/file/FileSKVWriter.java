package org.apache.accumulo.core.file;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Set;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;


public interface FileSKVWriter {
	boolean supportsLocalityGroups();
	void startNewLocalityGroup(String name, Set<ByteSequence> columnFamilies) throws IOException;
	void startDefaultLocalityGroup() throws IOException;
	void append(Key key, Value value) throws IOException;
	DataOutputStream createMetaStore(String name) throws IOException;
	void close() throws IOException;
}
