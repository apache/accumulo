package org.apache.accumulo.core.file.blockfile;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;


/*
 * Minimal interface to write a block to a 
 * block based file
 * 
 */

public interface ABlockWriter extends DataOutput {
	
	public long getCompressedSize() throws IOException;
	
	public void close() throws IOException;
	
	public long getRawSize() throws IOException;
	
	public DataOutputStream getStream() throws IOException;
	
}