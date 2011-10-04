package org.apache.accumulo.core.file.blockfile;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;


/*
 * Minimal interface to read a block from a 
 * block based file
 * 
 */


public interface ABlockReader extends DataInput{
	
	public long getRawSize();
	
	public DataInputStream getStream() throws IOException;
	
	public void close() throws IOException;
	
}
