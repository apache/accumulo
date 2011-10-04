package org.apache.accumulo.core.file.blockfile;

import java.io.IOException;


/**
 * 
 * Provides a generic interface for a Reader for a BlockBaseFile format.
 * Supports the minimal interface required.  
 * 
 * Read a metaBlock and a dataBlock
 *
 */


public interface BlockFileReader{
	
	public ABlockReader getMetaBlock(String name) throws IOException;
	
	public ABlockReader getDataBlock(int blockIndex) throws IOException;
	
	public void close() throws IOException;
	
}