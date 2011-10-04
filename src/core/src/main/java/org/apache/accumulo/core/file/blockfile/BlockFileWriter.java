package org.apache.accumulo.core.file.blockfile;

import java.io.IOException;
/**
 * 
 * Provides a generic interface for a Writer for a BlockBaseFile format.
 * Supports the minimal interface required.  
 * 
 * Write a metaBlock and a dataBlock.
 *
 */

public interface BlockFileWriter{
	
	public ABlockWriter prepareMetaBlock(String name, String compressionName) throws IOException;
	
	public ABlockWriter prepareMetaBlock(String name) throws IOException;
	
	public ABlockWriter prepareDataBlock() throws IOException;
	
	public void close() throws IOException;
}