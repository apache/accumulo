package org.apache.accumulo.core.client;

import java.util.Collection;

import org.apache.accumulo.core.data.Range;


public interface BatchDeleter extends ScannerBase {

	public void delete() throws MutationsRejectedException, TableNotFoundException;
	
	/**
	 * Allows scanning over multiple ranges efficiently.
	 * 
	 * @param ranges specifies the non-overlapping ranges to query
	 */
	void setRanges(Collection<Range> ranges);

}
