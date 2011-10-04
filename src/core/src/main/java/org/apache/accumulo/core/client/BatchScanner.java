package org.apache.accumulo.core.client;

import java.util.Collection;

import org.apache.accumulo.core.data.Range;


/**
 * Implementations of BatchScanner support efficient lookups 
 * of many ranges in accumulo.
 * 
 * Use this when looking up lots of ranges and you expect
 * each range to contain a small amount of data.  Also only use
 * this when you do not care about the returned data being
 * in sorted order.
 * 
 * If you want to lookup a few ranges and expect those
 * ranges to contain a lot of data, then use the Scanner
 * instead. Also, the Scanner will return data in sorted
 * order, this will not.
 */

public interface BatchScanner extends ScannerBase {
	
	/**
	 * Allows scanning over multiple ranges efficiently.
	 * 
	 * @param ranges specifies the non-overlapping ranges to query
	 */
	void setRanges(Collection<Range> ranges);
	
	/**
	 * Cleans up and finalizes the scanner
	 */
	void close();
}
