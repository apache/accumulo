package org.apache.accumulo.core.client;

import org.apache.accumulo.core.data.Range;

/**
 * Walk a table over a given range.
 * 
 * provides scanner functionality
 * 
 * "Clients can iterate over multiple column families, and there are several 
 * mechanisms for limiting the rows, columns, and timestamps traversed by a 
 * scan. For example, we could restrict [a] scan ... to only produce anchors 
 * whose columns match [a] regular expression ..., or to only produce 
 * anchors whose timestamps fall within ten days of the current time."
 */
public interface Scanner extends ScannerBase {
		
	/**
	 * When failure occurs, the scanner automatically retries.  This
	 * setting determines how long a scanner will retry.  By default
	 * a scanner will retry forever. 
	 * 
	 * @param timeOut in seconds
	 */
	public void setTimeOut(int timeOut);
	/**
	 * @return the timeout configured for this scanner
	 */
	public int getTimeOut();
	
	/**
	 * @param range key range to begin and end scan
	 */
	public void setRange(Range range);
	/**
	 * @return the range configured for this scanner
	 */
	public Range getRange();
   	
	/**
	 * @param size the number of Keys/Value pairs to fetch per call to Accumulo
	 */
   	public void setBatchSize(int size);
	/**
	 * @return the batch size configured for this scanner
	 */
	public int getBatchSize();
	
	public void enableIsolation();
	void disableIsolation();
}
