package org.apache.accumulo.core.file.keyfunctor;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;


public interface KeyFunctor {
	/**
	 * Implementations should return null if a range can not be converted to a bloom key.
	 *  
	 */
	public org.apache.hadoop.util.bloom.Key transform(Range range);
	public org.apache.hadoop.util.bloom.Key transform(Key key);
}
