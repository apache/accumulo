package org.apache.accumulo.core.file.keyfunctor;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.util.bloom.Key;


public class RowFunctor implements KeyFunctor{
	
	@Override
	public org.apache.hadoop.util.bloom.Key transform(org.apache.accumulo.core.data.Key acuKey) {
		byte keyData[];
		
		  ByteSequence row = acuKey.getRowData();
		  keyData = new byte[row.length()];
		  System.arraycopy(row.getBackingArray(), 0, keyData, 0, row.length());

		return new org.apache.hadoop.util.bloom.Key(keyData,1.0);
	}

	@Override
	public Key transform(Range range) {
		if(isRangeInBloomFilter(range, PartialKey.ROW)){
			return transform(range.getStartKey());
		}
		return null;
	}

	static boolean isRangeInBloomFilter(Range range, PartialKey keyDepth) {

		if(range.getStartKey() == null || range.getEndKey() == null){
			return false;
		}

		if(range.getStartKey().equals(range.getEndKey(), keyDepth))
			return true;

		//include everything but the deleted flag in the comparison... 
		return range.getStartKey().followingKey(keyDepth).equals(range.getEndKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME) && !range.isEndKeyInclusive();
	}
}
