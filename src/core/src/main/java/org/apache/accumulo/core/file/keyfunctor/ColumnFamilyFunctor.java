package org.apache.accumulo.core.file.keyfunctor;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.util.bloom.Key;


public class ColumnFamilyFunctor implements KeyFunctor{
	
	public static final PartialKey kDepth = PartialKey.ROW_COLFAM;
	
	@Override
	public org.apache.hadoop.util.bloom.Key transform(org.apache.accumulo.core.data.Key acuKey) {
		
		byte keyData[];
		  
		ByteSequence row = acuKey.getRowData();
		ByteSequence cf = acuKey.getColumnFamilyData();
		keyData = new byte[row.length()+cf.length()];
		System.arraycopy(row.getBackingArray(), row.offset(), keyData, 0, row.length());
		System.arraycopy(cf.getBackingArray(), cf.offset(), keyData, row.length(), cf.length());

		return new org.apache.hadoop.util.bloom.Key(keyData,1.0);
	}

	@Override
	public Key transform(Range range) {
		if(RowFunctor.isRangeInBloomFilter(range, PartialKey.ROW_COLFAM)){
			return transform(range.getStartKey());
		}
		return null;
	}

}
