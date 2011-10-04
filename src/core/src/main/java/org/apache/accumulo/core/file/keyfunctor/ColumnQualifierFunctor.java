package org.apache.accumulo.core.file.keyfunctor;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.util.bloom.Key;


public class ColumnQualifierFunctor implements KeyFunctor{

	@Override
	public org.apache.hadoop.util.bloom.Key transform(org.apache.accumulo.core.data.Key acuKey) {
		byte keyData[];
		  
		  ByteSequence row = acuKey.getRowData();
		  ByteSequence cf = acuKey.getColumnFamilyData();
		  ByteSequence cq = acuKey.getColumnQualifierData();
		  keyData = new byte[row.length()+cf.length()+cq.length()];
		  System.arraycopy(row.getBackingArray(), row.offset(), keyData, 0, row.length());
		  System.arraycopy(cf.getBackingArray(), cf.offset(), keyData, row.length(), cf.length());
		  System.arraycopy(cq.getBackingArray(), cq.offset(), keyData, row.length()+cf.length(), cq.length());
		  
		return new org.apache.hadoop.util.bloom.Key(keyData,1.0);
	}

	@Override
	public Key transform(Range range) {
		if(RowFunctor.isRangeInBloomFilter(range, PartialKey.ROW_COLFAM_COLQUAL)){
			return transform(range.getStartKey());
		}
		return null;
	}


}
