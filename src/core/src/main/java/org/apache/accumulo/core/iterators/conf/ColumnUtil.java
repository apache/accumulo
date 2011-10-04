package org.apache.accumulo.core.iterators.conf;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;


public class ColumnUtil {
	private static int hash(byte[] bytes, int offset, int len){
		 int hash = 1;
		 int end = offset + len;
		 
		 for (int i = offset; i < end; i++)
			 hash = (31 * hash) + bytes[i];
		 
		 return hash;
	}
	
	private static int hash(ByteSequence bs){
		 return hash(bs.getBackingArray(), bs.offset(), bs.length());
	}
	
	public static class ColFamHashKey {
		Text columnFamily;
		
		Key key;
		
		private int hashCode;
		
		ColFamHashKey(){
			columnFamily = null;
		}
		
		ColFamHashKey(Text cf){
			columnFamily = cf;
			hashCode = hash(columnFamily.getBytes(), 0, columnFamily.getLength()) ;
		}
		
		void set(Key key){
			this.key = key;
			hashCode = hash(key.getColumnFamilyData());
		}
		
		public int hashCode(){
			return hashCode;
		}
		
		public boolean equals(Object o){
			if (o instanceof ColFamHashKey)
				return equals((ColFamHashKey) o);
			return false;
		}
		
		public boolean equals(ColFamHashKey ohk) {
			if(columnFamily == null)
				return key.compareColumnFamily(ohk.columnFamily) == 0;
			return ohk.key.compareColumnFamily(columnFamily) == 0;
		}
	}
	
	public static class ColHashKey {
		Text columnFamily;
		Text columnQualifier;
		
		Key key;
		
		private int hashValue;
		
		ColHashKey(){
			columnFamily = null;
			columnQualifier = null;
		}
		
		ColHashKey(Text cf, Text cq){
			columnFamily = cf;
			columnQualifier = cq;
			hashValue = hash(columnFamily.getBytes(), 0, columnFamily.getLength()) + 
				hash(columnQualifier.getBytes(), 0, columnQualifier.getLength());
		}
		
		void set(Key key){
			this.key = key;
			hashValue = hash(key.getColumnFamilyData()) + hash(key.getColumnQualifierData());
		}

		public int hashCode(){
			return hashValue;
		}
		
		public boolean equals(Object o){
			if (o instanceof ColHashKey)
				return equals((ColHashKey) o);
			return false;
		}
		
		public boolean equals(ColHashKey ohk) {
			if(columnFamily == null)
				return key.compareColumnFamily(ohk.columnFamily) == 0 &&
					key.compareColumnQualifier(ohk.columnQualifier) == 0;
			return ohk.key.compareColumnFamily(columnFamily) == 0 &&
				ohk.key.compareColumnQualifier(columnQualifier) == 0;
		}
	}
}
