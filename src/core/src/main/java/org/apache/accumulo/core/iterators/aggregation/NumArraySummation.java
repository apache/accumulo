package org.apache.accumulo.core.iterators.aggregation;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.WritableUtils;


public class NumArraySummation implements Aggregator {
	long[] sum = new long[0];
	
	public Value aggregate() {
		try {
			return new Value(NumArraySummation.longArrayToBytes(sum));
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void collect(Value value) {
		long[] la;
		try {
			la = NumArraySummation.bytesToLongArray(value.get());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		if (la.length > sum.length) {
			for (int i=0; i < sum.length; i++) {
				la[i] = NumSummation.safeAdd(la[i],sum[i]);
			}
			sum = la;
		}
		else {
			for (int i=0; i < la.length; i++) {
				sum[i] = NumSummation.safeAdd(sum[i],la[i]);
			}
		}
	}
	
	public static byte[] longArrayToBytes(long[] la) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(baos);

		WritableUtils.writeVInt(dos, la.length);
		for (int i = 0; i < la.length; i++) {
			WritableUtils.writeVLong(dos, la[i]);
		}
		
		return baos.toByteArray();
	}
	
	public static long[] bytesToLongArray(byte[] b) throws IOException {
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(b));
		int len = WritableUtils.readVInt(dis);
		
		long[] la = new long[len];
		
		for (int i = 0; i < len; i++) {
			la[i] = WritableUtils.readVLong(dis);
		}
		
		return la;
	}

	public void reset() {
		sum = new long[0];
	}

}
