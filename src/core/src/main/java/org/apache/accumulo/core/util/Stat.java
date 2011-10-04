package org.apache.accumulo.core.util;

public class Stat {
	
	long max = Long.MIN_VALUE;
	long min = Long.MAX_VALUE;
	long sum = 0;
	int count = 0;
	double partialStdDev = 0;
	
	public void addStat(long stat){
		if(stat > max) max = stat;
		if(stat < min) min = stat;
		
		sum += stat;
		
		partialStdDev += stat * stat;
		
		count++;
	}
	
	public long getMin(){
		return min;
	}
	
	public long getMax(){
		return max;
	}
	
	public double getAverage(){
		return ((double)sum)/count;
	}
	
	public double getStdDev(){
		return Math.sqrt(partialStdDev/count - getAverage() * getAverage());
	}
	
	public String toString(){
		return String.format("%,d %,d %,.2f %,d", getMin(), getMax(), getAverage(), count);
	}

	public void clear() {
		sum = 0;
		count = 0;
		partialStdDev = 0;
	}

	public long getSum() {
		return sum;
	}
}
