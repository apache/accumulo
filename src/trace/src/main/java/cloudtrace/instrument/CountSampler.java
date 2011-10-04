package cloudtrace.instrument;

import java.util.Random;

/**
 * Sampler that returns true every N calls. 
 *
 */
public class CountSampler implements Sampler {
	
	final static Random random = new Random();

	final long frequency;
	long count = random.nextLong(); 
	
	public CountSampler(long frequency) {
		this.frequency = frequency;
	}
	
	@Override
	public boolean next() {
		return (count++ % frequency) == 0;
	}

}
