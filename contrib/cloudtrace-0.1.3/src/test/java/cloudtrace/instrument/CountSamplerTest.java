package cloudtrace.instrument;

import junit.framework.Assert;

import org.junit.Test;


public class CountSamplerTest {
	
	@Test
	public void testNext() {
		CountSampler half = new CountSampler(2);
		CountSampler hundred = new CountSampler(100);
		int halfCount = 0;
		int hundredCount = 0;
		for (int i = 0; i < 200; i++) {
			if (half.next())
				halfCount++;
			if (hundred.next()) 
				hundredCount++;
		}
		Assert.assertEquals(2, hundredCount);
		Assert.assertEquals(100, halfCount);
	}
}
