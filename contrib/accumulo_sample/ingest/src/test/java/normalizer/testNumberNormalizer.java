package normalizer;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class testNumberNormalizer {

	
	@Test
	public void test1() throws Exception {
		NumberNormalizer nn = new NumberNormalizer();
		
		String n1 = nn.normalizeFieldValue(null, "1");
		String n2 = nn.normalizeFieldValue(null, "1.00000000");
		
		assertTrue( n1.compareTo(n2) < 0);
		
	}
	
	@Test
	public void test2() {
		NumberNormalizer nn = new NumberNormalizer();
		
		String n1 = nn.normalizeFieldValue(null, "-1.0");
		String n2 = nn.normalizeFieldValue(null, "1.0");

		assertTrue( n1.compareTo(n2) < 0 );
		
	}
	
	
	@Test
	public void test3(){
		NumberNormalizer nn = new NumberNormalizer();
		String n1 = nn.normalizeFieldValue(null, "-0.0001");
		String n2 = nn.normalizeFieldValue(null, "0");
		String n3 = nn.normalizeFieldValue(null, "0.00001");

		assertTrue((n1.compareTo(n2) < 0) && (n2.compareTo(n3) < 0));
	}
	
	
	
	@Test
	public void test4(){
		NumberNormalizer nn = new NumberNormalizer();
		String nn1 = nn.normalizeFieldValue(null, Integer.toString(Integer.MAX_VALUE));
		String nn2 = nn.normalizeFieldValue(null, Integer.toString(Integer.MAX_VALUE-1));
		
		assertTrue( (nn2.compareTo(nn1) < 0));
		
	}
	
	
	@Test
	public void test5(){
		NumberNormalizer nn = new NumberNormalizer();
		String nn1 = nn.normalizeFieldValue(null, "-0.001");
		String nn2 = nn.normalizeFieldValue(null, "-0.0009");
		String nn3 = nn.normalizeFieldValue(null, "-0.00090");
	
		assertTrue((nn3.compareTo(nn2) == 0) && (nn2.compareTo(nn1) > 0));
		
	}
	
	@Test
	public void test6(){
		NumberNormalizer nn = new NumberNormalizer();
		String nn1 = nn.normalizeFieldValue(null, "00.0");		
		String nn2 = nn.normalizeFieldValue(null, "0");
		String nn3 = nn.normalizeFieldValue(null, "0.0");
	
		
		assertTrue((nn3.compareTo(nn2) == 0) && (nn2.compareTo(nn1) == 0));
		
	}
	
}
