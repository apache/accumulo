package org.apache.accumulo.examples.filedata;

import junit.framework.TestCase;

import org.apache.accumulo.examples.filedata.KeyUtil;
import org.apache.hadoop.io.Text;

public class KeyUtilTest extends TestCase {
	public static void checkSeps(String... s) {
		Text t = KeyUtil.buildNullSepText(s);
		String[] rets = KeyUtil.splitNullSepText(t);

		int length = 0;
		for (String str : s) length += str.length();
		assertEquals(t.getLength(),length+s.length-1);
		assertEquals(rets.length,s.length);
		for (int i = 0; i < s.length; i++)
			assertEquals(s[i],rets[i]);		
	}

	public void testNullSep() {
		checkSeps("abc","d","","efgh");
		checkSeps("ab","");
		checkSeps("abcde");
		checkSeps("");
		checkSeps("","");
	}
}
