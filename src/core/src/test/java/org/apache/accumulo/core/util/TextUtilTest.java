package org.apache.accumulo.core.util;

import junit.framework.TestCase;

import org.apache.accumulo.core.util.TextUtil;
import org.apache.hadoop.io.Text;

/**
 * Test the TextUtil class.
 *
 */
public class TextUtilTest extends TestCase {
	/**
	 * co
	 */
	public void testGetBytes(){
		String longMessage = "This is some text";
		Text longMessageText = new Text(longMessage);
		String smallerMessage = "a";
		Text smallerMessageText = new Text(smallerMessage);
		Text someText = new Text(longMessage);
		assertTrue(someText.equals(longMessageText));
		someText.set(smallerMessageText);
		assertTrue(someText.getLength() != someText.getBytes().length);
		assertTrue(TextUtil.getBytes(someText).length == smallerMessage.length());
		assertTrue((new Text(TextUtil.getBytes(someText))).equals(smallerMessageText));
	}

}
