package org.apache.accumulo.core.util;

import java.net.InetSocketAddress;

import junit.framework.TestCase;

import org.apache.accumulo.core.util.AddressUtil;
import org.apache.hadoop.io.Text;

import org.apache.thrift.transport.TSocket;

/**
 * Test the AddressUtil class.
 *
 */
public class AddressUtilTest extends TestCase {
	public void testAddress(){
		InetSocketAddress addr = AddressUtil.parseAddress("127.0.0.1", 12345);
		assertTrue(addr.equals(new InetSocketAddress("127.0.0.1", 12345)));
		InetSocketAddress addr2 = AddressUtil.parseAddress("127.0.0.1:1234", 12345);
		assertTrue(addr2.equals(new InetSocketAddress("127.0.0.1", 1234)));
		InetSocketAddress addr3 = AddressUtil.parseAddress("127.0.0.1:", 12345);
		assertTrue(addr3.equals(new InetSocketAddress("127.0.0.1", 12345)));
		try {
			AddressUtil.parseAddress("127.0.0.1:junk", 12345);
			fail("Number Format Exception Not Thrown");
		} catch (NumberFormatException ex) {
		    assertTrue(true);
		}
		InetSocketAddress addr5 = AddressUtil.parseAddress(new Text("127.0.0.1:543"), 12345);
		assertTrue(addr5.equals(new InetSocketAddress("127.0.0.1", 543)));
		TSocket sock = AddressUtil.createTSocket("127.0.0.11111", 0);
		// lame:
		assertTrue(sock != null);
	}
	
	public void testToString() {
		assertTrue(AddressUtil.toString(new InetSocketAddress("127.0.0.1", 1234)).equals("127.0.0.1:1234"));
	}

}
