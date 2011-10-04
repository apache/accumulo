package org.apache.accumulo.core.util;

import java.util.Iterator;

import org.apache.accumulo.core.util.shell.commands.EscapeTokenizer;

import junit.framework.TestCase;


public class EscapeTokenizerTest extends TestCase {

	public void test1() {
		EscapeTokenizer et = new EscapeTokenizer("wat,2", ",");
		Iterator<String> iter = et.iterator();
		assertTrue(iter.next().equals("wat"));
		assertTrue(iter.next().equals("2"));
		assertTrue(! iter.hasNext());
	}
	
	public void test2() {
		EscapeTokenizer et = new EscapeTokenizer("option1=2,option2=3,", ",=");
		Iterator<String> iter = et.iterator();
		assertTrue(iter.next().equals("option1"));
		assertTrue(iter.next().equals("2"));
		assertTrue(iter.next().equals("option2"));
		assertTrue(iter.next().equals("3"));
		assertTrue(! iter.hasNext());
	}
	
	public void test3() {
		EscapeTokenizer et = new EscapeTokenizer("option\\3=2,option4=3,", ",=");
		Iterator<String> iter = et.iterator();
		assertTrue(iter.next().equals("option\\3"));
		assertTrue(iter.next().equals("2"));
		assertTrue(iter.next().equals("option4"));
		assertTrue(iter.next().equals("3"));
		assertTrue(! iter.hasNext());
	}
	
	public void test4() {
		EscapeTokenizer et = new EscapeTokenizer("option\\=2=2,option4=3,", ",=");
		Iterator<String> iter = et.iterator();
		assertTrue(iter.next().equals("option=2"));
		assertTrue(iter.next().equals("2"));
		assertTrue(iter.next().equals("option4"));
		assertTrue(iter.next().equals("3"));
		assertTrue(! iter.hasNext());
	}
	
	public void test5() {
		EscapeTokenizer et = new EscapeTokenizer("test\\=,\\=2=4", ",=");
		Iterator<String> iter = et.iterator();
		assertTrue(iter.next().equals("test="));
		assertTrue(iter.next().equals("=2"));
		assertTrue(iter.next().equals("4"));
		assertTrue(! iter.hasNext());
	}
	
	public void test6() {
		EscapeTokenizer et = new EscapeTokenizer("wat\\,2", ",");
		Iterator<String> iter = et.iterator();
		assertTrue(iter.next().equals("wat,2"));
		assertTrue(! iter.hasNext());
	}
	
	public void test7() {
		EscapeTokenizer et = new EscapeTokenizer("wat\\=2=4", ",=");
		Iterator<String> iter = et.iterator();
		assertTrue(iter.next().equals("wat=2"));
		assertTrue(iter.next().equals("4"));
		assertTrue(! iter.hasNext());
	}
	public void test8() {
		EscapeTokenizer et = new EscapeTokenizer("1,2,3,4",",");
		Iterator<String> iter = et.iterator();
		assertTrue(iter.next().equals("1"));
		assertTrue(iter.next().equals("2"));
		assertTrue(iter.next().equals("3"));
		assertTrue(iter.next().equals("4"));
		assertTrue(! iter.hasNext());
	}
	public void test9() {
		EscapeTokenizer et = new EscapeTokenizer("1\\,2,3,4",",");
		Iterator<String> iter = et.iterator();
		assertTrue(iter.next().equals("1,2"));
		assertTrue(iter.next().equals("3"));
		assertTrue(iter.next().equals("4"));
		assertTrue(! iter.hasNext());
	}
	public void test10() {
		EscapeTokenizer et = new EscapeTokenizer("1,\2,3,4",",");
		Iterator<String> iter = et.iterator();
		assertTrue(iter.next().equals("1"));
		assertTrue(iter.next().equals("\2"));
		assertTrue(iter.next().equals("3"));
		assertTrue(iter.next().equals("4"));
		assertTrue(! iter.hasNext());
	}
	public void test11() {
		EscapeTokenizer et = new EscapeTokenizer("1,,,,,2,3,4",",");
		Iterator<String> iter = et.iterator();
		assertTrue(iter.next().equals("1"));
		assertTrue(iter.next().equals("2"));
		assertTrue(iter.next().equals("3"));
		assertTrue(iter.next().equals("4"));
		assertTrue(! iter.hasNext());
	}
}
