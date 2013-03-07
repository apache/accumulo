/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.util;

import java.util.Iterator;

import junit.framework.TestCase;

import org.apache.accumulo.core.util.shell.commands.EscapeTokenizer;

public class EscapeTokenizerTest extends TestCase {
  
  public void test1() {
    EscapeTokenizer et = new EscapeTokenizer("wat,2", ",");
    Iterator<String> iter = et.iterator();
    assertTrue(iter.next().equals("wat"));
    assertTrue(iter.next().equals("2"));
    assertTrue(!iter.hasNext());
  }
  
  public void test2() {
    EscapeTokenizer et = new EscapeTokenizer("option1=2,option2=3,", ",=");
    Iterator<String> iter = et.iterator();
    assertTrue(iter.next().equals("option1"));
    assertTrue(iter.next().equals("2"));
    assertTrue(iter.next().equals("option2"));
    assertTrue(iter.next().equals("3"));
    assertTrue(!iter.hasNext());
  }
  
  public void test3() {
    EscapeTokenizer et = new EscapeTokenizer("option\\3=2,option4=3,", ",=");
    Iterator<String> iter = et.iterator();
    assertTrue(iter.next().equals("option\\3"));
    assertTrue(iter.next().equals("2"));
    assertTrue(iter.next().equals("option4"));
    assertTrue(iter.next().equals("3"));
    assertTrue(!iter.hasNext());
  }
  
  public void test4() {
    EscapeTokenizer et = new EscapeTokenizer("option\\=2=2,option4=3,", ",=");
    Iterator<String> iter = et.iterator();
    assertTrue(iter.next().equals("option=2"));
    assertTrue(iter.next().equals("2"));
    assertTrue(iter.next().equals("option4"));
    assertTrue(iter.next().equals("3"));
    assertTrue(!iter.hasNext());
  }
  
  public void test5() {
    EscapeTokenizer et = new EscapeTokenizer("test\\=,\\=2=4", ",=");
    Iterator<String> iter = et.iterator();
    assertTrue(iter.next().equals("test="));
    assertTrue(iter.next().equals("=2"));
    assertTrue(iter.next().equals("4"));
    assertTrue(!iter.hasNext());
  }
  
  public void test6() {
    EscapeTokenizer et = new EscapeTokenizer("wat\\,2", ",");
    Iterator<String> iter = et.iterator();
    assertTrue(iter.next().equals("wat,2"));
    assertTrue(!iter.hasNext());
  }
  
  public void test7() {
    EscapeTokenizer et = new EscapeTokenizer("wat\\=2=4", ",=");
    Iterator<String> iter = et.iterator();
    assertTrue(iter.next().equals("wat=2"));
    assertTrue(iter.next().equals("4"));
    assertTrue(!iter.hasNext());
  }
  
  public void test8() {
    EscapeTokenizer et = new EscapeTokenizer("1,2,3,4", ",");
    Iterator<String> iter = et.iterator();
    assertTrue(iter.next().equals("1"));
    assertTrue(iter.next().equals("2"));
    assertTrue(iter.next().equals("3"));
    assertTrue(iter.next().equals("4"));
    assertTrue(!iter.hasNext());
  }
  
  public void test9() {
    EscapeTokenizer et = new EscapeTokenizer("1\\,2,3,4", ",");
    Iterator<String> iter = et.iterator();
    assertTrue(iter.next().equals("1,2"));
    assertTrue(iter.next().equals("3"));
    assertTrue(iter.next().equals("4"));
    assertTrue(!iter.hasNext());
  }
  
  public void test10() {
    EscapeTokenizer et = new EscapeTokenizer("1,\2,3,4", ",");
    Iterator<String> iter = et.iterator();
    assertTrue(iter.next().equals("1"));
    assertTrue(iter.next().equals("\2"));
    assertTrue(iter.next().equals("3"));
    assertTrue(iter.next().equals("4"));
    assertTrue(!iter.hasNext());
  }
  
  public void test11() {
    EscapeTokenizer et = new EscapeTokenizer("1,,,,,2,3,4", ",");
    Iterator<String> iter = et.iterator();
    assertTrue(iter.next().equals("1"));
    assertTrue(iter.next().equals("2"));
    assertTrue(iter.next().equals("3"));
    assertTrue(iter.next().equals("4"));
    assertTrue(!iter.hasNext());
  }
}
