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
package org.apache.accumulo.examples.wikisearch.reader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileWriter;
import java.io.StringReader;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.apache.accumulo.core.util.ContextFactory;
import org.apache.accumulo.examples.wikisearch.ingest.WikipediaInputFormat.WikipediaInputSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

public class AggregatingRecordReaderTest {
  
  public static class MyErrorHandler implements ErrorHandler {
    
    @Override
    public void error(SAXParseException exception) throws SAXException {
      // System.out.println(exception.getMessage());
    }
    
    @Override
    public void fatalError(SAXParseException exception) throws SAXException {
      // System.out.println(exception.getMessage());
    }
    
    @Override
    public void warning(SAXParseException exception) throws SAXException {
      // System.out.println(exception.getMessage());
    }
    
  }
  
  private static final String xml1 = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + "<doc>\n" + "  <a>A</a>\n" + "  <b>B</b>\n" + "</doc>\n"
      + "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + "<doc>\n" + "  <a>C</a>\n" + "  <b>D</b>\n" + "</doc>\n" + "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
      + "<doc>\n" + "  <a>E</a>\n" + "  <b>F</b>\n" + "</doc>\n";
  
  private static final String xml2 = "  <b>B</b>\n" + "</doc>\n" + "<doc>\n" + "  <a>C</a>\n" + "  <b>D</b>\n" + "</doc>\n" + "<doc>\n" + "  <a>E</a>\n"
      + "  <b>F</b>\n" + "</doc>\n";
  
  private static final String xml3 = "<doc>\n" + "  <a>A</a>\n" + "  <b>B</b>\n" + "</doc>\n" + "<doc>\n" + "  <a>C</a>\n" + "  <b>D</b>\n" + "</doc>\n"
      + "<doc>\n" + "  <a>E</a>\n";
  
  private static final String xml4 = "<doc>" + "  <a>A</a>" + "  <b>B</b>" + "</doc>" + "<doc>" + "  <a>C</a>" + "  <b>D</b>" + "</doc>" + "<doc>"
      + "  <a>E</a>" + "  <b>F</b>" + "</doc>";
  
  private static final String xml5 = "<doc attr=\"G\">" + "  <a>A</a>" + "  <b>B</b>" + "</doc>" + "<doc>" + "  <a>C</a>" + "  <b>D</b>" + "</doc>"
      + "<doc attr=\"H\"/>" + "<doc>" + "  <a>E</a>" + "  <b>F</b>" + "</doc>" + "<doc attr=\"I\"/>";
  
  private Configuration conf = null;
  private TaskAttemptContext ctx = null;
  private static DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
  private XPathFactory xpFactory = XPathFactory.newInstance();
  private XPathExpression EXPR_A = null;
  private XPathExpression EXPR_B = null;
  private XPathExpression EXPR_ATTR = null;
  
  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.set(AggregatingRecordReader.START_TOKEN, "<doc");
    conf.set(AggregatingRecordReader.END_TOKEN, "</doc>");
    conf.set(AggregatingRecordReader.RETURN_PARTIAL_MATCHES, Boolean.toString(true));
    ctx = ContextFactory.createTaskAttemptContext(conf);
    XPath xp = xpFactory.newXPath();
    EXPR_A = xp.compile("/doc/a");
    EXPR_B = xp.compile("/doc/b");
    EXPR_ATTR = xp.compile("/doc/@attr");
  }
  
  public File createFile(String data) throws Exception {
    // Write out test file
    File f = File.createTempFile("aggReaderTest", ".xml");
    f.deleteOnExit();
    FileWriter writer = new FileWriter(f);
    writer.write(data);
    writer.flush();
    writer.close();
    return f;
  }
  
  private void testXML(Text xml, String aValue, String bValue, String attrValue) throws Exception {
    StringReader reader = new StringReader(xml.toString());
    InputSource source = new InputSource(reader);
    
    DocumentBuilder parser = factory.newDocumentBuilder();
    parser.setErrorHandler(new MyErrorHandler());
    Document root = parser.parse(source);
    assertNotNull(root);
    
    reader = new StringReader(xml.toString());
    source = new InputSource(reader);
    assertEquals(EXPR_A.evaluate(source), aValue);
    
    reader = new StringReader(xml.toString());
    source = new InputSource(reader);
    assertEquals(EXPR_B.evaluate(source), bValue);
    
    reader = new StringReader(xml.toString());
    source = new InputSource(reader);
    assertEquals(EXPR_ATTR.evaluate(source), attrValue);
  }
  
  @Test
  public void testIncorrectArgs() throws Exception {
    File f = createFile(xml1);
    
    // Create FileSplit
    Path p = new Path(f.toURI().toString());
    WikipediaInputSplit split = new WikipediaInputSplit(new FileSplit(p, 0, f.length(), null), 0);
    AggregatingRecordReader reader = new AggregatingRecordReader();
    try {
      // Clear the values for BEGIN and STOP TOKEN
      conf.set(AggregatingRecordReader.START_TOKEN, null);
      conf.set(AggregatingRecordReader.END_TOKEN, null);
      reader.initialize(split, ctx);
      // If we got here, then the code didnt throw an exception
      fail();
    } catch (Exception e) {
      // Do nothing, we succeeded
      f = null;
    }
    reader.close();
  }
  
  @Test
  public void testCorrectXML() throws Exception {
    File f = createFile(xml1);
    
    // Create FileSplit
    Path p = new Path(f.toURI().toString());
    WikipediaInputSplit split = new WikipediaInputSplit(new FileSplit(p, 0, f.length(), null), 0);
    
    // Initialize the RecordReader
    AggregatingRecordReader reader = new AggregatingRecordReader();
    reader.initialize(split, ctx);
    assertTrue(reader.nextKeyValue());
    testXML(reader.getCurrentValue(), "A", "B", "");
    assertTrue(reader.nextKeyValue());
    testXML(reader.getCurrentValue(), "C", "D", "");
    assertTrue(reader.nextKeyValue());
    testXML(reader.getCurrentValue(), "E", "F", "");
    assertTrue(!reader.nextKeyValue());
    
  }
  
  @Test
  public void testPartialXML() throws Exception {
    File f = createFile(xml2);
    
    // Create FileSplit
    Path p = new Path(f.toURI().toString());
    WikipediaInputSplit split = new WikipediaInputSplit(new FileSplit(p, 0, f.length(), null), 0);
    
    // Initialize the RecordReader
    AggregatingRecordReader reader = new AggregatingRecordReader();
    reader.initialize(split, ctx);
    assertTrue(reader.nextKeyValue());
    testXML(reader.getCurrentValue(), "C", "D", "");
    assertTrue(reader.nextKeyValue());
    testXML(reader.getCurrentValue(), "E", "F", "");
    assertTrue(!reader.nextKeyValue());
  }
  
  public void testPartialXML2WithNoPartialRecordsReturned() throws Exception {
    conf.set(AggregatingRecordReader.RETURN_PARTIAL_MATCHES, Boolean.toString(false));
    File f = createFile(xml3);
    
    // Create FileSplit
    Path p = new Path(f.toURI().toString());
    WikipediaInputSplit split = new WikipediaInputSplit(new FileSplit(p, 0, f.length(), null), 0);
    
    // Initialize the RecordReader
    AggregatingRecordReader reader = new AggregatingRecordReader();
    reader.initialize(split, ctx);
    assertTrue(reader.nextKeyValue());
    testXML(reader.getCurrentValue(), "A", "B", "");
    assertTrue(reader.nextKeyValue());
    testXML(reader.getCurrentValue(), "C", "D", "");
    assertTrue(!reader.nextKeyValue());
  }
  
  @Test
  public void testPartialXML2() throws Exception {
    File f = createFile(xml3);
    
    // Create FileSplit
    Path p = new Path(f.toURI().toString());
    WikipediaInputSplit split = new WikipediaInputSplit(new FileSplit(p, 0, f.length(), null), 0);
    
    // Initialize the RecordReader
    AggregatingRecordReader reader = new AggregatingRecordReader();
    reader.initialize(split, ctx);
    assertTrue(reader.nextKeyValue());
    testXML(reader.getCurrentValue(), "A", "B", "");
    assertTrue(reader.nextKeyValue());
    testXML(reader.getCurrentValue(), "C", "D", "");
    assertTrue(reader.nextKeyValue());
    try {
      testXML(reader.getCurrentValue(), "E", "", "");
      fail("Fragment returned, and it somehow passed XML parsing.");
    } catch (SAXParseException e) {
      // ignore
    }
    assertTrue(!reader.nextKeyValue());
  }
  
  @Test
  public void testLineSplitting() throws Exception {
    File f = createFile(xml4);
    
    // Create FileSplit
    Path p = new Path(f.toURI().toString());
    WikipediaInputSplit split = new WikipediaInputSplit(new FileSplit(p, 0, f.length(), null), 0);
    
    // Initialize the RecordReader
    AggregatingRecordReader reader = new AggregatingRecordReader();
    reader.initialize(split, ctx);
    assertTrue(reader.nextKeyValue());
    testXML(reader.getCurrentValue(), "A", "B", "");
    assertTrue(reader.nextKeyValue());
    testXML(reader.getCurrentValue(), "C", "D", "");
    assertTrue(reader.nextKeyValue());
    testXML(reader.getCurrentValue(), "E", "F", "");
    assertTrue(!reader.nextKeyValue());
  }
  
  @Test
  public void testNoEndTokenHandling() throws Exception {
    File f = createFile(xml5);
    // Create FileSplit
    Path p = new Path(f.toURI().toString());
    WikipediaInputSplit split = new WikipediaInputSplit(new FileSplit(p, 0, f.length(), null), 0);
    
    // Initialize the RecordReader
    AggregatingRecordReader reader = new AggregatingRecordReader();
    reader.initialize(split, ctx);
    assertTrue("Not enough records returned.", reader.nextKeyValue());
    testXML(reader.getCurrentValue(), "A", "B", "G");
    assertTrue("Not enough records returned.", reader.nextKeyValue());
    testXML(reader.getCurrentValue(), "C", "D", "");
    assertTrue("Not enough records returned.", reader.nextKeyValue());
    testXML(reader.getCurrentValue(), "", "", "H");
    assertTrue("Not enough records returned.", reader.nextKeyValue());
    testXML(reader.getCurrentValue(), "E", "F", "");
    assertTrue("Not enough records returned.", reader.nextKeyValue());
    testXML(reader.getCurrentValue(), "", "", "I");
    assertTrue("Too many records returned.", !reader.nextKeyValue());
  }
  
}
