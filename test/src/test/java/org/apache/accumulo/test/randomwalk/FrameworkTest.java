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
package org.apache.accumulo.test.randomwalk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.accumulo.test.randomwalk.unit.CreateTable;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public class FrameworkTest {

  // Need to use fully qualified name here because of conflict with org.apache.accumulo.test.randomwalk.Test
  @org.junit.Test
  public void testXML() throws SAXException, URISyntaxException, ParserConfigurationException, IOException {
    SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
    Schema moduleSchema = sf.newSchema(getFile("/randomwalk/module.xsd"));

    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    dbf.setSchema(moduleSchema);

    DocumentBuilder docbuilder = dbf.newDocumentBuilder();
    Document document = docbuilder.parse(getFile("/randomwalk/Basic.xml"));

    assertNotEquals("Parsing randomwalk xml should result in nodes.", 0, document.getChildNodes().getLength());
  }

  private File getFile(String resource) throws URISyntaxException {
    return new File(this.getClass().getResource(resource).toURI());
  }

  @org.junit.Test
  public void testRWTest() {
    Test t1 = new CreateTable();
    assertEquals("org.apache.accumulo.test.randomwalk.unit.CreateTable", t1.toString());

    Test t2 = new CreateTable();
    assertEquals("CreateTable test nodes were not equal.", t1, t2);
  }

}
