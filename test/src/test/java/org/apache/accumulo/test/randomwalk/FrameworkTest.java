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

import java.io.File;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import junit.framework.TestCase;

import org.apache.accumulo.test.randomwalk.unit.CreateTable;
import org.junit.Assert;

public class FrameworkTest extends TestCase {

  public void testXML() {

    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder docbuilder;

    SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
    Schema moduleSchema = null;
    try {
      moduleSchema = sf.newSchema(new File(this.getClass().getResource("/randomwalk/module.xsd").toURI()));
    } catch (Exception e) {
      Assert.fail("Caught exception: " + e);
    }

    dbf.setSchema(moduleSchema);

    try {
      File f = new File(this.getClass().getResource("/randomwalk/Basic.xml").toURI());
      docbuilder = dbf.newDocumentBuilder();
      docbuilder.parse(f);
    } catch (Exception e) {
      Assert.fail("Caught exception: " + e);
    }
  }

  public void testRWTest() {

    Test t1 = new CreateTable();
    assertTrue(t1.toString().equals("org.apache.accumulo.test.randomwalk.unit.CreateTable"));

    Test t2 = new CreateTable();
    assertTrue(t1.equals(t2));
  }

}
