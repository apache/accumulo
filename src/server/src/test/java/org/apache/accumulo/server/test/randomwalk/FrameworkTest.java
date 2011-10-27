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
package org.apache.accumulo.server.test.randomwalk;

import java.io.File;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.XMLConstants;

import org.apache.accumulo.server.test.randomwalk.Framework;
import org.apache.accumulo.server.test.randomwalk.Module;
import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;
import org.apache.accumulo.server.test.randomwalk.unit.CreateTable;

import junit.framework.TestCase;

public class FrameworkTest extends TestCase {
  
  public void testXML() {
    
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder docbuilder;
    
    SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
    Schema moduleSchema = null;
    try {
      moduleSchema = sf.newSchema(new File(this.getClass().getClassLoader().getResource("randomwalk/module.xsd").toURI()));
    } catch (Exception e) {
      e.printStackTrace();
    }
    
    dbf.setSchema(moduleSchema);
    
    try {
      File f = new File(this.getClass().getClassLoader().getResource("randomwalk/Basic.xml").toURI());
      docbuilder = dbf.newDocumentBuilder();
      docbuilder.parse(f);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  public void testRWTest() {
    
    Test t1 = new CreateTable();
    assertTrue(t1.toString().equals("org.apache.accumulo.server.test.randomwalk.unit.CreateTable"));
    
    Test t2 = new CreateTable();
    assertTrue(t1.equals(t2));
  }
  
  public void testModule() {
    
    // don't run test if accumulo home is not set
    String acuHome = System.getenv("ACCUMULO_HOME");
    if (acuHome == null)
      return;
    
    String confDir = acuHome + "/test/system/randomwalk/conf/";
    Framework.setConfigDir(confDir);
    try {
      Module module = new Module(new File(confDir + "modules/unit/Basic.xml"));
      module.visit(new State(new Properties()), new Properties());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  public void testFramework() {
    
    // don't run test if accumulo home is not set
    String acuHome = System.getenv("ACCUMULO_HOME");
    if (acuHome == null)
      return;
    
    Framework framework = Framework.getInstance();
    String confDir = acuHome + "/test/system/randomwalk/conf/";
    framework.run("unit/Basic.xml", new State(new Properties()), confDir);
  }
}
