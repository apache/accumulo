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
		assertTrue(t1.toString().equals("accumulo.server.test.randomwalk.unit.CreateTable"));
		
		Test t2 = new CreateTable();
		assertTrue(t1.equals(t2));
	}
	
	public void testModule() {
		
		// don't run test if accumulo home is not set
		String acuHome = System.getenv("ACCUMULO_HOME");
		if (acuHome == null)
			return;
		
		String confDir = acuHome+"/test/system/randomwalk/conf/";
		Framework.setConfigDir(confDir);
		try {
			Module module = new Module(new File(confDir+"modules/unit/Basic.xml"));
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
		String confDir = acuHome+"/test/system/randomwalk/conf/";
		framework.run("unit/Basic.xml", new State(new Properties()), confDir);		
	}
}
