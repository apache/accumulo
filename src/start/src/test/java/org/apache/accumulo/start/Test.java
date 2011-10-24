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
package org.apache.accumulo.start;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;

import junit.framework.TestCase;

import org.apache.accumulo.start.classloader.AccumuloClassLoader;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Test extends TestCase {
  
  private static final Logger log = Logger.getLogger(Test.class);
  
  File tmpDir = null;
  File destJar = null;
  URL jarA = null;
  URL jarB = null;
  URL jarC = null;
  
  @Override
  public void setUp() {
    
    String aHome = System.getenv("ACCUMULO_HOME");
    if (aHome == null) fail("ACCUMULO_HOME must be set");
    tmpDir = new File(aHome + "/lib/ext");
    if (!tmpDir.exists())
      tmpDir.mkdir();
    destJar = new File(tmpDir, "Test.jar");
    if (destJar.exists()) {
      destJar.delete();
      try {
        Thread.sleep(3000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    
    jarA = Test.class.getResource("/ClassLoaderTestA/Test.jar");
    assertNotNull(jarA);
    jarB = Test.class.getResource("/ClassLoaderTestB/Test.jar");
    assertNotNull(jarB);
    jarC = Test.class.getResource("/ClassLoaderTestC/Test.jar");
    assertNotNull(jarC);
  }
  
  private void copyJar(URL jar) throws Exception {
    if (destJar.exists()) {
      destJar.delete();
    }
    // make sure the new jar has a different timestamp
    // must sleep at least 1 sec between creating jars, because java caches zip files
    // based on last mod time... the granularity of last mod time is only to the
    // second! If you create, deleted, create a jar file in the same second java will
    // not pick up the new jar (even if using a new URLClassLoader because caching is in
    // the native zip code and is based on file name and last access time)
    
    destJar.createNewFile();
    destJar.deleteOnExit();
    OutputStream os = new FileOutputStream(destJar);
    
    File jarFile = new File(jar.toURI());
    InputStream is = new FileInputStream(jarFile);
    
    IOUtils.copyLarge(is, os);
    is.close();
    os.close();
    // give the class loader time to pick up the new jar
    Thread.sleep(1500);
  }
  
  public test.Test create() throws Exception {
    ClassLoader cl = AccumuloClassLoader.getClassLoader();
    // Load the TestObject class from the new classloader.
    cl.loadClass("test.TestObject");
    Class<?> c = Class.forName("test.TestObject", true, cl);
    Object o = c.newInstance();
    if (o instanceof test.Test) {
      return (test.Test) o;
    } else {
      throw new Exception("Not instance of TestObject");
    }
  }
  
  public void testReloadingClassLoader() throws Exception {
    BasicConfigurator.configure();
    // comment to see logging:
    Logger.getRootLogger().setLevel(Level.ERROR);
    
    // Copy JarA to the dir
    if (log.isDebugEnabled()) log.debug("Test with Jar A");
    copyJar(jarA);
    // Load the TestObject class from the new classloader.
    test.Test a = create();
    assertEquals(a.hello(), "Hello from testA");
    assertTrue(a.add() == 1);
    assertTrue(a.add() == 2);
    // Copy jarB and wait to reload
    if (log.isDebugEnabled()) log.debug("Test with Jar B");
    copyJar(jarB);
    test.Test b = create();
    assertEquals(a.hello(), "Hello from testA");
    assertEquals(b.hello(), "Hello from testB");
    assertTrue(b.add() == 1);
    assertTrue(b.add() == 2);
    assertTrue(a.add() == 3);
    assertTrue(a.add() == 4);
    if (log.isDebugEnabled()) log.debug("Test with Jar C");
    copyJar(jarC);
    test.Test c = create();
    assertEquals(a.hello(), "Hello from testA");
    assertEquals(b.hello(), "Hello from testB");
    assertEquals(c.hello(), "Hello from testC");
    assertTrue(c.add() == 1);
    assertTrue(c.add() == 2);
    assertTrue(b.add() == 3);
    assertTrue(b.add() == 4);
    assertTrue(a.add() == 5);
    assertTrue(a.add() == 6);
    
    if (log.isDebugEnabled()) log.debug("Deleting jar");
    assertTrue(destJar.delete());
    // give the class loader time to remove the classes from the deleted jar
    Thread.sleep(1500);
    try {
      create();
      assertTrue(false);
    } catch (ClassNotFoundException cnfe) {}
    
    // uncomment this block when #2987 is fixed
    if (log.isDebugEnabled()) log.debug("Test with Jar C");
    copyJar(jarC);
    test.Test e = create();
    assertEquals(e.hello(), "Hello from testC");
    
  }
  
}
