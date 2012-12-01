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
package org.apache.accumulo.start.classloader.vfs;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;

import org.apache.accumulo.test.AccumuloDFSBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

@RunWith(PowerMockRunner.class)
@PrepareForTest(AccumuloVFSClassLoader.class)
@SuppressStaticInitializationFor({"org.apache.accumulo.start.classloader.AccumuloVFSClassLoader", 
  "org.apache.log4j.LogManager"})
@PowerMockIgnore({"org.apache.log4j.*", 
  "org.apache.hadoop.log.metrics", "org.apache.commons.logging.*",
  "org.xml.*", "javax.xml.*", "org.w3c.dom.*", "org.apache.hadoop.*"})
public class AccumuloVFSClassLoaderTest extends AccumuloDFSBase {


  /*
   * Test that if not enabled, the AccumuloClassLoader is used
   */
  @Test
  public void testNoConfigChanges() throws Exception {
    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "ACC_CONF", new Configuration());
    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "lock", new Object());
    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "log", Logger.getLogger(AccumuloVFSClassLoader.class));
    ClassLoader acl = AccumuloVFSClassLoader.getClassLoader();
    Assert.assertTrue((acl instanceof URLClassLoader));
    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "loader", (AccumuloContextClassLoader) null);
  }
  
  /*
   * Test that if enabled, but not configured, that the code creates the 2nd level classloader
   */
  @Test
  public void testDefaultConfig() throws Exception {
    Configuration conf = new Configuration();
    URL defaultDir = this.getClass().getResource("/disabled");
    conf.addResource(new File(defaultDir.getPath() + "/conf/accumulo-site.xml").toURI().toURL());
    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "ACC_CONF", conf);
    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "lock", new Object());
    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "log", Logger.getLogger(AccumuloVFSClassLoader.class));
    ClassLoader acl = AccumuloVFSClassLoader.getClassLoader();
    Assert.assertTrue((acl instanceof URLClassLoader));
    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "loader", (AccumuloContextClassLoader) null);
//    URLClassLoader ucl = (URLClassLoader) acl;
//    URL[] classpath = ucl.getURLs();
//    System.out.println(Arrays.toString(classpath));
  }
  
  /*
   * Test with default context configured
   */
  @Test
  public void testDefaultContextConfigured() throws Exception {
    
    //Create default context directory
    @SuppressWarnings("resource")
    FileSystem hdfs = cluster.getFileSystem();
    Path DEFAULT = new Path("/accumulo/classpath");
    hdfs.mkdirs(DEFAULT);
    
    //Copy jar file to TEST_DIR
    URL jarPath = this.getClass().getResource("/HelloWorld.jar");
    Path src = new Path(jarPath.toURI().toString());
    Path dst = new Path(DEFAULT, src.getName());
    hdfs.copyFromLocalFile(src, dst);
    
    URL defaultDir = this.getClass().getResource("/default");
    conf.addResource(new File(defaultDir.getPath() + "/conf/accumulo-site.xml").toURI().toURL());
    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "ACC_CONF", conf);
    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "lock", new Object());
    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "log", Logger.getLogger(AccumuloVFSClassLoader.class));
    ClassLoader acl = AccumuloVFSClassLoader.getClassLoader();
    Assert.assertTrue((acl instanceof AccumuloContextClassLoader));
    AccumuloContextClassLoader accl = (AccumuloContextClassLoader) acl;
    AccumuloReloadingVFSClassLoader arvcl = accl.getClassLoader(AccumuloContextClassLoader.DEFAULT_CONTEXT);
    Assert.assertEquals(1, arvcl.getFiles().length);
    Assert.assertTrue(arvcl.getFiles()[0].getURL().toString().equals(AccumuloDFSBase.HDFS_URI + "/accumulo/classpath/HelloWorld.jar"));
    Class<?> clazz1 = arvcl.loadClass("test.HelloWorld");
    Object o1 = clazz1.newInstance();
    Assert.assertEquals("Hello World!", o1.toString());
    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "loader", (AccumuloContextClassLoader) null);
    
    hdfs.delete(DEFAULT, true);

  }

  @Test
  public void testAdditionalContextConfigured() throws Exception {

    //Create default and application1 context directory
    @SuppressWarnings("resource")
    FileSystem hdfs = cluster.getFileSystem();
    Path DEFAULT = new Path("/accumulo/classpath");
    hdfs.mkdirs(DEFAULT);
    Path APPLICATION = new Path("/application1/classpath");
    hdfs.mkdirs(APPLICATION);

    
    //Copy jar file to DEFAULT and APPLICATION directories
    URL jarPath = this.getClass().getResource("/HelloWorld.jar");
    Path src = new Path(jarPath.toURI().toString());
    Path dst = new Path(DEFAULT, src.getName());
    hdfs.copyFromLocalFile(src, dst);
    dst = new Path(APPLICATION, src.getName());
    hdfs.copyFromLocalFile(src, dst);
    
    URL defaultDir = this.getClass().getResource("/application1");
    conf.addResource(new File(defaultDir.getPath() + "/conf/accumulo-site.xml").toURI().toURL());
    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "ACC_CONF", conf);
    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "lock", new Object());
    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "log", Logger.getLogger(AccumuloVFSClassLoader.class));

    ClassLoader acl = AccumuloVFSClassLoader.getClassLoader();
    Assert.assertTrue((acl instanceof AccumuloContextClassLoader));
    AccumuloContextClassLoader accl = (AccumuloContextClassLoader) acl;
    //DEFAULT CONTEXT
    AccumuloReloadingVFSClassLoader arvcl = accl.getClassLoader(AccumuloContextClassLoader.DEFAULT_CONTEXT);
    Assert.assertEquals(1, arvcl.getFiles().length);
    Assert.assertTrue(arvcl.getFiles()[0].getURL().toString().equals(AccumuloDFSBase.HDFS_URI + "/accumulo/classpath/HelloWorld.jar"));
    Class<?> clazz1 = arvcl.loadClass("test.HelloWorld");
    Object o1 = clazz1.newInstance();
    Assert.assertEquals("Hello World!", o1.toString());

    //APPLICATION CONTEXT
    AccumuloReloadingVFSClassLoader arvcl2 = accl.getClassLoader("application1");
    Assert.assertEquals(1, arvcl2.getFiles().length);
    Assert.assertTrue(arvcl2.getFiles()[0].getURL().toString().equals(AccumuloDFSBase.HDFS_URI + "/application1/classpath/HelloWorld.jar"));
    Class<?> clazz2 = arvcl2.loadClass("test.HelloWorld");
    Object o2 = clazz2.newInstance();
    Assert.assertEquals("Hello World!", o2.toString());
    
    Assert.assertTrue(!o1.equals(o2));
    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "loader", (AccumuloContextClassLoader) null);
    
    hdfs.delete(DEFAULT, true);
    hdfs.delete(APPLICATION, true);
    
  }

  @Test
  public void testLoadClass() throws Exception {
    //Create default and application1 context directory
    @SuppressWarnings("resource")
    FileSystem hdfs = cluster.getFileSystem();
    Path DEFAULT = new Path("/accumulo/classpath");
    hdfs.mkdirs(DEFAULT);
    Path APPLICATION = new Path("/application1/classpath");
    hdfs.mkdirs(APPLICATION);

    
    //Copy jar file to DEFAULT and APPLICATION directories
    URL jarPath = this.getClass().getResource("/HelloWorld.jar");
    Path src = new Path(jarPath.toURI().toString());
    Path dst = new Path(DEFAULT, src.getName());
    hdfs.copyFromLocalFile(src, dst);
    dst = new Path(APPLICATION, src.getName());
    hdfs.copyFromLocalFile(src, dst);
    
    URL defaultDir = this.getClass().getResource("/application1");
    conf.addResource(new File(defaultDir.getPath() + "/conf/accumulo-site.xml").toURI().toURL());
    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "ACC_CONF", conf);
    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "lock", new Object());
    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "log", Logger.getLogger(AccumuloVFSClassLoader.class));

    Class<?> clazz1 = AccumuloVFSClassLoader.loadClass("test.HelloWorld");
    Object o1 = clazz1.newInstance();
    Assert.assertEquals("Hello World!", o1.toString());
    
    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "loader", (AccumuloContextClassLoader) null);
    
    hdfs.delete(DEFAULT, true);
    hdfs.delete(APPLICATION, true);
    
  }

}
