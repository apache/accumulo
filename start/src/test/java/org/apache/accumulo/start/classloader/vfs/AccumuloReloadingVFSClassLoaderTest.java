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

import java.net.URL;

import org.apache.accumulo.test.AccumuloDFSBase;
import org.apache.commons.vfs2.FileObject;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AccumuloReloadingVFSClassLoaderTest extends AccumuloDFSBase {

  private static final Path TEST_DIR = new Path(HDFS_URI + "/test-dir");

  private FileSystem hdfs = null;
  private AccumuloReloadingVFSClassLoader cl = null;
 
  @Before
  public void setup() throws Exception {
    Logger.getRootLogger().setLevel(Level.ERROR);

    this.hdfs = cluster.getFileSystem();
    this.hdfs.mkdirs(TEST_DIR);
    
    //Copy jar file to TEST_DIR
    URL jarPath = this.getClass().getResource("/HelloWorld.jar");
    Path src = new Path(jarPath.toURI().toString());
    Path dst = new Path(TEST_DIR, src.getName());
    this.hdfs.copyFromLocalFile(src, dst);
    
  }
  
  @Test
  public void testConstructor() throws Exception {
    FileObject testDir = vfs.resolveFile(TEST_DIR.toUri().toString());
    FileObject[] dirContents = testDir.getChildren();
    cl = new AccumuloReloadingVFSClassLoader(dirContents, vfs, ClassLoader.getSystemClassLoader());
    FileObject[] files = cl.getFiles();
    Assert.assertArrayEquals(dirContents, files);
  }
  
  @Test
  public void testReloading() throws Exception {
    FileObject testDir = vfs.resolveFile(TEST_DIR.toUri().toString());
    FileObject[] dirContents = testDir.getChildren();
    cl = new AccumuloReloadingVFSClassLoader(dirContents, vfs, ClassLoader.getSystemClassLoader(), 1000);
    FileObject[] files = cl.getFiles();
    Assert.assertArrayEquals(dirContents, files);

    Class<?> clazz1 = cl.loadClass("test.HelloWorld");
    Object o1 = clazz1.newInstance();
    Assert.assertEquals("Hello World!", o1.toString());

    //Check that the class is the same before the update
    Class<?> clazz1_5 = cl.loadClass("test.HelloWorld");
    Assert.assertEquals(clazz1, clazz1_5);
    
    //Update the class
    URL jarPath = this.getClass().getResource("/HelloWorld.jar");
    Path src = new Path(jarPath.toURI().toString());
    Path dst = new Path(TEST_DIR, "HelloWorld.jar");
    this.hdfs.copyFromLocalFile(src, dst);

    //Wait for the monitor to notice
    Thread.sleep(2000);
    
    Class<?> clazz2 = cl.loadClass("test.HelloWorld");
    Object o2 = clazz2.newInstance();
    Assert.assertEquals("Hello World!", o2.toString());
    
    //This is false because they are loaded by a different classloader
    Assert.assertFalse(clazz1.equals(clazz2));
    Assert.assertFalse(o1.equals(o2));
    
  }
  
  @After
  public void tearDown() throws Exception {
    cl.close();
    this.hdfs.delete(TEST_DIR, true);
  }

}
