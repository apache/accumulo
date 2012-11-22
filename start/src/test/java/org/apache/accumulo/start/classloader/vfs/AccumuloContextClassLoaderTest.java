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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AccumuloContextClassLoaderTest extends AccumuloDFSBase {
  
  private static final Path TEST_DIR = new Path(HDFS_URI + "/test-dir");
  private static final Path TEST_DIR2 = new Path(HDFS_URI + "/test-dir2");

  private FileSystem hdfs = null;
  private AccumuloContextClassLoader cl = null;

  @Before
  public void setup() throws Exception {
    this.hdfs = cluster.getFileSystem();
    this.hdfs.mkdirs(TEST_DIR);
    this.hdfs.mkdirs(TEST_DIR2);
    
    //Copy jar file to TEST_DIR
    URL jarPath = this.getClass().getResource("/HelloWorld.jar");
    Path src = new Path(jarPath.toURI().toString());
    Path dst = new Path(TEST_DIR, src.getName());
    this.hdfs.copyFromLocalFile(src, dst);

    Path dst2 = new Path(TEST_DIR2, src.getName());
    this.hdfs.copyFromLocalFile(src, dst2);

  }

  @Test
  public void differentContexts() throws Exception {
    FileObject testDir = vfs.resolveFile(TEST_DIR.toUri().toString());
    FileObject[] dirContents = testDir.getChildren();
    cl = new AccumuloContextClassLoader(dirContents, vfs, ClassLoader.getSystemClassLoader());
    FileObject[] files = cl.getClassLoader(AccumuloContextClassLoader.DEFAULT_CONTEXT).getFiles();
    Assert.assertArrayEquals(dirContents, files);

    FileObject testDir2 = vfs.resolveFile(TEST_DIR2.toUri().toString());
    FileObject[] dirContents2 = testDir2.getChildren();
    cl.addContext("MYCONTEXT", dirContents2);
    FileObject[] files2 = cl.getClassLoader("MYCONTEXT").getFiles();
    Assert.assertArrayEquals(dirContents2, files2);
    
    Class<?> defaultContextClass = cl.loadClass("test.HelloWorld");
    Object o1 = defaultContextClass.newInstance();
    Assert.assertEquals("Hello World!", o1.toString());

    Class<?> myContextClass = cl.loadClass("MYCONTEXT", "test.HelloWorld");
    Object o2 = myContextClass.newInstance();
    Assert.assertEquals("Hello World!", o2.toString());
    
    Assert.assertFalse(defaultContextClass.equals(myContextClass));

  }
  
  @After
  public void tearDown() throws Exception {
    cl.close();
    this.hdfs.delete(TEST_DIR, true);
  }

}
