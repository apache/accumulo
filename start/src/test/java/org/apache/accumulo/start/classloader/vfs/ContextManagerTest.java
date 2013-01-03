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
import java.util.HashSet;

import org.apache.accumulo.start.classloader.vfs.ContextManager.ContextConfig;
import org.apache.accumulo.test.AccumuloDFSBase;
import org.apache.commons.vfs2.FileObject;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ContextManagerTest extends AccumuloDFSBase {
  
  private static final Path TEST_DIR = new Path(HDFS_URI + "/test-dir");
  private static final Path TEST_DIR2 = new Path(HDFS_URI + "/test-dir2");

  private FileSystem hdfs = null;

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
    
    ContextManager cm = new ContextManager(vfs, ClassLoader.getSystemClassLoader());
    cm.setContextConfig(new ContextConfig() {
      @Override
      public String getContextURIs(String context) {
        if (context.equals("CX1")) {
          return new Path(TEST_DIR, "HelloWorld.jar").toUri().toString();
        } else if (context.equals("CX2")) {
          return new Path(TEST_DIR2, "HelloWorld.jar").toUri().toString();
        }
        return null;
      }
      
      @Override
      public boolean isIsolated(String context) {
        // TODO Auto-generated method stub
        return false;
      }
    });

    FileObject testDir = vfs.resolveFile(TEST_DIR.toUri().toString());
    FileObject[] dirContents = testDir.getChildren();
    ClassLoader cl1 = cm.getClassLoader("CX1");
    FileObject[] files = ((AccumuloReloadingVFSClassLoader) cl1).getFiles();
    Assert.assertArrayEquals(dirContents, files);


    FileObject testDir2 = vfs.resolveFile(TEST_DIR2.toUri().toString());
    FileObject[] dirContents2 = testDir2.getChildren();
    ClassLoader cl2 = cm.getClassLoader("CX2");
    FileObject[] files2 = ((AccumuloReloadingVFSClassLoader) cl2).getFiles();
    Assert.assertArrayEquals(dirContents2, files2);
    
    Class<?> defaultContextClass = cl1.loadClass("test.HelloWorld");
    Object o1 = defaultContextClass.newInstance();
    Assert.assertEquals("Hello World!", o1.toString());

    Class<?> myContextClass = cl2.loadClass("test.HelloWorld");
    Object o2 = myContextClass.newInstance();
    Assert.assertEquals("Hello World!", o2.toString());
    
    Assert.assertFalse(defaultContextClass.equals(myContextClass));

    cm.removeUnusedContexts(new HashSet<String>());
  }
  
  @After
  public void tearDown() throws Exception {
    this.hdfs.delete(TEST_DIR, true);
  }

}
