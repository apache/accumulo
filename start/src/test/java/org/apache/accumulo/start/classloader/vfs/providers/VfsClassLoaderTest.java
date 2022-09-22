/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.start.classloader.vfs.providers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URL;

import org.apache.accumulo.start.test.AccumuloDFSBase;
import org.apache.commons.vfs2.FileChangeEvent;
import org.apache.commons.vfs2.FileListener;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.impl.DefaultFileMonitor;
import org.apache.commons.vfs2.impl.VFSClassLoader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class VfsClassLoaderTest extends AccumuloDFSBase {

  private static final Path TEST_DIR = new Path(getHdfsUri() + "/test-dir");

  private FileSystem hdfs = null;
  private VFSClassLoader cl = null;

  @BeforeEach
  public void setup() throws Exception {

    this.hdfs = cluster.getFileSystem();
    this.hdfs.mkdirs(TEST_DIR);

    // Copy jar file to TEST_DIR
    URL jarPath = this.getClass().getResource("/HelloWorld.jar");
    Path src = new Path(jarPath.toURI().toString());
    Path dst = new Path(TEST_DIR, src.getName());
    this.hdfs.copyFromLocalFile(src, dst);

    FileObject testDir = vfs.resolveFile(TEST_DIR.toUri().toString());
    FileObject[] dirContents = testDir.getChildren();

    // Point the VFSClassLoader to all of the objects in TEST_DIR
    this.cl = new VFSClassLoader(dirContents, vfs);
  }

  @Test
  public void testGetClass() throws Exception {
    Class<?> helloWorldClass = this.cl.loadClass("test.HelloWorld");
    Object o = helloWorldClass.getDeclaredConstructor().newInstance();
    assertEquals("Hello World!", o.toString());
  }

  @Test
  public void testFileMonitor() throws Exception {
    MyFileMonitor listener = new MyFileMonitor();
    DefaultFileMonitor monitor = new DefaultFileMonitor(listener);
    monitor.setRecursive(true);
    FileObject testDir = vfs.resolveFile(TEST_DIR.toUri().toString());
    monitor.addFile(testDir);
    monitor.start();

    // Copy jar file to a new file name
    URL jarPath = this.getClass().getResource("/HelloWorld.jar");
    Path src = new Path(jarPath.toURI().toString());
    Path dst = new Path(TEST_DIR, "HelloWorld2.jar");
    this.hdfs.copyFromLocalFile(src, dst);

    // VFS-487 significantly wait to avoid failure
    Thread.sleep(7000);
    assertTrue(listener.isFileCreated());

    // Update the jar
    jarPath = this.getClass().getResource("/HelloWorld.jar");
    src = new Path(jarPath.toURI().toString());
    dst = new Path(TEST_DIR, "HelloWorld2.jar");
    this.hdfs.copyFromLocalFile(src, dst);

    // VFS-487 significantly wait to avoid failure
    Thread.sleep(7000);
    assertTrue(listener.isFileChanged());

    this.hdfs.delete(dst, false);
    // VFS-487 significantly wait to avoid failure
    Thread.sleep(7000);
    assertTrue(listener.isFileDeleted());

    monitor.stop();

  }

  @AfterEach
  public void tearDown() throws Exception {
    this.hdfs.delete(TEST_DIR, true);
  }

  public static class MyFileMonitor implements FileListener {

    private boolean fileChanged = false;
    private boolean fileDeleted = false;
    private boolean fileCreated = false;

    @Override
    public void fileCreated(FileChangeEvent event) throws Exception {
      // System.out.println(event.getFile() + " created");
      this.fileCreated = true;
    }

    @Override
    public void fileDeleted(FileChangeEvent event) throws Exception {
      // System.out.println(event.getFile() + " deleted");
      this.fileDeleted = true;
    }

    @Override
    public void fileChanged(FileChangeEvent event) throws Exception {
      // System.out.println(event.getFile() + " changed");
      this.fileChanged = true;
    }

    public boolean isFileChanged() {
      return fileChanged;
    }

    public boolean isFileDeleted() {
      return fileDeleted;
    }

    public boolean isFileCreated() {
      return fileCreated;
    }

  }
}
