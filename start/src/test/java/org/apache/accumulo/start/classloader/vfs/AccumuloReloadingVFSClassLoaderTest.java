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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.impl.VFSClassLoader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths not set by user input")
public class AccumuloReloadingVFSClassLoaderTest {

  private TemporaryFolder folder1 =
      new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));
  String folderPath;
  private FileSystemManager vfs;

  @Before
  public void setup() throws Exception {
    vfs = ContextManagerTest.getVFS();

    folder1.create();
    folderPath = folder1.getRoot().toURI() + ".*";

    FileUtils.copyURLToFile(this.getClass().getResource("/HelloWorld.jar"),
        folder1.newFile("HelloWorld.jar"));
  }

  FileObject[] createFileSystems(FileObject[] fos) throws FileSystemException {
    FileObject[] rfos = new FileObject[fos.length];
    for (int i = 0; i < fos.length; i++) {
      if (vfs.canCreateFileSystem(fos[i])) {
        rfos[i] = vfs.createFileSystem(fos[i]);
      } else {
        rfos[i] = fos[i];
      }
    }

    return rfos;
  }

  @Test
  public void testConstructor() throws Exception {
    FileObject testDir = vfs.resolveFile(folder1.getRoot().toURI().toString());
    FileObject[] dirContents = testDir.getChildren();

    AccumuloReloadingVFSClassLoader arvcl = new AccumuloReloadingVFSClassLoader(folderPath, vfs,
        ClassLoader::getSystemClassLoader, true);

    VFSClassLoader cl = (VFSClassLoader) arvcl.getClassLoader();

    FileObject[] files = cl.getFileObjects();
    assertArrayEquals(createFileSystems(dirContents), files);

    arvcl.close();
  }

  @Test
  public void testReloading() throws Exception {
    FileObject testDir = vfs.resolveFile(folder1.getRoot().toURI().toString());
    FileObject[] dirContents = testDir.getChildren();

    AccumuloReloadingVFSClassLoader arvcl = new AccumuloReloadingVFSClassLoader(folderPath, vfs,
        ClassLoader::getSystemClassLoader, 1000, true);

    FileObject[] files = ((VFSClassLoader) arvcl.getClassLoader()).getFileObjects();
    assertArrayEquals(createFileSystems(dirContents), files);

    // set retry settings sufficiently low that not everything is reloaded in the first round
    arvcl.setMaxRetries(1);

    Class<?> clazz1 = arvcl.getClassLoader().loadClass("test.HelloWorld");
    Object o1 = clazz1.newInstance();
    assertEquals("Hello World!", o1.toString());

    // Check that the class is the same before the update
    Class<?> clazz1_5 = arvcl.getClassLoader().loadClass("test.HelloWorld");
    assertEquals(clazz1, clazz1_5);

    assertTrue(new File(folder1.getRoot(), "HelloWorld.jar").delete());

    // VFS-487 significantly wait to avoid failure
    Thread.sleep(7000);

    // Update the class
    FileUtils.copyURLToFile(this.getClass().getResource("/HelloWorld.jar"),
        folder1.newFile("HelloWorld2.jar"));

    // Wait for the monitor to notice
    // VFS-487 significantly wait to avoid failure
    Thread.sleep(7000);

    Class<?> clazz2 = arvcl.getClassLoader().loadClass("test.HelloWorld");
    Object o2 = clazz2.newInstance();
    assertEquals("Hello World!", o2.toString());

    // This is false because they are loaded by a different classloader
    assertNotEquals(clazz1, clazz2);
    assertNotEquals(o1, o2);

    arvcl.close();
  }

  @Test
  public void testReloadingWithLongerTimeout() throws Exception {
    FileObject testDir = vfs.resolveFile(folder1.getRoot().toURI().toString());
    FileObject[] dirContents = testDir.getChildren();

    AccumuloReloadingVFSClassLoader arvcl = new AccumuloReloadingVFSClassLoader(folderPath, vfs,
        ClassLoader::getSystemClassLoader, 1000, true);

    FileObject[] files = ((VFSClassLoader) arvcl.getClassLoader()).getFileObjects();
    assertArrayEquals(createFileSystems(dirContents), files);

    // set retry settings sufficiently high such that reloading happens in the first rounds
    arvcl.setMaxRetries(3);

    Class<?> clazz1 = arvcl.getClassLoader().loadClass("test.HelloWorld");
    Object o1 = clazz1.newInstance();
    assertEquals("Hello World!", o1.toString());

    // Check that the class is the same before the update
    Class<?> clazz1_5 = arvcl.getClassLoader().loadClass("test.HelloWorld");
    assertEquals(clazz1, clazz1_5);

    assertTrue(new File(folder1.getRoot(), "HelloWorld.jar").delete());

    // VFS-487 significantly wait to avoid failure
    Thread.sleep(7000);

    // Update the class
    FileUtils.copyURLToFile(this.getClass().getResource("/HelloWorld.jar"),
        folder1.newFile("HelloWorld2.jar"));

    // Wait for the monitor to notice
    // VFS-487 significantly wait to avoid failure
    Thread.sleep(7000);

    Class<?> clazz2 = arvcl.getClassLoader().loadClass("test.HelloWorld");
    Object o2 = clazz2.newInstance();
    assertEquals("Hello World!", o2.toString());

    // This is true because they are loaded by the same classloader due to the new retry
    assertTrue(clazz1.equals(clazz2));
    assertFalse(o1.equals(o2));

    arvcl.close();
  }

  @After
  public void tearDown() throws Exception {
    folder1.delete();
  }

}
