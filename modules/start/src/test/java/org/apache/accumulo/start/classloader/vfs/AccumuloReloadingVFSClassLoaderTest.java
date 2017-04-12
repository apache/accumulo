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

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.impl.VFSClassLoader;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class AccumuloReloadingVFSClassLoaderTest {

  private TemporaryFolder folder1 = new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));
  String folderPath;
  private FileSystemManager vfs;

  @Before
  public void setup() throws Exception {
    Logger.getRootLogger().setLevel(Level.ERROR);

    vfs = ContextManagerTest.getVFS();

    folder1.create();
    folderPath = folder1.getRoot().toURI().toString() + ".*";

    FileUtils.copyURLToFile(this.getClass().getResource("/HelloWorld.jar"), folder1.newFile("HelloWorld.jar"));
  }

  FileObject[] createFileSystems(FileObject[] fos) throws FileSystemException {
    FileObject[] rfos = new FileObject[fos.length];
    for (int i = 0; i < fos.length; i++) {
      if (vfs.canCreateFileSystem(fos[i]))
        rfos[i] = vfs.createFileSystem(fos[i]);
      else
        rfos[i] = fos[i];
    }

    return rfos;
  }

  @Test
  public void testConstructor() throws Exception {
    FileObject testDir = vfs.resolveFile(folder1.getRoot().toURI().toString());
    FileObject[] dirContents = testDir.getChildren();

    AccumuloReloadingVFSClassLoader arvcl = new AccumuloReloadingVFSClassLoader(folderPath, vfs, new ReloadingClassLoader() {
      @Override
      public ClassLoader getClassLoader() {
        return ClassLoader.getSystemClassLoader();
      }
    }, true);

    VFSClassLoader cl = (VFSClassLoader) arvcl.getClassLoader();

    FileObject[] files = cl.getFileObjects();
    Assert.assertArrayEquals(createFileSystems(dirContents), files);

    arvcl.close();
  }

  @Test
  public void testReloading() throws Exception {
    FileObject testDir = vfs.resolveFile(folder1.getRoot().toURI().toString());
    FileObject[] dirContents = testDir.getChildren();

    AccumuloReloadingVFSClassLoader arvcl = new AccumuloReloadingVFSClassLoader(folderPath, vfs, new ReloadingClassLoader() {
      @Override
      public ClassLoader getClassLoader() {
        return ClassLoader.getSystemClassLoader();
      }
    }, 1000, true);

    FileObject[] files = ((VFSClassLoader) arvcl.getClassLoader()).getFileObjects();
    Assert.assertArrayEquals(createFileSystems(dirContents), files);

    Class<?> clazz1 = arvcl.getClassLoader().loadClass("test.HelloWorld");
    Object o1 = clazz1.newInstance();
    Assert.assertEquals("Hello World!", o1.toString());

    // Check that the class is the same before the update
    Class<?> clazz1_5 = arvcl.getClassLoader().loadClass("test.HelloWorld");
    Assert.assertEquals(clazz1, clazz1_5);

    assertTrue(new File(folder1.getRoot(), "HelloWorld.jar").delete());

    // VFS-487 significantly wait to avoid failure
    Thread.sleep(7000);

    // Update the class
    FileUtils.copyURLToFile(this.getClass().getResource("/HelloWorld.jar"), folder1.newFile("HelloWorld2.jar"));

    // Wait for the monitor to notice
    // VFS-487 significantly wait to avoid failure
    Thread.sleep(7000);

    Class<?> clazz2 = arvcl.getClassLoader().loadClass("test.HelloWorld");
    Object o2 = clazz2.newInstance();
    Assert.assertEquals("Hello World!", o2.toString());

    // This is false because they are loaded by a different classloader
    Assert.assertFalse(clazz1.equals(clazz2));
    Assert.assertFalse(o1.equals(o2));

    arvcl.close();
  }

  // This test fails because of an error with the underlying monitor (ACCUMULO-1507/VFS-487). Uncomment when this has been addressed.
  //
  // This is caused by the filed being deleted and then readded in the same monitor tick. This causes the file to ultimately register the deletion over any
  // other events.
  @Test
  @Ignore
  public void testFastDeleteAndReAdd() throws Exception {
    FileObject testDir = vfs.resolveFile(folder1.getRoot().toURI().toString());
    FileObject[] dirContents = testDir.getChildren();

    AccumuloReloadingVFSClassLoader arvcl = new AccumuloReloadingVFSClassLoader(folderPath, vfs, new ReloadingClassLoader() {

      @Override
      public ClassLoader getClassLoader() {
        return ClassLoader.getSystemClassLoader();
      }
    }, 1000, true);

    FileObject[] files = ((VFSClassLoader) arvcl.getClassLoader()).getFileObjects();
    Assert.assertArrayEquals(createFileSystems(dirContents), files);

    Class<?> clazz1 = arvcl.getClassLoader().loadClass("test.HelloWorld");
    Object o1 = clazz1.newInstance();
    Assert.assertEquals("Hello World!", o1.toString());

    // Check that the class is the same before the update
    Class<?> clazz1_5 = arvcl.getClassLoader().loadClass("test.HelloWorld");
    Assert.assertEquals(clazz1, clazz1_5);

    assertTrue(new File(folder1.getRoot(), "HelloWorld.jar").delete());

    // Update the class
    FileUtils.copyURLToFile(this.getClass().getResource("/HelloWorld.jar"), folder1.newFile("HelloWorld.jar"));

    // Wait for the monitor to notice
    // VFS-487 significantly wait to avoid failure
    Thread.sleep(7000);

    Class<?> clazz2 = arvcl.getClassLoader().loadClass("test.HelloWorld");
    Object o2 = clazz2.newInstance();
    Assert.assertEquals("Hello World!", o2.toString());

    // This is false because they are loaded by a different classloader
    Assert.assertFalse(clazz1.equals(clazz2));
    Assert.assertFalse(o1.equals(o2));

    arvcl.close();
  }

  @Test
  @Ignore
  public void testModifiedClass() throws Exception {

    FileObject testDir = vfs.resolveFile(folder1.getRoot().toURI().toString());
    FileObject[] dirContents = testDir.getChildren();

    AccumuloReloadingVFSClassLoader arvcl = new AccumuloReloadingVFSClassLoader(folderPath, vfs, new ReloadingClassLoader() {
      @Override
      public ClassLoader getClassLoader() {
        return ClassLoader.getSystemClassLoader();
      }
    }, 1000, true);

    FileObject[] files = ((VFSClassLoader) arvcl.getClassLoader()).getFileObjects();
    Assert.assertArrayEquals(createFileSystems(dirContents), files);

    ClassLoader loader1 = arvcl.getClassLoader();
    Class<?> clazz1 = loader1.loadClass("test.HelloWorld");
    Object o1 = clazz1.newInstance();
    Assert.assertEquals("Hello World!", o1.toString());

    // Check that the class is the same before the update
    Class<?> clazz1_5 = arvcl.getClassLoader().loadClass("test.HelloWorld");
    Assert.assertEquals(clazz1, clazz1_5);

    // java does aggressive caching of jar files. When using java code to read jar files that are created in the same second, it will only see the first jar
    // file
    Thread.sleep(1000);

    assertTrue(new File(folder1.getRoot(), "HelloWorld.jar").delete());

    // Update the class
    FileUtils.copyURLToFile(this.getClass().getResource("/HelloWorld2.jar"), folder1.newFile("HelloWorld.jar"));

    // Wait for the monitor to notice
    // VFS-487 significantly wait to avoid failure
    Thread.sleep(7000);

    Class<?> clazz2 = arvcl.getClassLoader().loadClass("test.HelloWorld");
    Object o2 = clazz2.newInstance();
    Assert.assertEquals("Hallo Welt", o2.toString());

    // This is false because they are loaded by a different classloader
    Assert.assertFalse(clazz1.equals(clazz2));
    Assert.assertFalse(o1.equals(o2));

    Class<?> clazz3 = loader1.loadClass("test.HelloWorld");
    Object o3 = clazz3.newInstance();
    Assert.assertEquals("Hello World!", o3.toString());

    arvcl.close();
  }

  @After
  public void tearDown() throws Exception {
    folder1.delete();
  }

}
