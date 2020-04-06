/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.start.classloader.vfs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import java.io.File;
import java.util.HashSet;

import org.apache.accumulo.start.classloader.vfs.ContextManager.ContextConfig;
import org.apache.commons.io.FileUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.impl.VFSClassLoader;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths not set by user input")
public class ContextManagerTest {

  @Rule
  public TemporaryFolder tempFolder =
      new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));

  private FileSystemManager vfs;
  private File folder1;
  private File folder2;
  private String uri1;
  private String uri2;

  static FileSystemManager getVFS() {
    try {
      return AccumuloVFSClassLoader.generateVfs();
    } catch (FileSystemException e) {
      throw new RuntimeException("Error setting up VFS", e);
    }
  }

  @Before
  public void setup() throws Exception {

    vfs = getVFS();
    folder1 = tempFolder.newFolder();
    folder2 = tempFolder.newFolder();

    FileUtils.copyURLToFile(this.getClass().getResource("/HelloWorld.jar"),
        new File(folder1, "HelloWorld.jar"));
    FileUtils.copyURLToFile(this.getClass().getResource("/HelloWorld.jar"),
        new File(folder2, "HelloWorld.jar"));

    uri1 = new File(folder1, "HelloWorld.jar").toURI().toString();
    uri2 = folder2.toURI() + ".*";

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
  public void differentContexts() throws Exception {

    ContextManager cm = new ContextManager(vfs, ClassLoader::getSystemClassLoader);

    cm.setContextConfig(context -> {
      if (context.equals("CX1")) {
        return new ContextConfig(uri1, true);
      } else if (context.equals("CX2")) {
        return new ContextConfig(uri2, true);
      }
      return null;
    });

    FileObject testDir = vfs.resolveFile(folder1.toURI().toString());
    FileObject[] dirContents = testDir.getChildren();
    ClassLoader cl1 = cm.getClassLoader("CX1");
    FileObject[] files = ((VFSClassLoader) cl1).getFileObjects();
    assertArrayEquals(createFileSystems(dirContents), files);

    FileObject testDir2 = vfs.resolveFile(folder2.toURI().toString());
    FileObject[] dirContents2 = testDir2.getChildren();
    ClassLoader cl2 = cm.getClassLoader("CX2");
    FileObject[] files2 = ((VFSClassLoader) cl2).getFileObjects();
    assertArrayEquals(createFileSystems(dirContents2), files2);

    Class<?> defaultContextClass = cl1.loadClass("test.HelloWorld");
    Object o1 = defaultContextClass.getDeclaredConstructor().newInstance();
    assertEquals("Hello World!", o1.toString());

    Class<?> myContextClass = cl2.loadClass("test.HelloWorld");
    Object o2 = myContextClass.getDeclaredConstructor().newInstance();
    assertEquals("Hello World!", o2.toString());

    assertNotEquals(defaultContextClass, myContextClass);

    cm.removeUnusedContexts(new HashSet<>());
  }

  @Test
  public void testPostDelegation() throws Exception {
    final VFSClassLoader parent = new VFSClassLoader(new FileObject[] {vfs.resolveFile(uri1)}, vfs);

    Class<?> pclass = parent.loadClass("test.HelloWorld");

    ContextManager cm = new ContextManager(vfs, () -> parent);

    cm.setContextConfig(context -> {
      if (context.equals("CX1")) {
        return new ContextConfig(uri2.toString(), true);
      } else if (context.equals("CX2")) {
        return new ContextConfig(uri2.toString(), false);
      }
      return null;
    });

    assertSame(cm.getClassLoader("CX1").loadClass("test.HelloWorld"), pclass);
    assertNotSame(cm.getClassLoader("CX2").loadClass("test.HelloWorld"), pclass);
  }

}
