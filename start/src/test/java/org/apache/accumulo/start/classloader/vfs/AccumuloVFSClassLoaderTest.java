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
package org.apache.accumulo.start.classloader.vfs;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.net.URLClassLoader;

import org.apache.accumulo.start.classloader.AccumuloClassLoader;
import org.apache.commons.io.FileUtils;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.impl.VFSClassLoader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@Deprecated
@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths not set by user input")
@RunWith(PowerMockRunner.class)
@PrepareForTest(AccumuloVFSClassLoader.class)
@SuppressStaticInitializationFor({"org.apache.accumulo.start.classloader.AccumuloVFSClassLoader",
    "org.apache.log4j.LogManager"})
@PowerMockIgnore({"org.apache.log4j.*", "org.apache.hadoop.log.metrics",
    "org.apache.commons.logging.*", "org.xml.*", "javax.xml.*", "org.w3c.dom.*",
    "org.apache.hadoop.*", "com.sun.org.apache.xerces.*"})
public class AccumuloVFSClassLoaderTest {

  @Rule
  public TemporaryFolder folder1 =
      new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));

  /*
   * Test that the default (empty dynamic class paths) does not create the 2nd level loader
   */
  @Test
  public void testDefaultConfig() throws Exception {

    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "loader",
        (AccumuloReloadingVFSClassLoader) null);

    File conf = folder1.newFile("accumulo.properties");
    FileWriter out = new FileWriter(conf, UTF_8);
    out.append("general.classpaths=\n");
    out.append("general.vfs.classpaths=\n");
    out.close();

    Whitebox.setInternalState(AccumuloClassLoader.class, "accumuloConfigUrl", conf.toURI().toURL());
    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "lock", new Object());
    ClassLoader acl = AccumuloVFSClassLoader.getClassLoader();
    assertTrue((acl instanceof URLClassLoader));
    // We can't check to see if the parent is an instance of BuiltinClassLoader
    // Let's assert it's not something we now about
    assertFalse((acl.getParent() instanceof VFSClassLoader));
    assertFalse((acl.getParent() instanceof URLClassLoader));
  }

  /*
   * Test that if configured with dynamic class paths, that the code creates the 2nd level loader
   */
  @Test
  public void testDynamicConfig() throws Exception {

    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "loader",
        (AccumuloReloadingVFSClassLoader) null);

    File conf = folder1.newFile("accumulo.properties");
    FileWriter out = new FileWriter(conf, UTF_8);
    out.append("general.classpaths=\n");
    out.append("general.vfs.classpaths=\n");
    out.append("general.dynamic.classpaths=" + System.getProperty("user.dir") + "\n");
    out.close();

    Whitebox.setInternalState(AccumuloClassLoader.class, "accumuloConfigUrl", conf.toURI().toURL());
    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "lock", new Object());
    ClassLoader acl = AccumuloVFSClassLoader.getClassLoader();
    assertTrue((acl instanceof VFSClassLoader));
    assertTrue((acl.getParent() instanceof URLClassLoader));
  }

  /*
   * Test with default context configured
   */
  @Test
  public void testDefaultContextConfigured() throws Exception {

    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "loader",
        (AccumuloReloadingVFSClassLoader) null);

    // Copy jar file to TEST_DIR
    FileUtils.copyURLToFile(this.getClass().getResource("/HelloWorld.jar"),
        folder1.newFile("HelloWorld.jar"));

    File conf = folder1.newFile("accumulo.properties");
    FileWriter out = new FileWriter(conf, UTF_8);
    out.append("general.classpaths=\n");
    out.append(
        "general.vfs.classpaths=" + new File(folder1.getRoot(), "HelloWorld.jar").toURI() + "\n");
    out.append("general.dynamic.classpaths=" + System.getProperty("user.dir") + "\n");
    out.close();

    Whitebox.setInternalState(AccumuloClassLoader.class, "accumuloConfigUrl", conf.toURI().toURL());
    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "lock", new Object());
    ClassLoader acl = AccumuloVFSClassLoader.getClassLoader();
    assertTrue((acl instanceof VFSClassLoader));
    assertTrue((acl.getParent() instanceof VFSClassLoader));
    VFSClassLoader arvcl = (VFSClassLoader) acl.getParent();
    assertEquals(1, arvcl.getFileObjects().length);
    // We can't be sure what the authority/host will be due to FQDN mappings, so just check the path
    assertTrue(arvcl.getFileObjects()[0].getURL().toString().contains("HelloWorld.jar"));
    Class<?> clazz1 = arvcl.loadClass("test.HelloWorld");
    Object o1 = clazz1.getDeclaredConstructor().newInstance();
    assertEquals("Hello World!", o1.toString());
    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "loader",
        (AccumuloReloadingVFSClassLoader) null);
  }

  @Test
  public void testDefaultCacheDirectory() throws Exception {

    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "loader",
        (AccumuloReloadingVFSClassLoader) null);

    File conf = folder1.newFile("accumulo.properties");
    FileWriter out = new FileWriter(conf, UTF_8);
    out.append("general.classpaths=\n");
    out.append("general.vfs.classpaths=\n");
    out.close();

    Whitebox.setInternalState(AccumuloClassLoader.class, "accumuloConfigUrl", conf.toURI().toURL());
    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "lock", new Object());
    AccumuloVFSClassLoader.getClassLoader();
    FileSystemManager manager = AccumuloVFSClassLoader.generateVfs();
    UniqueFileReplicator replicator = Whitebox.getInternalState(manager, "fileReplicator");
    File tempDir = Whitebox.getInternalState(replicator, "tempDir");
    String tempDirParent = tempDir.getParent();
    String tempDirName = tempDir.getName();
    String javaIoTmpDir = System.getProperty("java.io.tmpdir");

    // trim off any final separator, because java.io.File does the same.
    if (javaIoTmpDir.endsWith(File.separator)) {
      javaIoTmpDir = javaIoTmpDir.substring(0, javaIoTmpDir.length() - File.separator.length());
    }

    assertEquals(javaIoTmpDir, tempDirParent);
    assertTrue(tempDirName.startsWith("accumulo-vfs-cache-"));
    assertTrue(tempDirName.endsWith(System.getProperty("user.name", "nouser")));

    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "loader",
        (AccumuloReloadingVFSClassLoader) null);
  }

  @Test
  public void testCacheDirectoryConfigured() throws Exception {

    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "loader",
        (AccumuloReloadingVFSClassLoader) null);
    String cacheDir = "/some/random/cache/dir";

    File conf = folder1.newFile("accumulo.properties");
    FileWriter out = new FileWriter(conf, UTF_8);
    out.append("general.classpaths=\n");
    out.append(AccumuloVFSClassLoader.VFS_CACHE_DIR + "=" + cacheDir + "\n");
    out.close();

    Whitebox.setInternalState(AccumuloClassLoader.class, "accumuloConfigUrl", conf.toURI().toURL());
    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "lock", new Object());
    AccumuloVFSClassLoader.getClassLoader();
    FileSystemManager manager = AccumuloVFSClassLoader.generateVfs();
    UniqueFileReplicator replicator = Whitebox.getInternalState(manager, "fileReplicator");
    File tempDir = Whitebox.getInternalState(replicator, "tempDir");
    String tempDirParent = tempDir.getParent();
    String tempDirName = tempDir.getName();
    assertEquals(cacheDir, tempDirParent);
    assertTrue(tempDirName.startsWith("accumulo-vfs-cache-"));
    assertTrue(tempDirName.endsWith(System.getProperty("user.name", "nouser")));

    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "loader",
        (AccumuloReloadingVFSClassLoader) null);
  }
}
