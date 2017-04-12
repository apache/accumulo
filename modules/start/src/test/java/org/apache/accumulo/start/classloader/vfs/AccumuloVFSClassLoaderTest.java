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
import java.io.FileWriter;
import java.io.IOException;
import java.net.URLClassLoader;

import org.apache.accumulo.start.classloader.AccumuloClassLoader;
import org.apache.commons.io.FileUtils;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.impl.VFSClassLoader;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

@RunWith(PowerMockRunner.class)
@PrepareForTest(AccumuloVFSClassLoader.class)
@SuppressStaticInitializationFor({"org.apache.accumulo.start.classloader.AccumuloVFSClassLoader", "org.apache.log4j.LogManager"})
@PowerMockIgnore({"org.apache.log4j.*", "org.apache.hadoop.log.metrics", "org.apache.commons.logging.*", "org.xml.*", "javax.xml.*", "org.w3c.dom.*",
    "org.apache.hadoop.*"})
public class AccumuloVFSClassLoaderTest {

  private TemporaryFolder folder1 = new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));

  @Before
  public void setup() throws IOException {
    folder1.create();
  }

  @After
  public void tearDown() {
    folder1.delete();
  }

  /*
   * Test that if enabled, but not configured, that the code creates the 2nd level classloader
   */
  @Test
  public void testDefaultConfig() throws Exception {

    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "loader", (AccumuloReloadingVFSClassLoader) null);

    File conf = folder1.newFile("accumulo-site.xml");
    FileWriter out = new FileWriter(conf);
    out.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    out.append("<configuration>\n");
    out.append("<property>\n");
    out.append("<name>general.classpaths</name>\n");
    out.append("<value></value>\n");
    out.append("</property>\n");
    out.append("<property>\n");
    out.append("<name>general.vfs.classpaths</name>\n");
    out.append("<value></value>\n");
    out.append("</property>\n");
    out.append("</configuration>\n");
    out.close();

    Whitebox.setInternalState(AccumuloClassLoader.class, "accumuloConfigUrl", conf.toURI().toURL());
    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "lock", new Object());
    ClassLoader acl = AccumuloVFSClassLoader.getClassLoader();
    Assert.assertTrue((acl instanceof VFSClassLoader));
    Assert.assertTrue((acl.getParent() instanceof URLClassLoader));
  }

  /*
   * Test with default context configured
   */
  @Test
  public void testDefaultContextConfigured() throws Exception {

    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "loader", (AccumuloReloadingVFSClassLoader) null);

    // Copy jar file to TEST_DIR
    FileUtils.copyURLToFile(this.getClass().getResource("/HelloWorld.jar"), folder1.newFile("HelloWorld.jar"));

    File conf = folder1.newFile("accumulo-site.xml");
    FileWriter out = new FileWriter(conf);
    out.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    out.append("<configuration>\n");
    out.append("<property>\n");
    out.append("<name>general.classpaths</name>\n");
    out.append("<value></value>\n");
    out.append("</property>\n");
    out.append("<property>\n");
    out.append("<name>general.vfs.classpaths</name>\n");
    out.append("<value>" + new File(folder1.getRoot(), "HelloWorld.jar").toURI() + "</value>\n");
    out.append("</property>\n");
    out.append("</configuration>\n");
    out.close();

    Whitebox.setInternalState(AccumuloClassLoader.class, "accumuloConfigUrl", conf.toURI().toURL());
    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "lock", new Object());
    ClassLoader acl = AccumuloVFSClassLoader.getClassLoader();
    Assert.assertTrue((acl instanceof VFSClassLoader));
    Assert.assertTrue((acl.getParent() instanceof VFSClassLoader));
    VFSClassLoader arvcl = (VFSClassLoader) acl.getParent();
    Assert.assertEquals(1, arvcl.getFileObjects().length);
    // We can't be sure what the authority/host will be due to FQDN mappings, so just check the path
    Assert.assertTrue(arvcl.getFileObjects()[0].getURL().toString().contains("HelloWorld.jar"));
    Class<?> clazz1 = arvcl.loadClass("test.HelloWorld");
    Object o1 = clazz1.newInstance();
    Assert.assertEquals("Hello World!", o1.toString());
    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "loader", (AccumuloReloadingVFSClassLoader) null);
  }

  @Test
  public void testDefaultCacheDirectory() throws Exception {

    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "loader", (AccumuloReloadingVFSClassLoader) null);

    File conf = folder1.newFile("accumulo-site.xml");
    FileWriter out = new FileWriter(conf);
    out.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    out.append("<configuration>\n");
    out.append("<property>\n");
    out.append("<name>general.classpaths</name>\n");
    out.append("<value></value>\n");
    out.append("</property>\n");
    out.append("<property>\n");
    out.append("<name>general.vfs.classpaths</name>\n");
    out.append("<value></value>\n");
    out.append("</property>\n");
    out.append("</configuration>\n");
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

    Assert.assertTrue(javaIoTmpDir.equals(tempDirParent));
    Assert.assertTrue(tempDirName.startsWith("accumulo-vfs-cache-"));
    Assert.assertTrue(tempDirName.endsWith(System.getProperty("user.name", "nouser")));

    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "loader", (AccumuloReloadingVFSClassLoader) null);
  }

  @Test
  public void testCacheDirectoryConfigured() throws Exception {

    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "loader", (AccumuloReloadingVFSClassLoader) null);
    String cacheDir = "/some/random/cache/dir";

    File conf = folder1.newFile("accumulo-site.xml");
    FileWriter out = new FileWriter(conf);
    out.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    out.append("<configuration>\n");
    out.append("<property>\n");
    out.append("<name>general.classpaths</name>\n");
    out.append("<value></value>\n");
    out.append("</property>\n");
    out.append("<property>\n");
    out.append("<name>" + AccumuloVFSClassLoader.VFS_CACHE_DIR + "</name>\n");
    out.append("<value>" + cacheDir + "</value>\n");
    out.append("</property>\n");
    out.append("</configuration>\n");
    out.close();

    Whitebox.setInternalState(AccumuloClassLoader.class, "accumuloConfigUrl", conf.toURI().toURL());
    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "lock", new Object());
    AccumuloVFSClassLoader.getClassLoader();
    FileSystemManager manager = AccumuloVFSClassLoader.generateVfs();
    UniqueFileReplicator replicator = Whitebox.getInternalState(manager, "fileReplicator");
    File tempDir = Whitebox.getInternalState(replicator, "tempDir");
    String tempDirParent = tempDir.getParent();
    String tempDirName = tempDir.getName();
    Assert.assertTrue(cacheDir.equals(tempDirParent));
    Assert.assertTrue(tempDirName.startsWith("accumulo-vfs-cache-"));
    Assert.assertTrue(tempDirName.endsWith(System.getProperty("user.name", "nouser")));

    Whitebox.setInternalState(AccumuloVFSClassLoader.class, "loader", (AccumuloReloadingVFSClassLoader) null);
  }
}
