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
import java.net.URI;
import java.util.HashSet;

import org.apache.accumulo.start.classloader.vfs.ContextManager.ContextConfig;
import org.apache.accumulo.start.classloader.vfs.ContextManager.ContextsConfig;
import org.apache.accumulo.start.classloader.vfs.providers.HdfsFileProvider;
import org.apache.commons.io.FileUtils;
import org.apache.commons.vfs2.CacheStrategy;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.cache.DefaultFilesCache;
import org.apache.commons.vfs2.cache.SoftRefFilesCache;
import org.apache.commons.vfs2.impl.DefaultFileReplicator;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.impl.FileContentInfoFilenameFactory;
import org.apache.commons.vfs2.impl.VFSClassLoader;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ContextManagerTest {
  
  private TemporaryFolder folder1 = new TemporaryFolder();
  private TemporaryFolder folder2 = new TemporaryFolder();
  private DefaultFileSystemManager vfs;
  private URI uri1;
  private URI uri2;
  
  static DefaultFileSystemManager getVFS() {
    DefaultFileSystemManager vfs = new DefaultFileSystemManager();
    try {
      vfs.setFilesCache(new DefaultFilesCache());
      vfs.addProvider("res", new org.apache.commons.vfs2.provider.res.ResourceFileProvider());
      vfs.addProvider("zip", new org.apache.commons.vfs2.provider.zip.ZipFileProvider());
      vfs.addProvider("gz", new org.apache.commons.vfs2.provider.gzip.GzipFileProvider());
      vfs.addProvider("ram", new org.apache.commons.vfs2.provider.ram.RamFileProvider());
      vfs.addProvider("file", new org.apache.commons.vfs2.provider.local.DefaultLocalFileProvider());
      vfs.addProvider("jar", new org.apache.commons.vfs2.provider.jar.JarFileProvider());
      vfs.addProvider("http", new org.apache.commons.vfs2.provider.http.HttpFileProvider());
      vfs.addProvider("https", new org.apache.commons.vfs2.provider.https.HttpsFileProvider());
      vfs.addProvider("ftp", new org.apache.commons.vfs2.provider.ftp.FtpFileProvider());
      vfs.addProvider("ftps", new org.apache.commons.vfs2.provider.ftps.FtpsFileProvider());
      vfs.addProvider("war", new org.apache.commons.vfs2.provider.jar.JarFileProvider());
      vfs.addProvider("par", new org.apache.commons.vfs2.provider.jar.JarFileProvider());
      vfs.addProvider("ear", new org.apache.commons.vfs2.provider.jar.JarFileProvider());
      vfs.addProvider("sar", new org.apache.commons.vfs2.provider.jar.JarFileProvider());
      vfs.addProvider("ejb3", new org.apache.commons.vfs2.provider.jar.JarFileProvider());
      vfs.addProvider("tmp", new org.apache.commons.vfs2.provider.temp.TemporaryFileProvider());
      vfs.addProvider("tar", new org.apache.commons.vfs2.provider.tar.TarFileProvider());
      vfs.addProvider("tbz2", new org.apache.commons.vfs2.provider.tar.TarFileProvider());
      vfs.addProvider("tgz", new org.apache.commons.vfs2.provider.tar.TarFileProvider());
      vfs.addProvider("bz2", new org.apache.commons.vfs2.provider.bzip2.Bzip2FileProvider());
      vfs.addProvider("hdfs", new HdfsFileProvider());
      vfs.addExtensionMap("jar", "jar");
      vfs.addExtensionMap("zip", "zip");
      vfs.addExtensionMap("gz", "gz");
      vfs.addExtensionMap("tar", "tar");
      vfs.addExtensionMap("tbz2", "tar");
      vfs.addExtensionMap("tgz", "tar");
      vfs.addExtensionMap("bz2", "bz2");
      vfs.addMimeTypeMap("application/x-tar", "tar");
      vfs.addMimeTypeMap("application/x-gzip", "gz");
      vfs.addMimeTypeMap("application/zip", "zip");
      vfs.setFileContentInfoFactory(new FileContentInfoFilenameFactory());
      vfs.setFilesCache(new SoftRefFilesCache());
      vfs.setReplicator(new DefaultFileReplicator());
      vfs.setCacheStrategy(CacheStrategy.ON_RESOLVE);
      vfs.init();
    } catch (FileSystemException e) {
      throw new RuntimeException("Error setting up VFS", e);
    }
    
    return vfs;
  }

  @Before
  public void setup() throws Exception {
    
    vfs = getVFS();

    folder1.create();
    folder2.create();
    
    FileUtils.copyURLToFile(this.getClass().getResource("/HelloWorld.jar"), folder1.newFile("HelloWorld.jar"));
    FileUtils.copyURLToFile(this.getClass().getResource("/HelloWorld.jar"), folder2.newFile("HelloWorld.jar"));
    
    uri1 = new File(folder1.getRoot(), "HelloWorld.jar").toURI();
    uri2 = folder2.getRoot().toURI();

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
    
    ContextManager cm = new ContextManager(vfs, new ReloadingClassLoader() {
      @Override
      public ClassLoader getClassLoader() {
        return ClassLoader.getSystemClassLoader();
      }
    });

    cm.setContextConfig(new ContextsConfig() {
      @Override
      public ContextConfig getContextConfig(String context) {
        if (context.equals("CX1")) {
          return new ContextConfig(uri1.toString(), true);
        } else if (context.equals("CX2")) {
          return new ContextConfig(uri2.toString(), true);
        }
        return null;
      }
    });

    FileObject testDir = vfs.resolveFile(folder1.getRoot().toURI().toString());
    FileObject[] dirContents = testDir.getChildren();
    ClassLoader cl1 = cm.getClassLoader("CX1");
    FileObject[] files = ((VFSClassLoader) cl1).getFileObjects();
    Assert.assertArrayEquals(createFileSystems(dirContents), files);

    FileObject testDir2 = vfs.resolveFile(folder2.getRoot().toURI().toString());
    FileObject[] dirContents2 = testDir2.getChildren();
    ClassLoader cl2 = cm.getClassLoader("CX2");
    FileObject[] files2 = ((VFSClassLoader) cl2).getFileObjects();
    Assert.assertArrayEquals(createFileSystems(dirContents2), files2);
    
    Class<?> defaultContextClass = cl1.loadClass("test.HelloWorld");
    Object o1 = defaultContextClass.newInstance();
    Assert.assertEquals("Hello World!", o1.toString());

    Class<?> myContextClass = cl2.loadClass("test.HelloWorld");
    Object o2 = myContextClass.newInstance();
    Assert.assertEquals("Hello World!", o2.toString());
    
    Assert.assertFalse(defaultContextClass.equals(myContextClass));

    cm.removeUnusedContexts(new HashSet<String>());
  }
  
  @Test
  public void testPostDelegation() throws Exception {
    final VFSClassLoader parent = new VFSClassLoader(new FileObject[] {vfs.resolveFile(uri1.toString())}, vfs);
    
    Class<?> pclass = parent.loadClass("test.HelloWorld");
    
    ContextManager cm = new ContextManager(vfs, new ReloadingClassLoader() {
      @Override
      public ClassLoader getClassLoader() {
        return parent;
      }
    });
    
    cm.setContextConfig(new ContextsConfig() {
      @Override
      public ContextConfig getContextConfig(String context) {
        if (context.equals("CX1")) {
          return new ContextConfig(uri2.toString(), true);
        } else if (context.equals("CX2")) {
          return new ContextConfig(uri2.toString(), false);
        }
        return null;
      }
    });
    
    Assert.assertTrue(cm.getClassLoader("CX1").loadClass("test.HelloWorld") == pclass);
    Assert.assertFalse(cm.getClassLoader("CX2").loadClass("test.HelloWorld") == pclass);
  }

  @After
  public void tearDown() throws Exception {
    folder1.delete();
    folder2.delete();
  }

}
