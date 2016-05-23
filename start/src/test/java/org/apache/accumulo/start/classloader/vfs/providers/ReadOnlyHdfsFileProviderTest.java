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
package org.apache.accumulo.start.classloader.vfs.providers;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.test.AccumuloDFSBase;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.util.RandomAccessMode;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ReadOnlyHdfsFileProviderTest extends AccumuloDFSBase {

  private static final String TEST_DIR1 = getHdfsUri() + "/test-dir";
  private static final Path DIR1_PATH = new Path("/test-dir");
  private static final String TEST_FILE1 = TEST_DIR1 + "/accumulo-test-1.jar";
  private static final Path FILE1_PATH = new Path(DIR1_PATH, "accumulo-test-1.jar");

  private DefaultFileSystemManager manager = null;
  private FileSystem hdfs = null;

  @Before
  public void setup() throws Exception {
    manager = new DefaultFileSystemManager();
    manager.addProvider("hdfs", new HdfsFileProvider());
    manager.init();
    this.hdfs = cluster.getFileSystem();
  }

  private FileObject createTestFile(FileSystem hdfs) throws IOException {
    // Create the directory
    hdfs.mkdirs(DIR1_PATH);
    FileObject dir = manager.resolveFile(TEST_DIR1);
    Assert.assertNotNull(dir);
    Assert.assertTrue(dir.exists());
    Assert.assertTrue(dir.getType().equals(FileType.FOLDER));

    // Create the file in the directory
    hdfs.create(FILE1_PATH).close();
    FileObject f = manager.resolveFile(TEST_FILE1);
    Assert.assertNotNull(f);
    Assert.assertTrue(f.exists());
    Assert.assertTrue(f.getType().equals(FileType.FILE));
    return f;
  }

  @Test
  public void testInit() throws Exception {
    FileObject fo = manager.resolveFile(TEST_FILE1);
    Assert.assertNotNull(fo);
  }

  @Test
  public void testExistsFails() throws Exception {
    FileObject fo = manager.resolveFile(TEST_FILE1);
    Assert.assertNotNull(fo);
    Assert.assertFalse(fo.exists());
  }

  @Test
  public void testExistsSucceeds() throws Exception {
    FileObject fo = manager.resolveFile(TEST_FILE1);
    Assert.assertNotNull(fo);
    Assert.assertFalse(fo.exists());

    // Create the file
    @SuppressWarnings("unused")
    FileObject f = createTestFile(hdfs);

  }

  @Test(expected = UnsupportedOperationException.class)
  public void testCanRenameTo() throws Exception {
    FileObject fo = createTestFile(this.hdfs);
    Assert.assertNotNull(fo);
    fo.canRenameTo(fo);
  }

  @Test
  public void testDoListChildren() throws Exception {
    FileObject fo = manager.resolveFile(TEST_DIR1);
    Assert.assertNotNull(fo);
    Assert.assertFalse(fo.exists());

    // Create the test file
    FileObject file = createTestFile(hdfs);
    FileObject dir = file.getParent();

    FileObject[] children = dir.getChildren();
    Assert.assertTrue(children.length == 1);
    Assert.assertTrue(children[0].getName().equals(file.getName()));

  }

  @Test
  public void testGetContentSize() throws Exception {
    FileObject fo = manager.resolveFile(TEST_DIR1);
    Assert.assertNotNull(fo);
    Assert.assertFalse(fo.exists());

    // Create the test file
    FileObject file = createTestFile(hdfs);
    Assert.assertEquals(0, file.getContent().getSize());
  }

  @Test
  public void testGetInputStream() throws Exception {
    FileObject fo = manager.resolveFile(TEST_DIR1);
    Assert.assertNotNull(fo);
    Assert.assertFalse(fo.exists());

    // Create the test file
    FileObject file = createTestFile(hdfs);
    file.getContent().getInputStream().close();
  }

  @Test
  public void testIsHidden() throws Exception {
    FileObject fo = manager.resolveFile(TEST_DIR1);
    Assert.assertNotNull(fo);
    Assert.assertFalse(fo.exists());

    // Create the test file
    FileObject file = createTestFile(hdfs);
    Assert.assertFalse(file.isHidden());
  }

  @Test
  public void testIsReadable() throws Exception {
    FileObject fo = manager.resolveFile(TEST_DIR1);
    Assert.assertNotNull(fo);
    Assert.assertFalse(fo.exists());

    // Create the test file
    FileObject file = createTestFile(hdfs);
    Assert.assertTrue(file.isReadable());
  }

  @Test
  public void testIsWritable() throws Exception {
    FileObject fo = manager.resolveFile(TEST_DIR1);
    Assert.assertNotNull(fo);
    Assert.assertFalse(fo.exists());

    // Create the test file
    FileObject file = createTestFile(hdfs);
    Assert.assertFalse(file.isWriteable());
  }

  @Test
  public void testLastModificationTime() throws Exception {
    FileObject fo = manager.resolveFile(TEST_DIR1);
    Assert.assertNotNull(fo);
    Assert.assertFalse(fo.exists());

    // Create the test file
    FileObject file = createTestFile(hdfs);
    Assert.assertFalse(-1 == file.getContent().getLastModifiedTime());
  }

  @Test
  public void testGetAttributes() throws Exception {
    FileObject fo = manager.resolveFile(TEST_DIR1);
    Assert.assertNotNull(fo);
    Assert.assertFalse(fo.exists());

    // Create the test file
    FileObject file = createTestFile(hdfs);
    Map<String,Object> attributes = file.getContent().getAttributes();
    Assert.assertTrue(attributes.containsKey(HdfsFileAttributes.BLOCK_SIZE.toString()));
    Assert.assertTrue(attributes.containsKey(HdfsFileAttributes.GROUP.toString()));
    Assert.assertTrue(attributes.containsKey(HdfsFileAttributes.LAST_ACCESS_TIME.toString()));
    Assert.assertTrue(attributes.containsKey(HdfsFileAttributes.LENGTH.toString()));
    Assert.assertTrue(attributes.containsKey(HdfsFileAttributes.MODIFICATION_TIME.toString()));
    Assert.assertTrue(attributes.containsKey(HdfsFileAttributes.OWNER.toString()));
    Assert.assertTrue(attributes.containsKey(HdfsFileAttributes.PERMISSIONS.toString()));
  }

  @Test(expected = FileSystemException.class)
  public void testRandomAccessContent() throws Exception {
    FileObject fo = manager.resolveFile(TEST_DIR1);
    Assert.assertNotNull(fo);
    Assert.assertFalse(fo.exists());

    // Create the test file
    FileObject file = createTestFile(hdfs);
    file.getContent().getRandomAccessContent(RandomAccessMode.READWRITE).close();
  }

  @Test
  public void testRandomAccessContent2() throws Exception {
    FileObject fo = manager.resolveFile(TEST_DIR1);
    Assert.assertNotNull(fo);
    Assert.assertFalse(fo.exists());

    // Create the test file
    FileObject file = createTestFile(hdfs);
    file.getContent().getRandomAccessContent(RandomAccessMode.READ).close();
  }

  @Test
  public void testEquals() throws Exception {
    FileObject fo = manager.resolveFile(TEST_DIR1);
    Assert.assertNotNull(fo);
    Assert.assertFalse(fo.exists());

    // Create the test file
    FileObject file = createTestFile(hdfs);
    // Get a handle to the same file
    FileObject file2 = manager.resolveFile(TEST_FILE1);
    Assert.assertEquals(file, file2);
  }

  @After
  public void tearDown() throws Exception {
    if (null != hdfs) {
      hdfs.delete(DIR1_PATH, true);
      hdfs.close();
    }
    manager.close();
  }

}
