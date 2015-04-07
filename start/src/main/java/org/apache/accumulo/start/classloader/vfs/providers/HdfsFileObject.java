/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.start.classloader.vfs.providers;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.vfs2.FileNotFolderException;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.RandomAccessContent;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileObject;
import org.apache.commons.vfs2.util.RandomAccessMode;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * A VFS representation of an HDFS file.
 *
 * @since 2.1
 */
public class HdfsFileObject extends AbstractFileObject {
  private final HdfsFileSystem fs;
  private final FileSystem hdfs;
  private final Path path;
  private FileStatus stat;

  /**
   * Constructs a new HDFS FileObject
   *
   * @param name
   *          FileName
   * @param fs
   *          HdfsFileSystem instance
   * @param hdfs
   *          Hadoop FileSystem instance
   * @param p
   *          Path to the file in HDFS
   */
  protected HdfsFileObject(final AbstractFileName name, final HdfsFileSystem fs, final FileSystem hdfs, final Path p) {
    super(name, fs);
    this.fs = fs;
    this.hdfs = hdfs;
    this.path = p;
  }

  @Override
  public boolean canRenameTo(final FileObject newfile) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void doAttach() throws Exception {
    try {
      this.stat = this.hdfs.getFileStatus(this.path);
    } catch (final FileNotFoundException e) {
      this.stat = null;
      return;
    }
  }

  @Override
  protected Map<String,Object> doGetAttributes() throws Exception {
    if (null == this.stat) {
      return super.doGetAttributes();
    } else {
      final Map<String,Object> attrs = new HashMap<String,Object>();
      attrs.put(HdfsFileAttributes.LAST_ACCESS_TIME.toString(), this.stat.getAccessTime());
      attrs.put(HdfsFileAttributes.BLOCK_SIZE.toString(), this.stat.getBlockSize());
      attrs.put(HdfsFileAttributes.GROUP.toString(), this.stat.getGroup());
      attrs.put(HdfsFileAttributes.OWNER.toString(), this.stat.getOwner());
      attrs.put(HdfsFileAttributes.PERMISSIONS.toString(), this.stat.getPermission().toString());
      attrs.put(HdfsFileAttributes.LENGTH.toString(), this.stat.getLen());
      attrs.put(HdfsFileAttributes.MODIFICATION_TIME.toString(), this.stat.getModificationTime());
      return attrs;
    }
  }

  @Override
  protected long doGetContentSize() throws Exception {
    return stat.getLen();
  }

  @Override
  protected InputStream doGetInputStream() throws Exception {
    return this.hdfs.open(this.path);
  }

  @Override
  protected long doGetLastModifiedTime() throws Exception {
    if (null != this.stat) {
      return this.stat.getModificationTime();
    } else {
      return -1;
    }
  }

  @Override
  protected RandomAccessContent doGetRandomAccessContent(final RandomAccessMode mode) throws Exception {
    if (mode.equals(RandomAccessMode.READWRITE)) {
      throw new UnsupportedOperationException();
    }
    return new HdfsRandomAccessContent(this.path, this.hdfs);
  }

  @Override
  protected FileType doGetType() throws Exception {
    try {
      doAttach();
      if (null == stat) {
        return FileType.IMAGINARY;
      }
      if (stat.isDirectory()) {
        return FileType.FOLDER;
      } else {
        return FileType.FILE;
      }
    } catch (final FileNotFoundException fnfe) {
      return FileType.IMAGINARY;
    }
  }

  @Override
  protected boolean doIsHidden() throws Exception {
    return false;
  }

  @Override
  protected boolean doIsReadable() throws Exception {
    return true;
  }

  @Override
  protected boolean doIsSameFile(final FileObject destFile) throws FileSystemException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected boolean doIsWriteable() throws Exception {
    return false;
  }

  @Override
  protected String[] doListChildren() throws Exception {
    if (this.doGetType() != FileType.FOLDER) {
      throw new FileNotFolderException(this);
    }

    final FileStatus[] files = this.hdfs.listStatus(this.path);
    final String[] children = new String[files.length];
    int i = 0;
    for (final FileStatus status : files) {
      children[i++] = status.getPath().getName();
    }
    return children;
  }

  @Override
  protected FileObject[] doListChildrenResolved() throws Exception {
    if (this.doGetType() != FileType.FOLDER) {
      return null;
    }
    final String[] children = doListChildren();
    final FileObject[] fo = new FileObject[children.length];
    for (int i = 0; i < children.length; i++) {
      final Path p = new Path(this.path, children[i]);
      fo[i] = this.fs.resolveFile(p.toUri().toString());
    }
    return fo;
  }

  @Override
  protected void doRemoveAttribute(final String attrName) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void doSetAttribute(final String attrName, final Object value) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  protected boolean doSetLastModifiedTime(final long modtime) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean equals(final Object o) {
    if (null == o) {
      return false;
    }
    if (o == this) {
      return true;
    }
    if (o instanceof HdfsFileObject) {
      final HdfsFileObject other = (HdfsFileObject) o;
      if (other.path.equals(this.path)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean exists() throws FileSystemException {
    try {
      doAttach();
      return this.stat != null;
    } catch (final FileNotFoundException fne) {
      return false;
    } catch (final Exception e) {
      throw new FileSystemException("Unable to check existance ", e);
    }
  }

  @Override
  public int hashCode() {
    return this.path.getName().toString().hashCode();
  }

}
