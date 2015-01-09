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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.CacheStrategy;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * A VFS FileSystem that interacts with HDFS.
 *
 * @since 2.1
 */
public class HdfsFileSystem extends AbstractFileSystem {
  private static final Log log = LogFactory.getLog(HdfsFileSystem.class);

  private FileSystem fs;

  protected HdfsFileSystem(final FileName rootName, final FileSystemOptions fileSystemOptions) {
    super(rootName, null, fileSystemOptions);
  }

  /**
   * @see org.apache.commons.vfs2.provider.AbstractFileSystem#addCapabilities(java.util.Collection)
   */
  @Override
  protected void addCapabilities(final Collection<Capability> capabilities) {
    capabilities.addAll(HdfsFileProvider.CAPABILITIES);
  }

  /**
   * @see org.apache.commons.vfs2.provider.AbstractFileSystem#close()
   */
  @Override
  synchronized public void close() {
    try {
      if (null != fs) {
        fs.close();
      }
    } catch (final IOException e) {
      throw new RuntimeException("Error closing HDFS client", e);
    }
    super.close();
  }

  /**
   * @see org.apache.commons.vfs2.provider.AbstractFileSystem#createFile(org.apache.commons.vfs2.provider.AbstractFileName)
   */
  @Override
  protected FileObject createFile(final AbstractFileName name) throws Exception {
    throw new FileSystemException("Operation not supported");
  }

  /**
   * @see org.apache.commons.vfs2.provider.AbstractFileSystem#resolveFile(org.apache.commons.vfs2.FileName)
   */
  @Override
  public FileObject resolveFile(final FileName name) throws FileSystemException {

    synchronized (this) {
      if (null == this.fs) {
        final String hdfsUri = name.getRootURI();
        final Configuration conf = new Configuration(true);
        conf.set(org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY, hdfsUri);
        this.fs = null;
        try {
          fs = org.apache.hadoop.fs.FileSystem.get(conf);
        } catch (final IOException e) {
          log.error("Error connecting to filesystem " + hdfsUri, e);
          throw new FileSystemException("Error connecting to filesystem " + hdfsUri, e);
        }
      }
    }

    boolean useCache = (null != getContext().getFileSystemManager().getFilesCache());
    FileObject file;
    if (useCache) {
      file = this.getFileFromCache(name);
    } else {
      file = null;
    }
    if (null == file) {
      String path = null;
      try {
        path = URLDecoder.decode(name.getPath(), "UTF-8");
      } catch (final UnsupportedEncodingException e) {
        path = name.getPath();
      }
      final Path filePath = new Path(path);
      file = new HdfsFileObject((AbstractFileName) name, this, fs, filePath);
      if (useCache) {
        this.putFileToCache(file);
      }

    }

    /**
     * resync the file information if requested
     */
    if (getFileSystemManager().getCacheStrategy().equals(CacheStrategy.ON_RESOLVE)) {
      file.refresh();
    }

    return file;
  }

}
