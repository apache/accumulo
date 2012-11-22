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
package org.apache.commons.vfs2.provider;

import java.io.IOException;
import java.util.Collection;

import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class ReadOnlyHdfsFileSystem extends AbstractFileSystem {
  
  private static final Logger log = Logger.getLogger(ReadOnlyHdfsFileSystem.class);

  private FileSystem fs = null;
  
  protected ReadOnlyHdfsFileSystem(FileName rootName, FileSystemOptions fileSystemOptions) {
    super(rootName, null, fileSystemOptions);
  }

  @Override
  public void close() {
    try {
      if (null != fs)
        fs.close();
    } catch (IOException e) {
      throw new RuntimeException("Error closing HDFS client", e);
    }
    super.close();
  }

  @Override
  protected FileObject createFile(AbstractFileName name) throws Exception {
    throw new FileSystemException("Operation not supported");
  }

  @Override
  protected void addCapabilities(Collection<Capability> capabilities) {
    capabilities.addAll(ReadOnlyHdfsFileProvider.capabilities);
  }

  @Override
  public FileObject resolveFile(FileName name) throws FileSystemException {
    
    synchronized (this) {
      if (null == this.fs) {
        String hdfsUri = name.getRootURI();
        Configuration conf = new Configuration(true);
        conf.set(org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY, hdfsUri);
        this.fs = null;
        try {
          fs = org.apache.hadoop.fs.FileSystem.get(conf);
          
        } catch (IOException e) {
          log.error("Error connecting to filesystem " + hdfsUri, e);
          throw new FileSystemException("Error connecting to filesystem " + hdfsUri, e);
        }
      }
    }

    Path filePath = new Path(name.getPath());
    return new HdfsFileObject((AbstractFileName) name, this, fs, filePath);
    
  }
  
}
