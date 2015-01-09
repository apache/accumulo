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

import org.apache.commons.vfs2.FileContent;
import org.apache.commons.vfs2.FileContentInfo;
import org.apache.commons.vfs2.FileContentInfoFactory;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.impl.DefaultFileContentInfo;

/**
 * Creates FileContentInfo instances for HDFS.
 *
 * @since 2.1
 */
public class HdfsFileContentInfoFactory implements FileContentInfoFactory {
  private static final String CONTENT = "text/plain";
  private static final String ENCODING = "UTF-8";

  /**
   * Creates a FileContentInfo for a the given FileContent.
   *
   * @param fileContent
   *          Use this FileContent to create a matching FileContentInfo
   * @return a FileContentInfo for the given FileContent with content set to "text/plain" and encoding set to "UTF-8"
   * @throws FileSystemException
   *           when a problem occurs creating the FileContentInfo.
   */
  @Override
  public FileContentInfo create(final FileContent fileContent) throws FileSystemException {
    return new DefaultFileContentInfo(CONTENT, ENCODING);
  }

}
