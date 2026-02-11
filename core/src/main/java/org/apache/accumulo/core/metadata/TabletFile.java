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
package org.apache.accumulo.core.metadata;

import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.fs.Path;

/**
 * An interface that represents different types of file references that are handled by code that
 * processes tablet files.
 */
public interface TabletFile {

  /**
   * Returns the fileName of the TabletFile. The value return is the name itself and not the entire
   * path. For example, if the full path for a TabletFile is
   * 'hdfs://nn1/accumulo/tables/5a/t-0001/F0002.rf', this method returns 'F0002.rf'.
   */
  String getFileName();

  /**
   * Returns the full path for the TabletFile on the file system. The path may be normalized
   * depending on the specific implementation. For example, a path in hdfs would be returned as
   * 'hdfs://nn1/accumulo/tables/5a/t-0001/F0002.rf'
   */
  Path getPath();

  /**
   * @return The range of the TabletFile
   *
   */
  Range getRange();

  /**
   * @return True if this file is fenced by a range
   *
   */
  boolean hasRange();

  /**
   * @return a string with the filename and row range if there is one.
   */
  String toMinimalString();
}
