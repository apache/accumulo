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
package org.apache.accumulo.core.file.blockfile;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

/*
 * Minimal interface to read a block from a
 * block based file
 *
 */

public interface ABlockReader extends DataInput {

  long getRawSize();

  DataInputStream getStream() throws IOException;

  void close() throws IOException;

  /**
   * An indexable block supports seeking, getting a position, and associating an arbitrary index with the block
   *
   * @return true, if the block is indexable; otherwise false.
   */
  boolean isIndexable();

  void seek(int position);

  /**
   * Get the file position.
   *
   * @return the file position.
   */
  int getPosition();

  <T> T getIndex(Class<T> clazz);
}
