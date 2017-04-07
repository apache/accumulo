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

import java.io.IOException;

/**
 *
 * Provides a generic interface for a Reader for a BlockBaseFile format. Supports the minimal interface required.
 *
 * Read a metaBlock and a dataBlock
 *
 */

public interface BlockFileReader {

  ABlockReader getMetaBlock(String name) throws IOException;

  ABlockReader getDataBlock(int blockIndex) throws IOException;

  void close() throws IOException;

  ABlockReader getMetaBlock(long offset, long compressedSize, long rawSize) throws IOException;

  ABlockReader getDataBlock(long offset, long compressedSize, long rawSize) throws IOException;

}
