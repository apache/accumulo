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
package org.apache.accumulo.core.client.admin.compaction;

import java.net.URI;

import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.metadata.CompactableFileImpl;

/**
 * A single file ready to compact, that will come in a set of possible candidates.
 *
 * @since 2.1.0
 */
public interface CompactableFile {

  public String getFileName();

  public URI getUri();

  /**
   * @return A range associated with the file. If a file has an associated range then Accumulo will
   *         limit reads to within the range. Not all files have an associated range, it a file does
   *         not have a range then an infinite range is returned. The URI plus this range uniquely
   *         identify a file.
   *
   * @since 3.1.0
   */
  public Range getRange();

  public long getEstimatedSize();

  public long getEstimatedEntries();

  static CompactableFile create(URI uri, long estimatedSize, long estimatedEntries) {
    return new CompactableFileImpl(uri, estimatedSize, estimatedEntries);
  }

  /**
   * Creates a new CompactableFile object that implements this interface.
   *
   * @since 3.1.0
   */
  static CompactableFile create(URI uri, Range range, long estimatedSize, long estimatedEntries) {
    return new CompactableFileImpl(uri, range, estimatedSize, estimatedEntries);
  }
}
