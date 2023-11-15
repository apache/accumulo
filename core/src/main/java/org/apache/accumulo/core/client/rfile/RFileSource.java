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
package org.apache.accumulo.core.client.rfile;

import java.io.InputStream;
import java.util.Objects;

import org.apache.accumulo.core.data.Range;

/**
 * RFile metadata is stored at the end of the file. Inorder to read an RFile, its length must be
 * known. This provides a way to pass an InputStream and length for reading an RFile.
 *
 * @since 1.8.0
 */
public class RFileSource {
  private final InputStream in;
  private final long len;
  private final Range range;

  public RFileSource(InputStream in, long len) {
    this(in, len, new Range());
  }

  /**
   * @since 3.1.0
   */
  public RFileSource(InputStream in, long len, Range range) {
    this.in = Objects.requireNonNull(in);
    this.len = len;
    this.range = Objects.requireNonNull(range);
  }

  public InputStream getInputStream() {
    return in;
  }

  public long getLength() {
    return len;
  }

  /**
   * @return The range of the RFileSource
   *
   * @since 3.1.0
   */
  public Range getRange() {
    return range;
  }
}
