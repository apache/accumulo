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

import static java.util.Objects.requireNonNull;

import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.fs.Path;

/**
 * A reference to a tablet file whose path is not in Accumulo's conventional tables directory. An
 * external file is still owned by Accumulo and is eligible for garbage collection when no longer
 * referenced.
 */
public class ExternalTabletFile extends ReferencedTabletFile {

  public ExternalTabletFile(Path path) {
    this(path, new Range());
  }

  public ExternalTabletFile(Path path, Range range) {
    super(requireNonNull(path), range, true);
  }

  public static ExternalTabletFile of(Path path) {
    return new ExternalTabletFile(path);
  }

  public static ExternalTabletFile of(Path path, Range range) {
    return new ExternalTabletFile(path, range);
  }
}
