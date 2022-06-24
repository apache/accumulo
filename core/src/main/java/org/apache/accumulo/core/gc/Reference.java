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
package org.apache.accumulo.core.gc;

import org.apache.accumulo.core.data.TableId;

/**
 * A GC reference used for collecting files and directories into a single stream. The GC deals with
 * two inputs conceptually: candidates and references. Candidates are files that could be possibly
 * be deleted if they are not defeated by a reference.
 */
public interface Reference {
  /**
   * Only return true if the reference is a directory.
   */
  boolean isDirectory();

  /**
   * Get the {@link TableId} of the reference.
   */
  TableId getTableId();

  /**
   * Get the exact string stored in the metadata table for this file or directory. A file will be
   * read from the Tablet "file" column family:
   * {@link org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily}
   * A directory will be read from the "srv:dir" column family:
   * {@link org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily}
   */
  String getMetadataEntry();
}
