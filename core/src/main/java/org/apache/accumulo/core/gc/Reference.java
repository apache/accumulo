/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.accumulo.core.metadata.schema.MetadataSchema;

/**
 * A GC reference to a tablet file or directory.
 */
public class Reference implements Comparable<Reference> {
  // parts of an absolute URI, like "hdfs://1.2.3.4/accumulo/tables/2a/t-0003"
  public final TableId tableId; // 2a
  public final String tabletDir; // t-0003

  // the exact string that is stored in the metadata
  public final String metadataEntry;

  public Reference(TableId tableId, String metadataEntry) {
    MetadataSchema.TabletsSection.ServerColumnFamily.validateDirCol(tableId.canonical());
    this.tableId = tableId;
    this.metadataEntry = metadataEntry;
    this.tabletDir = metadataEntry;
  }

  @Override
  public int compareTo(Reference that) {
    if (equals(that)) {
      return 0;
    } else {
      return this.metadataEntry.compareTo(that.metadataEntry);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Reference other = (Reference) obj;
    if (metadataEntry == null) {
      return other.metadataEntry == null;
    } else
      return metadataEntry.equals(other.metadataEntry);
  }

  @Override
  public int hashCode() {
    return this.metadataEntry.hashCode();
  }
}
