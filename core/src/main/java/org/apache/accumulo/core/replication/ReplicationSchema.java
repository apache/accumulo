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
package org.apache.accumulo.core.replication;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.schema.Section;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

/**
 * 
 */
public class ReplicationSchema {

  public static class StatusSection {
    public static final Text NAME = new Text("repl");
  }

  public static class WorkSection {
    private static final Section section = new Section("work", false, "worl", false);

    public static Range getRange() {
      return section.getRange();
    }

    public static String getRowPrefix() {
      return section.getRowPrefix();
    }
  }

  /**
   * Holds replication markers tracking status for files
   */
  public static class ReplicationSection {
    private static final Section section = new Section("repl", false, "repm", false);

    public static Range getRange() {
      return section.getRange();
    }

    public static String getRowPrefix() {
      return section.getRowPrefix();
    }

    /**
     * Extract the table ID from the colfam (inefficiently if called repeatedly)
     * @param k Key to extract from
     * @return The table ID
     * @see #getTableId(Key,Text) 
     */
    public static String getTableId(Key k) {
      Text buff = new Text();
      getTableId(k, buff);
      return buff.toString();
    }

    /**
     * Extract the table ID from the colfam into the given {@link Text}
     * @param k Key to extract from
     * @param buff Text to place table ID into
     */
    public static void getTableId(Key k, Text buff) {
      Preconditions.checkNotNull(k);
      Preconditions.checkNotNull(buff);
      Preconditions.checkArgument(getRange().contains(k), "Key (%s) does not fall within ReplicationSection range", k);

      k.getColumnFamily(buff);
    }

    /**
     * Extract the file name from the row suffix into the given {@link Text}
     * @param k Key to extract from
     * @param buff Text to place file name into
     */
    public static void getFile(Key k, Text buff) {
      Preconditions.checkNotNull(k);
      Preconditions.checkNotNull(buff);
      Preconditions.checkArgument(getRange().contains(k), "Key (%s) does not fall within ReplicationSection range", k);

      k.getRow(buff);
      ByteSequence rowByteSequence = k.getRowData();

      // No implementation that isn't backed by an array..
      Preconditions.checkArgument(rowByteSequence.isBackedByArray());

      byte[] rowBytes = rowByteSequence.getBackingArray();
      int rowLength = rowByteSequence.length();

      // We should have at the "~repl" plus something
      int rowPrefixLength = getRowPrefix().length();
      Preconditions.checkArgument(rowLength > rowPrefixLength);

      buff.set(rowBytes, rowPrefixLength, rowLength - rowPrefixLength);
    }
  }
}
