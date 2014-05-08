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

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

/**
 * 
 */
public class ReplicationSchema {

  /**
   * Portion of a file that must be replication to the given target: peer and some identifying location on that peer, e.g. remote table ID
   * <p>
   * <code>hdfs://localhost:8020/accumulo/wal/tserver+port/WAL work:serialized_ReplicationTarget [] -> Protobuf</code>
   */
  public static class WorkSection {
    public static final Text NAME = new Text("work");
    private static final ByteSequence BYTE_SEQ_NAME = new ArrayByteSequence("work");

    public static void getFile(Key k, Text buff) {
      Preconditions.checkNotNull(k);
      Preconditions.checkNotNull(buff);
      Preconditions.checkArgument(BYTE_SEQ_NAME.equals(k.getColumnFamilyData()), "Given replication work key with incorrect colfam");
      _getFile(k, buff);
    }

    public static ReplicationTarget getTarget(Key k) {
      return getTarget(k, new Text());
    }

    public static ReplicationTarget getTarget(Key k, Text buff) {
      Preconditions.checkArgument(BYTE_SEQ_NAME.equals(k.getColumnFamilyData()), "Given replication work key with incorrect colfam");
      k.getColumnQualifier(buff);

      return ReplicationTarget.from(buff);
    }

    /**
     * Limit the scanner to only pull replication work records
     * @param scanner
     */
    public static void limit(ScannerBase scanner) {
      scanner.fetchColumnFamily(NAME);
    }

    public static Mutation add(Mutation m, Text serializedTarget, Value v) {
      m.put(NAME, serializedTarget, v);
      return m;
    }
  }

  /**
   * Holds replication markers tracking status for files
   * <p>
   * <code>hdfs://localhost:8020/accumulo/wal/tserver+port/WAL repl:local_table_id [] -> Protobuf</code>
   */
  public static class StatusSection {
    public static final Text NAME = new Text("repl");
    private static final ByteSequence BYTE_SEQ_NAME = new ArrayByteSequence("repl");

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

      k.getColumnQualifier(buff);
    }

    /**
     * Extract the file name from the row suffix into the given {@link Text}
     * @param k Key to extract from
     * @param buff Text to place file name into
     */
    public static void getFile(Key k, Text buff) {
      Preconditions.checkNotNull(k);
      Preconditions.checkNotNull(buff);
      Preconditions.checkArgument(BYTE_SEQ_NAME.equals(k.getColumnFamilyData()), "Given replication status key with incorrect colfam");

      _getFile(k, buff);
    }

    /**
     * Limit the scanner to only return ingest records
     * @param scanner
     */
    public static void limit(ScannerBase scanner) {
      scanner.fetchColumnFamily(NAME);
    }

    public static Mutation add(Mutation m, Text tableId, Value v) {
      m.put(NAME, tableId, v);
      return m;
    }
  }

  private static void _getFile(Key k, Text buff) {
    k.getRow(buff);
  }
}
