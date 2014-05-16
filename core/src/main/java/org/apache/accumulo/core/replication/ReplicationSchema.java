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

import java.nio.charset.CharacterCodingException;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.lexicoder.ULongLexicoder;
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
   * <code>hdfs://localhost:8020/accumulo/wal/tserver+port/WAL work:serialized_ReplicationTarget [] -> Status Protobuf</code>
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
   * <code>hdfs://localhost:8020/accumulo/wal/tserver+port/WAL repl:local_table_id [] -> Status Protobuf</code>
   */
  public static class StatusSection {
    public static final Text NAME = new Text("repl");
    private static final ByteSequence BYTE_SEQ_NAME = new ArrayByteSequence("repl"); 

    /**
     * Extract the table ID from the key (inefficiently if called repeatedly)
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
     * Extract the table ID from the key into the given {@link Text}
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
     * Limit the scanner to only return Status records
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

  /**
   * Holds the order in which files needed for replication were closed. The intent is to be able to guarantee
   * that files which were closed earlier were replicated first and we don't replay data in the wrong order on our peers
   * <p>
   * <code>encodedTimeOfClosure_hdfs://localhost:8020/accumulo/wal/tserver+port/WAL order:source_table_id [] -> Status Protobuf</code>
   */
  public static class OrderSection {
    public static final Text NAME = new Text("order");
    public static final String ROW_SEPARATOR = "_";
    private static final ULongLexicoder longEncoder = new ULongLexicoder();

    /**
     * Extract the table ID from the given key (inefficiently if called repeatedly)
     * @param k OrderSection Key
     * @return source table id
     */
    public static String getTableId(Key k) {
      Text buff = new Text();
      getTableId(k, buff);
      return buff.toString();
    }

    /**
     * Extract the table ID from the given key
     * @param k OrderSection key
     * @param buff Text to place table ID into
     */
    public static void getTableId(Key k, Text buff) {
      Preconditions.checkNotNull(k);
      Preconditions.checkNotNull(buff);

      k.getColumnQualifier(buff);
    }

    /**
     * Limit the scanner to only return Order records
     * @param scanner
     */
    public static void limit(ScannerBase scanner) {
      scanner.fetchColumnFamily(NAME);
    }

    /**
     * Creates the Mutation for the Order section for the given file and time, adding the column
     * as well using {@link OrderSection#add(Mutation, Text, Value)}
     * @param file Filename
     * @param timeInMillis Time in millis that the file was closed
     * @param tableId Source table id
     * @param v Serialized Status msg as a Value
     * @return Mutation for the Order section
     */
    public static Mutation createMutation(String file, long timeInMillis, Text tableId, Value v) {
      Preconditions.checkNotNull(file);
      Preconditions.checkArgument(timeInMillis >= 0, "timeInMillis must be greater than zero");
      Preconditions.checkNotNull(v);

      // Encode the time so it sorts properly
      byte[] rowPrefix = longEncoder.encode(timeInMillis);
      Text row = new Text(rowPrefix);
      // Append the file as a suffix to the row
      row.append((ROW_SEPARATOR+file).getBytes(), 0, file.length() + ROW_SEPARATOR.length());

      // Make the mutation and add the column update
      Mutation m = new Mutation(row);
      return add(m, tableId, v);
    }

    /**
     * Add a column update to the given mutation with the provided tableId and value
     * @param m Mutation for OrderSection
     * @param tableId Source table id
     * @param v Serialized Status msg
     * @return The original Mutation
     */
    public static Mutation add(Mutation m, Text tableId, Value v) {
      m.put(NAME, tableId, v);
      return m;
    }

    public static long getTimeClosed(Key k) {
      return getTimeClosed(k, new Text());
    }

    public static long getTimeClosed(Key k, Text buff) {
      k.getRow(buff);
      int offset = buff.find(ROW_SEPARATOR);
      if (-1 == offset) {
        throw new IllegalArgumentException("Row does not contain expected separator for OrderSection");
      }

      byte[] encodedLong = new byte[offset];
      System.arraycopy(buff.getBytes(), 0, encodedLong, 0, offset);
      return longEncoder.decode(encodedLong);
    }

    public static String getFile(Key k) {
      Text buff = new Text();
      return getFile(k, buff);
    }

    public static String getFile(Key k, Text buff) {
      k.getRow(buff);
      int offset = buff.find(ROW_SEPARATOR);
      if (-1 == offset) {
        throw new IllegalArgumentException("Row does not contain expected separator for OrderSection");
      }

      try {
        return Text.decode(buff.getBytes(), offset + 1, buff.getLength() - (offset + 1));
      } catch (CharacterCodingException e) {
        throw new IllegalArgumentException("Could not decode file path", e);
      }
    }
  }

  private static void _getFile(Key k, Text buff) {
    k.getRow(buff);
  }
}
