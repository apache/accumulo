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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.collections4.map.LRUMap;

import com.google.common.base.Preconditions;

/**
 * This class provides an API for writing RFiles. It can be used to create file for bulk import into
 * Accumulo using {@link TableOperations#importDirectory(String)}
 *
 * <p>
 * A RFileWriter has the following constraints. Violating these constraints will result in runtime
 * exceptions.
 *
 * <ul>
 * <li>Keys must be appended in sorted order within a locality group.</li>
 * <li>Locality groups must have a mutually exclusive set of column families.</li>
 * <li>The default locality group must be started last.</li>
 * <li>If using RFile.newWriter().to("filename.rf"), the ".rf" extension is required.</li>
 * </ul>
 *
 * <p>
 * Below is an example of using RFileWriter
 *
 * <pre>
 * <code>
 *     Iterable&lt;Entry&lt;Key, Value&gt;&gt; localityGroup1Data = ...
 *     Iterable&lt;Entry&lt;Key, Value&gt;&gt; localityGroup2Data = ...
 *     Iterable&lt;Entry&lt;Key, Value&gt;&gt; defaultGroupData = ...
 *
 *     try(RFileWriter writer = RFile.newWriter().to("filename.rf").build()) {
 *
 *       // Start a locality group before appending data.
 *       writer.startNewLocalityGroup("groupA", "columnFam1", "columnFam2");
 *       // Append data to the locality group that was started above. Must append in sorted order.
 *       writer.append(localityGroup1Data);
 *
 *       // Add another locality group.
 *       writer.startNewLocalityGroup("groupB", "columnFam3", "columnFam4");
 *       writer.append(localityGroup2Data);
 *
 *       // The default locality group must be started last.
 *       // The column families for the default group do not need to be specified.
 *       writer.startDefaultLocalityGroup();
 *       // Data appended here can not contain any
 *       // column families specified in previous locality groups.
 *       writer.append(defaultGroupData);
 *
 *       // This is a try-with-resources so the writer is closed here at the end of the code block.
 *     }
 * </code>
 * </pre>
 *
 * <p>
 * Create instances by calling {@link RFile#newWriter()}
 *
 * @since 1.8.0
 */
public class RFileWriter implements AutoCloseable {

  private FileSKVWriter writer;
  private final LRUMap<ByteSequence,Boolean> validVisibilities;
  private boolean startedLG;
  private boolean startedDefaultLG;

  RFileWriter(FileSKVWriter fileSKVWriter, int visCacheSize) {
    this.writer = fileSKVWriter;
    this.validVisibilities = new LRUMap<>(visCacheSize);
  }

  private void _startNewLocalityGroup(String name, Set<ByteSequence> columnFamilies)
      throws IOException {
    Preconditions.checkState(!startedDefaultLG,
        "Cannot start a locality group after starting the default locality group");
    writer.startNewLocalityGroup(name, columnFamilies);
    startedLG = true;
  }

  /**
   * Before appending any data, a locality group must be started. The default locality group must be
   * started last.
   *
   * @param name locality group name, used for informational purposes
   * @param families the column families the locality group can contain
   *
   * @throws IllegalStateException When default locality group already started.
   */
  public void startNewLocalityGroup(String name, List<byte[]> families) throws IOException {
    HashSet<ByteSequence> fams = new HashSet<>();
    for (byte[] family : families) {
      fams.add(new ArrayByteSequence(family));
    }
    _startNewLocalityGroup(name, fams);
  }

  /**
   * See javadoc for {@link #startNewLocalityGroup(String, List)}
   *
   * @throws IllegalStateException When default locality group already started.
   */
  public void startNewLocalityGroup(String name, byte[]... families) throws IOException {
    startNewLocalityGroup(name, Arrays.asList(families));
  }

  /**
   * See javadoc for {@link #startNewLocalityGroup(String, List)}.
   *
   * @param families will be encoded using UTF-8
   *
   * @throws IllegalStateException When default locality group already started.
   */
  public void startNewLocalityGroup(String name, Set<String> families) throws IOException {
    HashSet<ByteSequence> fams = new HashSet<>();
    for (String family : families) {
      fams.add(new ArrayByteSequence(family));
    }
    _startNewLocalityGroup(name, fams);
  }

  /**
   * See javadoc for {@link #startNewLocalityGroup(String, List)}.
   *
   * @param families will be encoded using UTF-8
   *
   * @throws IllegalStateException When default locality group already started.
   */
  public void startNewLocalityGroup(String name, String... families) throws IOException {
    HashSet<ByteSequence> fams = new HashSet<>();
    for (String family : families) {
      fams.add(new ArrayByteSequence(family));
    }
    _startNewLocalityGroup(name, fams);
  }

  /**
   * A locality group in which the column families do not need to specified. The locality group must
   * be started after all other locality groups. Can not append column families that were in a
   * previous locality group. If no locality groups were started, then the first append will start
   * the default locality group.
   *
   * @throws IllegalStateException When default locality group already started.
   */

  public void startDefaultLocalityGroup() throws IOException {
    Preconditions.checkState(!startedDefaultLG);
    writer.startDefaultLocalityGroup();
    startedDefaultLG = true;
    startedLG = true;
  }

  /**
   * Append the key and value to the last locality group that was started. If no locality group was
   * started, then the default group will automatically be started.
   *
   * @param key This key must be greater than or equal to the last key appended. For non-default
   *        locality groups, the keys column family must be one of the column families specified
   *        when calling startNewLocalityGroup(). Must be non-null.
   * @param val value to append, must be non-null.
   *
   * @throws IllegalArgumentException This is thrown when data is appended out of order OR when the
   *         key contains a invalid visibility OR when a column family is not valid for a locality
   *         group.
   */
  public void append(Key key, Value val) throws IOException {
    if (!startedLG) {
      startDefaultLocalityGroup();
    }
    Boolean wasChecked = validVisibilities.get(key.getColumnVisibilityData());
    if (wasChecked == null) {
      byte[] cv = key.getColumnVisibilityData().toArray();
      new ColumnVisibility(cv);
      validVisibilities.put(new ArrayByteSequence(Arrays.copyOf(cv, cv.length)), Boolean.TRUE);
    }
    writer.append(key, val);
  }

  /**
   * This method has the same behavior as {@link #append(Key, Value)}.
   *
   * @param key Same restrictions on key as {@link #append(Key, Value)}.
   * @param value this parameter will be UTF-8 encoded. Must be non-null.
   * @since 2.0.0
   */
  public void append(Key key, CharSequence value) throws IOException {
    append(key, new Value(value));
  }

  /**
   * This method has the same behavior as {@link #append(Key, Value)}.
   *
   * @param key Same restrictions on key as {@link #append(Key, Value)}.
   * @param value Must be non-null.
   * @since 2.0.0
   */
  public void append(Key key, byte[] value) throws IOException {
    append(key, new Value(value));
  }

  /**
   * Append the keys and values to the last locality group that was started.
   *
   * @param keyValues The keys must be in sorted order. The first key returned by the iterable must
   *        be greater than or equal to the last key appended. For non-default locality groups, the
   *        keys column family must be one of the column families specified when calling
   *        startNewLocalityGroup(). Must be non-null. If no locality group was started, then the
   *        default group will automatically be started.
   *
   * @throws IllegalArgumentException This is thrown when data is appended out of order OR when the
   *         key contains a invalid visibility OR when a column family is not valid for a locality
   *         group.
   */
  public void append(Iterable<Entry<Key,Value>> keyValues) throws IOException {
    for (Entry<Key,Value> entry : keyValues) {
      append(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }
}
