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
package org.apache.accumulo.core.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.thrift.TMutation;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.rfile.RFile.Reader;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.io.Text;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;

public class LocalityGroupUtil {

  // private static final Logger log = Logger.getLogger(ColumnFamilySet.class);

  // using an ImmutableSet here for more efficient comparisons in LocalityGroupIterator
  public static final ImmutableSet<ByteSequence> EMPTY_CF_SET = ImmutableSet.of();

  /**
   * Create a set of families to be passed into the SortedKeyValueIterator seek call from a supplied set of columns. We are using the ImmutableSet to enable
   * faster comparisons down in the LocalityGroupIterator.
   *
   * @param columns
   *          The set of columns
   * @return An immutable set of columns
   */
  public static ImmutableSet<ByteSequence> families(Collection<Column> columns) {
    if (columns.size() == 0)
      return EMPTY_CF_SET;
    Builder<ByteSequence> builder = ImmutableSet.builder();
    columns.forEach(c -> builder.add(new ArrayByteSequence(c.getColumnFamily())));
    return builder.build();
  }

  @SuppressWarnings("serial")
  static public class LocalityGroupConfigurationError extends AccumuloException {
    LocalityGroupConfigurationError(String why) {
      super(why);
    }
  }

  public static Map<String,Set<ByteSequence>> getLocalityGroups(AccumuloConfiguration acuconf) throws LocalityGroupConfigurationError {
    Map<String,Set<ByteSequence>> result = new HashMap<>();
    String[] groups = acuconf.get(Property.TABLE_LOCALITY_GROUPS).split(",");
    for (String group : groups) {
      if (group.length() > 0)
        result.put(group, new HashSet<ByteSequence>());
    }
    HashSet<ByteSequence> all = new HashSet<>();
    for (Entry<String,String> entry : acuconf) {
      String property = entry.getKey();
      String value = entry.getValue();
      String prefix = Property.TABLE_LOCALITY_GROUP_PREFIX.getKey();
      if (property.startsWith(prefix)) {
        // this property configures a locality group, find out which one:
        String group = property.substring(prefix.length());
        String[] parts = group.split("\\.");
        group = parts[0];
        if (result.containsKey(group)) {
          if (parts.length == 1) {
            Set<ByteSequence> colFamsSet = decodeColumnFamilies(value);
            if (!Collections.disjoint(all, colFamsSet)) {
              colFamsSet.retainAll(all);
              throw new LocalityGroupConfigurationError("Column families " + colFamsSet + " in group " + group + " is already used by another locality group");
            }

            all.addAll(colFamsSet);
            result.put(group, colFamsSet);
          }
        }
      }
    }
    // result.put("", all);
    return result;
  }

  public static Set<ByteSequence> decodeColumnFamilies(String colFams) throws LocalityGroupConfigurationError {
    HashSet<ByteSequence> colFamsSet = new HashSet<>();

    for (String family : colFams.split(",")) {
      ByteSequence cfbs = decodeColumnFamily(family);
      colFamsSet.add(cfbs);
    }

    return colFamsSet;
  }

  public static ByteSequence decodeColumnFamily(String colFam) throws LocalityGroupConfigurationError {
    byte output[] = new byte[colFam.length()];
    int pos = 0;

    for (int i = 0; i < colFam.length(); i++) {
      char c = colFam.charAt(i);

      if (c == '\\') {
        // next char must be 'x' or '\'
        i++;

        if (i >= colFam.length()) {
          throw new LocalityGroupConfigurationError("Expected 'x' or '\' after '\'  in " + colFam);
        }

        char nc = colFam.charAt(i);

        switch (nc) {
          case '\\':
            output[pos++] = '\\';
            break;
          case 'x':
            // next two chars must be [0-9][0-9]
            i++;
            output[pos++] = (byte) (0xff & Integer.parseInt(colFam.substring(i, i + 2), 16));
            i++;
            break;
          default:
            throw new LocalityGroupConfigurationError("Expected 'x' or '\' after '\'  in " + colFam);
        }
      } else {
        output[pos++] = (byte) (0xff & c);
      }

    }

    return new ArrayByteSequence(output, 0, pos);

  }

  public static String encodeColumnFamilies(Set<Text> colFams) {
    SortedSet<String> ecfs = new TreeSet<>();

    StringBuilder sb = new StringBuilder();

    for (Text text : colFams) {
      String ecf = encodeColumnFamily(sb, text.getBytes(), text.getLength());
      ecfs.add(ecf);
    }

    return Joiner.on(",").join(ecfs);
  }

  public static String encodeColumnFamily(ByteSequence bs) {
    if (bs.offset() != 0) {
      throw new IllegalArgumentException("The offset cannot be non-zero.");
    }
    return encodeColumnFamily(new StringBuilder(), bs.getBackingArray(), bs.length());
  }

  private static String encodeColumnFamily(StringBuilder sb, byte[] ba, int len) {
    sb.setLength(0);

    for (int i = 0; i < len; i++) {
      int c = 0xff & ba[i];
      if (c == '\\')
        sb.append("\\\\");
      else if (c >= 32 && c <= 126 && c != ',')
        sb.append((char) c);
      else
        sb.append("\\x").append(String.format("%02X", c));
    }

    String ecf = sb.toString();
    return ecf;
  }

  public static class PartitionedMutation extends Mutation {
    private byte[] row;
    private List<ColumnUpdate> updates;

    public PartitionedMutation(byte[] row, List<ColumnUpdate> updates) {
      this.row = row;
      this.updates = updates;
    }

    @Override
    public byte[] getRow() {
      return row;
    }

    @Override
    public List<ColumnUpdate> getUpdates() {
      return updates;
    }

    @Override
    public TMutation toThrift() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int hashCode() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Mutation m) {
      throw new UnsupportedOperationException();
    }
  }

  public static class Partitioner {

    private Map<ByteSequence,Integer> colfamToLgidMap;
    private PreAllocatedArray<Map<ByteSequence,MutableLong>> groups;

    public Partitioner(PreAllocatedArray<Map<ByteSequence,MutableLong>> groups) {
      this.groups = groups;
      this.colfamToLgidMap = new HashMap<>();

      for (int i = 0; i < groups.length; i++) {
        for (ByteSequence cf : groups.get(i).keySet()) {
          colfamToLgidMap.put(cf, i);
        }
      }
    }

    public void partition(List<Mutation> mutations, PreAllocatedArray<List<Mutation>> partitionedMutations) {

      MutableByteSequence mbs = new MutableByteSequence(new byte[0], 0, 0);

      PreAllocatedArray<List<ColumnUpdate>> parts = new PreAllocatedArray<>(groups.length + 1);

      for (Mutation mutation : mutations) {
        if (mutation.getUpdates().size() == 1) {
          int lgid = getLgid(mbs, mutation.getUpdates().get(0));
          partitionedMutations.get(lgid).add(mutation);
        } else {
          for (int i = 0; i < parts.length; i++) {
            parts.set(i, null);
          }

          int lgcount = 0;

          for (ColumnUpdate cu : mutation.getUpdates()) {
            int lgid = getLgid(mbs, cu);

            if (parts.get(lgid) == null) {
              parts.set(lgid, new ArrayList<ColumnUpdate>());
              lgcount++;
            }

            parts.get(lgid).add(cu);
          }

          if (lgcount == 1) {
            for (int i = 0; i < parts.length; i++)
              if (parts.get(i) != null) {
                partitionedMutations.get(i).add(mutation);
                break;
              }
          } else {
            for (int i = 0; i < parts.length; i++)
              if (parts.get(i) != null)
                partitionedMutations.get(i).add(new PartitionedMutation(mutation.getRow(), parts.get(i)));
          }
        }
      }
    }

    private Integer getLgid(MutableByteSequence mbs, ColumnUpdate cu) {
      mbs.setArray(cu.getColumnFamily(), 0, cu.getColumnFamily().length);
      Integer lgid = colfamToLgidMap.get(mbs);
      if (lgid == null)
        lgid = groups.length;
      return lgid;
    }
  }

  /**
   * This method created to help seek an rfile for a locality group obtained from {@link Reader#getLocalityGroupCF()}. This method can possibly return an empty
   * list for the default locality group. When this happens the default locality group needs to be seeked differently. This method helps do that.
   *
   * <p>
   * For the default locality group will seek using the families of all other locality groups non-inclusive.
   *
   * @see Reader#getLocalityGroupCF()
   */
  public static void seek(FileSKVIterator reader, Range range, String lgName, Map<String,ArrayList<ByteSequence>> localityGroupCF) throws IOException {

    Collection<ByteSequence> families;
    boolean inclusive;
    if (lgName == null) {
      // this is the default locality group, create a set of all families not in the default group
      Set<ByteSequence> nonDefaultFamilies = new HashSet<>();
      for (Entry<String,ArrayList<ByteSequence>> entry : localityGroupCF.entrySet()) {
        if (entry.getKey() != null) {
          nonDefaultFamilies.addAll(entry.getValue());
        }
      }

      families = nonDefaultFamilies;
      inclusive = false;
    } else {
      families = localityGroupCF.get(lgName);
      inclusive = true;
    }

    reader.seek(range, families, inclusive);
  }

  static public void ensureNonOverlappingGroups(Map<String,Set<Text>> groups) {
    HashSet<Text> all = new HashSet<>();
    for (Entry<String,Set<Text>> entry : groups.entrySet()) {
      if (!Collections.disjoint(all, entry.getValue())) {
        throw new IllegalArgumentException("Group " + entry.getKey() + " overlaps with another group");
      }
      all.addAll(entry.getValue());
    }
  }
}
