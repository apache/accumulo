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

package org.apache.accumulo.core.file.summary;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.summary.KeyValueSummarizer;
import org.apache.accumulo.core.client.summary.KeyValueSummarizer.SummaryConsumer;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.collect.ImmutableMap;

/**
 * This class supports serializing summaries and periodically storing summaries. The implementations attempts to generate around 10 summaries that are evenly
 * spaced. This allows asking for summaries for sub-ranges of data in a rfile.
 * 
 * <p>
 * At first summaries are created for every 1000 keys values. After 10 summaries are added, the 10 summaries are merged to 5 and summaries are then created for
 * every 2000 key values. The code keeps merging summaries and doubling the amount of key values per summary. This results in each summary covering about the
 * same number of key values.
 *
 */

public class SummaryStore {

  // echo "accumulo summarize" | sha1sum | head -c 8
  private static long MAGIC = 0x15ea283ec03e4c49L;
  private static byte VER = 1;

  private String id;
  private KeyValueSummarizer kvs;
  private SummaryInfo[] summaries;

  private SummaryStore(String id, KeyValueSummarizer kvs, SummaryInfo[] summaries) {
    this.id = id;
    this.kvs = kvs;
    this.summaries = summaries;
  }

  public String getId() {
    return id;
  }

  public Map<String,Long> getSummary(ByteSequence startRow, ByteSequence endRow) {

    // TODO sanity check that start < end?

    if (summaries.length == 1) {
      // TODO could be after
      return summaries[0].summary;
    }

    // TODO use binary search

    int start = -1;
    int end = summaries.length - 1;

    for (int i = 0; i < summaries.length; i++) {
      if (startRow.compareTo(summaries[i].getLastRow()) <= 0) {
        start = i;
        break;
      }
    }

    if (start == -1) {
      return Collections.emptyMap();
    }

    for (int i = start; i < summaries.length; i++) {
      if (endRow.compareTo(summaries[i].getLastRow()) <= 0) {
        end = i;
        break;
      }
    }

    if (start == end) {
      return summaries[start].summary;
    }

    // TODO could pass in a map to merge into
    HashMap<String,Long> summary = new HashMap<>();
    for (int i = start; i <= end; i++) {
      kvs.merge(summary, summaries[i].summary);
    }

    return summary;
  }

  private static class SummaryConsumerImpl implements SummaryConsumer {

    HashMap<String,Long> summaries;

    @Override
    public void consume(String summary, long value) {
      summaries.put(summary, value);
    }
  }

  public static class Builder {
    private KeyValueSummarizer kvs;

    private int maxTotalSize = 1 << 17;
    private int maxSummaries = 10;

    private int cutoff = 1000;
    private int count = 0;

    private List<SummaryInfo> summaries = new ArrayList<>();

    private Key lastKey;

    private SummaryConsumerImpl sci = new SummaryConsumerImpl();

    private Builder(KeyValueSummarizer kvs) {
      this.kvs = kvs;
    }

    public void put(Key k, Value v) {
      kvs.collect(k, v);
      count++;

      if (count >= cutoff) {
        sci.summaries = new HashMap<>();
        kvs.summarize(sci);
        kvs.reset();
        addSummary(k.getRowData(), sci.summaries);
        count = 0;
      }

      lastKey = k;
    }

    private int totalSize() {
      return summaries.stream().mapToInt(si -> si.getEstimatedSize()).sum();
    }

    private static ByteSequence copy(ByteSequence bs) {
      if (bs.isBackedByArray()) {
        return new ArrayByteSequence(Arrays.copyOfRange(bs.getBackingArray(), bs.offset(), bs.offset() + bs.length()));
      } else {
        return new ArrayByteSequence(bs.toArray());
      }
    }

    private void addSummary(ByteSequence row, Map<String,Long> summary) {
      summaries.add(new SummaryInfo(copy(row), summary));

      while (summaries.size() % 2 == 0 && (summaries.size() > maxSummaries || totalSize() > maxTotalSize)) {
        List<SummaryInfo> mergedSummaries = new ArrayList<>();
        for (int i = 0; i < summaries.size(); i += 2) {
          kvs.merge(summaries.get(i).summary, summaries.get(i + 1).summary);
          mergedSummaries.add(new SummaryInfo(summaries.get(i + 1).getLastRow(), summaries.get(i).summary));
        }
        summaries = mergedSummaries;
        cutoff *= 2;
      }
    }

    public void save(DataOutputStream dos) throws IOException {
      dos.writeLong(MAGIC);
      dos.write(VER);

      dos.writeUTF(kvs.getClass().getName());
      dos.writeUTF(kvs.getId());

      if (count > 0) {
        sci.summaries = new HashMap<>();
        kvs.summarize(sci);
        kvs.reset();
        addSummary(lastKey.getRowData(), sci.summaries);
        count = 0;
      }

      // TODO need to merge summaries with same last row

      // create a symbol table
      HashMap<String,Integer> symbolTable = new HashMap<>();
      ArrayList<String> symbols = new ArrayList<>();
      for (SummaryInfo si : summaries) {
        for (String symbol : si.summary.keySet()) {
          if (!symbolTable.containsKey(symbol)) {
            symbolTable.put(symbol, symbols.size());
            symbols.add(symbol);
          }
        }
      }

      // write symbol table
      WritableUtils.writeVInt(dos, symbols.size());
      for (String symbol : symbols) {
        dos.writeUTF(symbol);
      }

      // write summaries
      WritableUtils.writeVInt(dos, summaries.size());
      for (SummaryInfo summaryInfo : summaries) {
        WritableUtils.writeVInt(dos, summaryInfo.getLastRow().length());
        dos.write(summaryInfo.getLastRow().toArray());
        saveSummary(dos, symbolTable, summaryInfo.summary);
      }

    }

    private void saveSummary(DataOutputStream dos, HashMap<String,Integer> symbolTable, Map<String,Long> summary) throws IOException {
      WritableUtils.writeVInt(dos, summary.size());
      for (Entry<String,Long> e : summary.entrySet()) {
        WritableUtils.writeVInt(dos, symbolTable.get(e.getKey()));
        WritableUtils.writeVLong(dos, e.getValue());
      }
    }
  }

  public static Builder builder(KeyValueSummarizer kvs) {
    return new Builder(kvs);
  }

  public static interface KeyValueSummarizerFactory {
    public KeyValueSummarizer getSummarizer(String id, String clazz);
  }

  public static SummaryStore load(DataInputStream in, KeyValueSummarizerFactory factory) throws IOException {
    long magic = in.readLong();
    if (magic != MAGIC) {
      throw new IOException("Bad magic : " + String.format("%x", magic));
    }

    byte ver = in.readByte();
    if (ver != VER) {
      throw new IOException("Unknown version : " + ver);
    }

    String summarizerClazz = in.readUTF();
    String expectedId = in.readUTF();

    KeyValueSummarizer kvs = factory.getSummarizer(expectedId, summarizerClazz);

    // load symbol table
    int numSymbols = WritableUtils.readVInt(in);
    String[] symbols = new String[numSymbols];
    for (int i = 0; i < numSymbols; i++) {
      symbols[i] = in.readUTF();
    }

    int numSummaries = WritableUtils.readVInt(in);
    SummaryInfo[] summaries = new SummaryInfo[numSummaries];
    for (int i = 0; i < numSummaries; i++) {
      int rowLen = WritableUtils.readVInt(in);
      byte[] row = new byte[rowLen];
      in.read(row);
      Map<String,Long> summary = readSummary(in, symbols);
      summaries[i] = new SummaryInfo(row, summary);
    }

    return new SummaryStore(expectedId, kvs, summaries);
  }

  private static Map<String,Long> readSummary(DataInputStream in, String[] symbols) throws IOException {
    com.google.common.collect.ImmutableMap.Builder<String,Long> imb = ImmutableMap.builder();
    int numEntries = WritableUtils.readVInt(in);

    for (int i = 0; i < numEntries; i++) {
      String symbol = symbols[WritableUtils.readVInt(in)];
      imb.put(symbol, WritableUtils.readVLong(in));
    }

    return imb.build();
  }
}
