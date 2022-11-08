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
package org.apache.accumulo.core.summary;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.summary.Summarizer;
import org.apache.accumulo.core.client.summary.Summarizer.Collector;
import org.apache.accumulo.core.client.summary.Summarizer.Combiner;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.summary.Gatherer.RowRange;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

/**
 * This class supports serializing summaries and periodically storing summaries. The implementations
 * attempts to generate around 10 summaries that are evenly spaced. This allows asking for summaries
 * for sub-ranges of data in a rfile.
 *
 * <p>
 * At first summaries are created for every 1000 keys values. After 10 summaries are added, the 10
 * summaries are merged to 5 and summaries are then created for every 2000 key values. The code
 * keeps merging summaries and doubling the amount of key values per summary. This results in each
 * summary covering about the same number of key values.
 */
class SummarySerializer {

  private SummarizerConfiguration sconf;
  private LgSummaries[] allSummaries;

  private SummarySerializer(SummarizerConfiguration sconf, LgSummaries[] allSummaries) {
    this.sconf = sconf;
    this.allSummaries = allSummaries;
  }

  private SummarySerializer(SummarizerConfiguration sconf) {
    this.sconf = sconf;
    // this indicates max size was exceeded
    this.allSummaries = null;
  }

  public SummarizerConfiguration getSummarizerConfiguration() {
    return sconf;
  }

  public void print(String prefix, String indent, PrintStream out) {

    if (allSummaries == null) {
      out.printf("%sSummary not stored because it was too large\n", prefix + indent);
    } else {
      for (LgSummaries lgs : allSummaries) {
        lgs.print(prefix, indent, out);
      }
    }
  }

  public Map<String,Long> getSummary(List<RowRange> ranges, SummarizerFactory sf) {

    Summarizer kvs = sf.getSummarizer(sconf);

    Map<String,Long> summary = new HashMap<>();
    for (LgSummaries lgs : allSummaries) {
      lgs.getSummary(ranges, kvs.combiner(sconf), summary);
    }
    return summary;
  }

  public boolean exceedsRange(List<RowRange> ranges) {
    boolean er = false;
    for (LgSummaries lgs : allSummaries) {
      for (RowRange ke : ranges) {
        er |= lgs.exceedsRange(ke.getStartRow(), ke.getEndRow());
        if (er) {
          return er;
        }
      }
    }

    return er;
  }

  public boolean exceededMaxSize() {
    return allSummaries == null;
  }

  private static class SummaryStoreImpl
      implements org.apache.accumulo.core.client.summary.Summarizer.StatisticConsumer {

    HashMap<String,Long> summaries;

    @Override
    public void accept(String summary, long value) {
      summaries.put(summary, value);
    }
  }

  private static class LgBuilder {
    private Summarizer summarizer;
    private SummarizerConfiguration conf;
    private Collector collector;

    private int maxSummaries = 10;

    private int cutoff = 1000;
    private int count = 0;

    private List<SummaryInfo> summaries = new ArrayList<>();

    private Key lastKey;

    private SummaryStoreImpl sci = new SummaryStoreImpl();

    private String name;

    private boolean sawFirst = false;
    private Text firstRow;

    private boolean finished = false;

    public LgBuilder(SummarizerConfiguration conf, Summarizer kvs) {
      this.conf = conf;
      this.summarizer = kvs;
      this.name = "<DEFAULT>";
      this.collector = kvs.collector(conf);
    }

    public LgBuilder(SummarizerConfiguration conf, Summarizer kvs, String name) {
      this.conf = conf;
      this.summarizer = kvs;
      this.name = name;
      this.collector = kvs.collector(conf);
    }

    public void put(Key k, Value v) {
      collector.accept(k, v);
      count++;

      if (!sawFirst) {
        firstRow = k.getRow();
        sawFirst = true;

      }

      if (count >= cutoff) {
        sci.summaries = new HashMap<>();
        collector.summarize(sci);
        collector = summarizer.collector(conf);
        addSummary(k.getRow(), sci.summaries, count);
        count = 0;
      }

      lastKey = k;
    }

    private List<SummaryInfo> merge(int end) {
      List<SummaryInfo> mergedSummaries = new ArrayList<>();
      for (int i = 0; i < end; i += 2) {
        int mergedCount = summaries.get(i).count + summaries.get(i + 1).count;
        summarizer.combiner(conf).merge(summaries.get(i).summary, summaries.get(i + 1).summary);
        mergedSummaries.add(new SummaryInfo(summaries.get(i + 1).getLastRow(),
            summaries.get(i).summary, mergedCount));
      }
      return mergedSummaries;
    }

    private void addSummary(Text row, Map<String,Long> summary, int count) {
      Preconditions.checkState(!finished);
      summaries.add(new SummaryInfo(row, summary, count));

      if (summaries.size() % 2 == 0 && summaries.size() > maxSummaries) {
        summaries = merge(summaries.size());
        cutoff *= 2;
      }
    }

    boolean collapse() {
      Preconditions.checkState(finished);
      if (summaries.size() <= 1) {
        return false;
      }

      int end = summaries.size();
      if (end % 2 == 1) {
        end--;
      }

      List<SummaryInfo> mergedSummaries = merge(end);

      if (summaries.size() % 2 == 1) {
        mergedSummaries.add(summaries.get(summaries.size() - 1));
      }

      summaries = mergedSummaries;

      return true;
    }

    void finish() {
      Preconditions.checkState(!finished);
      // summarize last data
      if (count > 0) {
        sci.summaries = new HashMap<>();
        collector.summarize(sci);
        collector = null;
        addSummary(lastKey.getRow(), sci.summaries, count);
        count = 0;
        finished = true;
      }
    }

    public void save(DataOutputStream dos, HashMap<String,Integer> symbolTable) throws IOException {
      Preconditions.checkState(count == 0);

      dos.writeUTF(name);

      if (firstRow == null) {
        WritableUtils.writeVInt(dos, 0);
      } else {
        firstRow.write(dos);
      }

      // write summaries
      WritableUtils.writeVInt(dos, summaries.size());
      for (SummaryInfo summaryInfo : summaries) {
        summaryInfo.getLastRow().write(dos);
        WritableUtils.writeVInt(dos, summaryInfo.count);
        saveSummary(dos, symbolTable, summaryInfo.summary);
      }
    }

    private void saveSummary(DataOutputStream dos, HashMap<String,Integer> symbolTable,
        Map<String,Long> summary) throws IOException {
      WritableUtils.writeVInt(dos, summary.size());
      for (Entry<String,Long> e : summary.entrySet()) {
        WritableUtils.writeVInt(dos, symbolTable.get(e.getKey()));
        WritableUtils.writeVLong(dos, e.getValue());
      }
    }
  }

  public static class Builder {
    private Summarizer kvs;

    private SummarizerConfiguration conf;

    private List<LgBuilder> locGroups;
    private LgBuilder lgb;

    private long maxSize;

    public Builder(SummarizerConfiguration conf, Summarizer kvs, long maxSize) {
      this.conf = conf;
      this.kvs = kvs;
      this.locGroups = new ArrayList<>();
      this.maxSize = maxSize;
    }

    public void put(Key k, Value v) {
      lgb.put(k, v);
    }

    public SummarizerConfiguration getSummarizerConfiguration() {
      return conf;
    }

    public void save(DataOutputStream dos) throws IOException {

      if (lgb != null) {
        lgb.finish();
        locGroups.add(lgb);
      }

      byte[] data = _save();

      while (data.length > maxSize) {
        boolean collapsedSome = false;
        for (LgBuilder lgBuilder : locGroups) {
          collapsedSome |= lgBuilder.collapse();
        }

        if (collapsedSome) {
          data = _save();
        } else {
          break;
        }
      }

      if (data.length > maxSize) {
        dos.writeBoolean(true);
      } else {
        dos.writeBoolean(false);
        // write this out to support efficient skipping
        WritableUtils.writeVInt(dos, data.length);
        dos.write(data);
      }
    }

    private byte[] _save() throws IOException {

      try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
          DataOutputStream dos = new DataOutputStream(baos)) {
        // create a symbol table
        HashMap<String,Integer> symbolTable = new HashMap<>();
        ArrayList<String> symbols = new ArrayList<>();
        for (LgBuilder lg : locGroups) {
          for (SummaryInfo si : lg.summaries) {
            for (String symbol : si.summary.keySet()) {
              if (!symbolTable.containsKey(symbol)) {
                symbolTable.put(symbol, symbols.size());
                symbols.add(symbol);
              }
            }
          }
        }

        // write symbol table
        WritableUtils.writeVInt(dos, symbols.size());
        for (String symbol : symbols) {
          dos.writeUTF(symbol);
        }

        WritableUtils.writeVInt(dos, locGroups.size());
        for (LgBuilder lg : locGroups) {
          lg.save(dos, symbolTable);
        }

        dos.flush();
        return baos.toByteArray();
      }
    }

    public void startNewLocalityGroup(String name) {
      if (lgb != null) {
        lgb.finish();
        locGroups.add(lgb);
      }

      lgb = new LgBuilder(conf, kvs, name);
    }

    public void startDefaultLocalityGroup() {
      if (lgb != null) {
        lgb.finish();
        locGroups.add(lgb);
      }

      lgb = new LgBuilder(conf, kvs);
    }
  }

  public static Builder builder(SummarizerConfiguration conf, SummarizerFactory factory,
      long maxSize) {
    return new Builder(conf, factory.getSummarizer(conf), maxSize);
  }

  static void skip(DataInputStream in) throws IOException {
    boolean exceededMaxSize = in.readBoolean();
    if (!exceededMaxSize) {
      long len = WritableUtils.readVInt(in);
      long skipped = in.skip(len);
      while (skipped < len) {
        skipped += in.skip(len - skipped);
      }
    }
  }

  static SummarySerializer load(SummarizerConfiguration sconf, DataInputStream in)
      throws IOException {
    boolean exceededMaxSize = in.readBoolean();
    if (exceededMaxSize) {
      return new SummarySerializer(sconf);
    } else {
      WritableUtils.readVInt(in);
      // load symbol table
      int numSymbols = WritableUtils.readVInt(in);
      String[] symbols = new String[numSymbols];
      for (int i = 0; i < numSymbols; i++) {
        symbols[i] = in.readUTF();
      }

      int numLGroups = WritableUtils.readVInt(in);
      LgSummaries[] allSummaries = new LgSummaries[numLGroups];
      for (int i = 0; i < numLGroups; i++) {
        allSummaries[i] = readLGroup(in, symbols);
      }

      return new SummarySerializer(sconf, allSummaries);
    }
  }

  private static class LgSummaries {

    private Text firstRow;
    private SummaryInfo[] summaries;
    private String lgroupName;

    LgSummaries(Text firstRow, SummaryInfo[] summaries, String lgroupName) {
      this.firstRow = firstRow;
      this.summaries = summaries;
      this.lgroupName = lgroupName;
    }

    boolean exceedsRange(Text startRow, Text endRow) {

      if (summaries.length == 0) {
        return false;
      }

      Text lastRow = summaries[summaries.length - 1].lastRow;
      if (startRow != null && firstRow.compareTo(startRow) <= 0
          && startRow.compareTo(lastRow) < 0) {
        return true;
      }
      return endRow != null && endRow.compareTo(firstRow) >= 0 && lastRow.compareTo(endRow) > 0;
    }

    void print(String prefix, String indent, PrintStream out) {
      String p = prefix + indent;
      out.printf("%sLocality group : %s\n", p, lgroupName);
      p += indent;
      for (SummaryInfo si : summaries) {
        out.printf("%sSummary of %d key values (row of last key '%s') : \n", p, si.count,
            si.lastRow);
        Set<Entry<String,Long>> es = si.summary.entrySet();
        String p2 = p + indent;
        for (Entry<String,Long> entry : es) {
          out.printf("%s%s = %s\n", p2, entry.getKey(), entry.getValue());
        }
      }
    }

    void getSummary(List<RowRange> ranges, Combiner combiner, Map<String,Long> summary) {
      boolean[] summariesThatOverlap = new boolean[summaries.length];

      for (RowRange keyExtent : ranges) {
        Text startRow = keyExtent.getStartRow();
        Text endRow = keyExtent.getEndRow();

        if (endRow != null && endRow.compareTo(firstRow) < 0) {
          continue;
        }

        int start = -1;
        int end = summaries.length - 1;

        if (startRow == null) {
          start = 0;
        } else {
          for (int i = 0; i < summaries.length; i++) {
            if (startRow.compareTo(summaries[i].getLastRow()) < 0) {
              start = i;
              break;
            }
          }
        }

        if (start == -1) {
          continue;
        }

        if (endRow == null) {
          end = summaries.length - 1;
        } else {
          for (int i = start; i < summaries.length; i++) {
            if (endRow.compareTo(summaries[i].getLastRow()) < 0) {
              end = i;
              break;
            }
          }
        }

        for (int i = start; i <= end; i++) {
          summariesThatOverlap[i] = true;
        }
      }

      for (int i = 0; i < summaries.length; i++) {
        if (summariesThatOverlap[i]) {
          combiner.merge(summary, summaries[i].summary);
        }
      }
    }
  }

  private static LgSummaries readLGroup(DataInputStream in, String[] symbols) throws IOException {
    String lgroupName = in.readUTF();

    // read first row
    Text firstRow = new Text();
    firstRow.readFields(in);

    // read summaries
    int numSummaries = WritableUtils.readVInt(in);
    SummaryInfo[] summaries = new SummaryInfo[numSummaries];
    for (int i = 0; i < numSummaries; i++) {
      int rowLen = WritableUtils.readVInt(in);
      byte[] row = new byte[rowLen];
      in.readFully(row);
      int count = WritableUtils.readVInt(in);
      Map<String,Long> summary = readSummary(in, symbols);
      summaries[i] = new SummaryInfo(row, summary, count);
    }

    return new LgSummaries(firstRow, summaries, lgroupName);
  }

  private static Map<String,Long> readSummary(DataInputStream in, String[] symbols)
      throws IOException {
    final var imb = ImmutableMap.<String,Long>builder();
    int numEntries = WritableUtils.readVInt(in);

    for (int i = 0; i < numEntries; i++) {
      String symbol = symbols[WritableUtils.readVInt(in)];
      imb.put(symbol, WritableUtils.readVLong(in));
    }

    return imb.build();
  }
}
