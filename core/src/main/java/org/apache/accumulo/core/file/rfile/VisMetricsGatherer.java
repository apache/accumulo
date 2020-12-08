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
package org.apache.accumulo.core.file.rfile;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.PrintStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import com.google.common.util.concurrent.AtomicLongMap;

/**
 * This class provides visibility metrics per locality group. The Map in getMetrics() maps the
 * locality group name to an ArrayList of VisibilityMetric objects. These contain the components of
 * a visibility metric; the visibility as a String, the number of times that is seen in a locality
 * group, the percentage of keys that contain that visibility in the locality group, the number of
 * blocks in the locality group that contain the visibility, and the percentage of blocks in the
 * locality group that contain the visibility.
 */
public class VisMetricsGatherer
    implements MetricsGatherer<Map<String,ArrayList<VisibilityMetric>>> {
  static final String KEY_HASH_ALGORITHM = "SHA-256";

  protected Map<String,AtomicLongMap<String>> metric;
  protected Map<String,AtomicLongMap<String>> blocks;
  protected ArrayList<Long> numEntries;
  protected ArrayList<Integer> numBlocks;
  private ArrayList<String> inBlock;
  protected ArrayList<String> localityGroups;
  private int numLG;
  private Map<String,ArrayList<ByteSequence>> localityGroupCF;

  public VisMetricsGatherer() {
    metric = new HashMap<>();
    blocks = new HashMap<>();
    numEntries = new ArrayList<>();
    numBlocks = new ArrayList<>();
    inBlock = new ArrayList<>();
    localityGroups = new ArrayList<>();
    numLG = 0;
  }

  @Override
  public void init(Map<String,ArrayList<ByteSequence>> cf) {
    localityGroupCF = cf;
  }

  @Override
  public void startLocalityGroup(Text oneCF) {
    String name = null;
    ByteSequence cf = new ArrayByteSequence(oneCF.toString());
    for (Entry<String,ArrayList<ByteSequence>> entry : localityGroupCF.entrySet()) {
      if (entry.getValue().contains(cf)) {
        if (entry.getKey() == null)
          name = null;
        else
          name = entry.getKey().toString();
        break;
      }
    }
    localityGroups.add(name);
    metric.put(name, AtomicLongMap.create(new HashMap<>()));
    blocks.put(name, AtomicLongMap.create(new HashMap<>()));
    numLG++;
    numEntries.add((long) 0);
    numBlocks.add(0);
  }

  @Override
  public void addMetric(Key key, Value val) {
    String myMetric = key.getColumnVisibility().toString();
    String currLG = localityGroups.get(numLG - 1);
    if (metric.get(currLG).containsKey(myMetric)) {
      metric.get(currLG).getAndIncrement(myMetric);
    } else
      metric.get(currLG).put(myMetric, 1);

    numEntries.set(numLG - 1, numEntries.get(numLG - 1) + 1);

    if (!inBlock.contains(myMetric) && blocks.get(currLG).containsKey(myMetric)) {
      blocks.get(currLG).incrementAndGet(myMetric);
      inBlock.add(myMetric);
    } else if (!inBlock.contains(myMetric) && !blocks.get(currLG).containsKey(myMetric)) {
      blocks.get(currLG).put(myMetric, 1);
      inBlock.add(myMetric);
    }

  }

  @Override
  public void startBlock() {
    inBlock.clear();
    numBlocks.set(numLG - 1, numBlocks.get(numLG - 1) + 1);
  }

  @Override
  public void printMetrics(boolean hash, String metricWord, PrintStream out) {
    for (int i = 0; i < numLG; i++) {
      String lGName = localityGroups.get(i);
      out.print("Locality Group: ");
      if (lGName == null)
        out.println("<DEFAULT>");
      else
        out.println(localityGroups.get(i));
      out.printf("%-27s", metricWord);
      out.println("Number of keys" + "\t   " + "Percent of keys" + "\t" + "Number of blocks" + "\t"
          + "Percent of blocks");
      for (Entry<String,Long> entry : metric.get(lGName).asMap().entrySet()) {
        if (hash) {
          String encodedKey = "";
          try {
            byte[] encodedBytes = MessageDigest.getInstance(KEY_HASH_ALGORITHM)
                .digest(entry.getKey().getBytes(UTF_8));
            encodedKey = new String(encodedBytes, UTF_8);
          } catch (NoSuchAlgorithmException e) {
            out.println(
                "Failed to convert key to " + KEY_HASH_ALGORITHM + " hash: " + e.getMessage());
          }
          out.printf("%-20s", encodedKey.substring(0, 8));
        } else
          out.printf("%-20s", entry.getKey());
        out.print("\t\t" + entry.getValue() + "\t\t\t");
        out.printf("%.2f", ((double) entry.getValue() / numEntries.get(i)) * 100);
        out.print("%\t\t\t");

        long blocksIn = blocks.get(lGName).get(entry.getKey());

        out.print(blocksIn + "\t\t   ");
        out.printf("%.2f", ((double) blocksIn / numBlocks.get(i)) * 100);
        out.print("%");

        out.println("");
      }
      out.println("Number of keys: " + numEntries.get(i));
      out.println();
    }
  }

  @Override
  public Map<String,ArrayList<VisibilityMetric>> getMetrics() {
    Map<String,ArrayList<VisibilityMetric>> getMetrics = new HashMap<>();
    for (int i = 0; i < numLG; i++) {
      String lGName = localityGroups.get(i);
      ArrayList<VisibilityMetric> rows = new ArrayList<>();
      for (Entry<String,Long> entry : metric.get(lGName).asMap().entrySet()) {
        long vis = entry.getValue();
        double visPer = ((double) entry.getValue() / numEntries.get(i)) * 100;

        long blocksIn = blocks.get(lGName).get(entry.getKey());
        double blocksPer = ((double) blocksIn / numBlocks.get(i)) * 100;

        rows.add(new VisibilityMetric(entry.getKey(), vis, visPer, blocksIn, blocksPer));
      }
      getMetrics.put(lGName, rows);
    }
    return getMetrics;
  }

}
