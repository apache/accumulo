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
package org.apache.accumulo.server.test.performance.metadata;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.TabletLocator;
import org.apache.accumulo.core.client.impl.TabletLocatorImpl;
import org.apache.accumulo.core.client.impl.Trie;
import org.apache.accumulo.core.client.impl.TabletLocator.TabletLocation;
import org.apache.accumulo.core.client.impl.TabletLocator.TabletServerMutations;
import org.apache.accumulo.core.client.impl.TabletLocatorImpl.TabletLocationObtainer;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;

/**
 * This class test the performance of binning mutations.
 */

public class MBinner {
  
  static interface DataGenerator {
    Text genRandomRow(Random r, int rowLen);
    
    List<TabletLocation> genSplits(int numTablets, int rowLen);
  }
  
  static class BinaryGenerator implements DataGenerator {
    public Text genRandomRow(Random r, int rowLen) {
      byte rba[] = new byte[rowLen];
      r.nextBytes(rba);
      rba[0] = (byte) (0x7f & rba[0]);
      return new Text(rba);
    }
    
    public List<TabletLocation> genSplits(int numTablets, int rowLen) {
      byte maxValue[] = new byte[rowLen];
      maxValue[0] = 0x7f;
      for (int i = 1; i < maxValue.length; i++) {
        maxValue[i] = (byte) 0xff;
      }
      
      BigInteger max = new BigInteger(maxValue);
      BigInteger splitDistance = max.divide(new BigInteger(numTablets + ""));
      BigInteger split = splitDistance;
      
      List<TabletLocation> locations = new ArrayList<TabletLocation>(numTablets);
      
      Text prevRow = null;
      
      for (int i = 0; i < numTablets - 1; i++) {
        byte[] rowBytes = new byte[rowLen];
        byte[] splitBytes = split.toByteArray();
        
        int diff = rowBytes.length - splitBytes.length;
        for (int j = 0; j < diff; j++)
          rowBytes[j] = 0;
        System.arraycopy(splitBytes, 0, rowBytes, diff, splitBytes.length);
        
        Text currRow = new Text(rowBytes);
        split = split.add(splitDistance);
        
        locations.add(new TabletLocation(new KeyExtent(new Text("0"), currRow, prevRow), "192.168.1." + (i % 256) + ":9997"));
        prevRow = currRow;
      }
      
      locations.add(new TabletLocation(new KeyExtent(new Text("0"), null, prevRow), "192.168.1.1:9997"));
      
      return locations;
    }
  }
  
  static class HexGenerator implements DataGenerator {
    
    @Override
    public Text genRandomRow(Random r, int rowLen) {
      return new Text(String.format("%016x", Math.abs(r.nextLong())));
    }
    
    @Override
    public List<TabletLocation> genSplits(int numTablets, int rowLen) {
      long max = Long.MAX_VALUE;
      long splitDistance = max / numTablets;
      long split = splitDistance;
      
      List<TabletLocation> locations = new ArrayList<TabletLocation>(numTablets);
      
      Text prevRow = null;
      
      for (int i = 0; i < numTablets - 1; i++) {
        
        Text currRow = new Text(String.format("%016x", split));
        split += splitDistance;
        
        locations.add(new TabletLocation(new KeyExtent(new Text("0"), currRow, prevRow), "192.168.1." + (i % 256) + ":9997"));
        prevRow = currRow;
      }
      
      locations.add(new TabletLocation(new KeyExtent(new Text("0"), null, prevRow), "192.168.1.1:9997"));
      
      return locations;
    }
    
  }
  
  static class BinTask implements Runnable {
    
    private TabletLocator locator;
    private int rowLen;
    private DataGenerator dg;
    
    public BinTask(TabletLocator tli, int rowLen, DataGenerator dg) {
      this.locator = tli;
      this.rowLen = rowLen;
      this.dg = dg;
    }
    
    @Override
    public void run() {
      
      ArrayList<Mutation> muts = new ArrayList<Mutation>();
      
      Random r = new Random();
      
      for (int i = 0; i < 100000; i++) {
        Mutation m = new Mutation(dg.genRandomRow(r, rowLen));
        muts.add(m);
      }
      
      Map<String,TabletServerMutations> bm = new HashMap<String,TabletServerMutations>();
      List<Mutation> failures = new ArrayList<Mutation>();
      
      while (true) {
        
        try {
          
          long t1 = System.currentTimeMillis();
          locator.binMutations(muts, bm, failures);
          long t2 = System.currentTimeMillis();
          
          int extents = 0;
          
          for (TabletServerMutations tsm : bm.values()) {
            extents += tsm.getMutations().size();
          }
          
          double rate = 100000 / ((t2 - t1) / 1000.0);
          System.out.printf("%d %,6.2f %,d %,d\n", Thread.currentThread().getId(), rate, bm.size(), extents);
          
          bm.clear();
          failures.clear();
          
        } catch (Exception e) {
          e.printStackTrace();
          return;
        }
      }
    }
    
  }
  
  static class SimpleLocationObtainer implements TabletLocationObtainer {
    
    int numTablets;
    int rowLen;
    private DataGenerator dg;
    
    public SimpleLocationObtainer(int nt, int rowLen, DataGenerator dg) {
      this.numTablets = nt;
      this.rowLen = rowLen;
      this.dg = dg;
    }
    
    @Override
    public List<TabletLocation> lookupTablet(TabletLocation src, Text row, Text stopRow, TabletLocator parent) throws AccumuloSecurityException,
        AccumuloException {
      return dg.genSplits(numTablets, rowLen);
    }
    
    @Override
    public List<TabletLocation> lookupTablets(String tserver, Map<KeyExtent,List<Range>> map, TabletLocator parent) throws AccumuloSecurityException,
        AccumuloException {
      throw new UnsupportedOperationException();
    }
    
  }
  
  static class SimpleParent extends TabletLocator {
    @Override
    public void binMutations(List<Mutation> mutations, Map<String,TabletServerMutations> binnedMutations, List<Mutation> failures) throws AccumuloException,
        AccumuloSecurityException, TableNotFoundException {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public List<Range> binRanges(List<Range> ranges, Map<String,Map<KeyExtent,List<Range>>> binnedRanges) throws AccumuloException, AccumuloSecurityException,
        TableNotFoundException {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public void invalidateCache(KeyExtent failedExtent) {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public void invalidateCache(Collection<KeyExtent> keySet) {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public void invalidateCache() {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public void invalidateCache(String server) {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public TabletLocation locateTablet(Text row, boolean skipRow, boolean retry) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
      return new TabletLocation(new KeyExtent(new Text("!0"), null, null), "192.168.1.1:9997");
    }
  }
  
  static class TrieLocator extends TabletLocator {
    
    private Trie<TabletLocation> trie;
    
    TrieLocator(int maxDepth, SimpleLocationObtainer slo) {
      trie = new Trie<TabletLocation>(maxDepth);
      
      try {
        List<TabletLocation> locs = slo.lookupTablet(null, null, null, null);
        
        for (TabletLocation loc : locs) {
          if (loc.tablet_extent.getEndRow() != null) trie.add(loc.tablet_extent.getEndRow(), loc);
        }
        
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      
      // trie.printInfo();
    }
    
    @Override
    public void binMutations(List<Mutation> mutations, Map<String,TabletServerMutations> binnedMutations, List<Mutation> failures) throws AccumuloException,
        AccumuloSecurityException, TableNotFoundException {
      
      int notNull = 0;
      
      // Text row = new Text();
      for (Mutation mut : mutations) {
        // row.set(mut.getRow());
        TabletLocation tl = trie.ceiling(new ArrayByteSequence(mut.getRow(), 0, mut.getRow().length));
        /*
         * if(!tl.tablet_extent.contains(row)){ throw new RuntimeException(tl.tablet_extent+" does not contain "+row); }
         */
        if (tl != null) notNull++;
      }
      
      System.out.println("notNull " + notNull);
      
    }
    
    @Override
    public List<Range> binRanges(List<Range> ranges, Map<String,Map<KeyExtent,List<Range>>> binnedRanges) throws AccumuloException, AccumuloSecurityException,
        TableNotFoundException {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public void invalidateCache(KeyExtent failedExtent) {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public void invalidateCache(Collection<KeyExtent> keySet) {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public void invalidateCache() {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public void invalidateCache(String server) {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public TabletLocation locateTablet(Text row, boolean skipRow, boolean retry) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
      throw new UnsupportedOperationException();
    }
    
  }
  
  static class TreeLocator extends TabletLocator {
    
    private TreeMap<Text,TabletLocation> mc;
    
    TreeLocator(SimpleLocationObtainer slo) {
      mc = new TreeMap<Text,TabletLocation>();
      
      try {
        List<TabletLocation> locs = slo.lookupTablet(null, null, null, null);
        
        for (TabletLocation loc : locs) {
          if (loc.tablet_extent.getEndRow() != null) mc.put(loc.tablet_extent.getEndRow(), loc);
        }
        
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    
    @Override
    public void binMutations(List<Mutation> mutations, Map<String,TabletServerMutations> binnedMutations, List<Mutation> failures) throws AccumuloException,
        AccumuloSecurityException, TableNotFoundException {
      
      Text row = new Text();
      for (Mutation mut : mutations) {
        row.set(mut.getRow());
        mc.ceilingEntry(row);
      }
      
    }
    
    @Override
    public List<Range> binRanges(List<Range> ranges, Map<String,Map<KeyExtent,List<Range>>> binnedRanges) throws AccumuloException, AccumuloSecurityException,
        TableNotFoundException {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public void invalidateCache(KeyExtent failedExtent) {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public void invalidateCache(Collection<KeyExtent> keySet) {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public void invalidateCache() {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public void invalidateCache(String server) {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public TabletLocation locateTablet(Text row, boolean skipRow, boolean retry) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
      throw new UnsupportedOperationException();
    }
    
  }
  
  public static void main(String[] args) throws Exception {
    if (args.length != 6) {
      System.out.println("Usage " + MBinner.class.getName() + " <num threads> <num tablets> <row len> <trie depth> binary|hex trie|tree|system");
      return;
    }
    
    int numThreads = Integer.parseInt(args[0]);
    int numTablets = Integer.parseInt(args[1]);
    int rowLen = Integer.parseInt(args[2]);
    int trieDepth = Integer.parseInt(args[3]);
    
    DataGenerator generator;
    
    if (args[4].equals("binary")) generator = new BinaryGenerator();
    else if (args[4].equals("hex")) generator = new HexGenerator();
    else throw new IllegalArgumentException(args[4]);
    
    TabletLocator tli;
    
    if (args[5].equals("trie")) tli = new TrieLocator(trieDepth, new SimpleLocationObtainer(numTablets, rowLen, generator));
    else if (args[5].equals("tree")) tli = new TreeLocator(new SimpleLocationObtainer(numTablets, rowLen, generator));
    else if (args[5].equals("system")) {
      TabletLocator parent = new SimpleParent();
      tli = new TabletLocatorImpl(new Text("0"), parent, new SimpleLocationObtainer(numTablets, rowLen, generator));
    } else throw new IllegalArgumentException(args[5]);
    
    for (int i = 0; i < numThreads; i++) {
      new Thread(new BinTask(tli, rowLen, generator)).start();
    }
  }
}
