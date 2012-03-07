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
package org.apache.accumulo.examples.simple.shard;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.IntersectingIterator;
import org.apache.hadoop.io.Text;

/**
 * Using the doc2word table created by Reverse.java, this program randomly selects N words per document. Then it continually queries a random set of words in
 * the shard table (created by {@link Index}) using the {@link IntersectingIterator}.
 * 
 * See docs/examples/README.shard for instructions.
 */

public class ContinuousQuery {
  public static void main(String[] args) throws Exception {
    
    if (args.length != 7 && args.length != 8) {
      System.err.println("Usage : " + ContinuousQuery.class.getName()
          + " <instance> <zoo keepers> <shard table> <doc2word table> <user> <pass> <num query terms> [iterations]");
      System.exit(-1);
    }
    
    String instance = args[0];
    String zooKeepers = args[1];
    String table = args[2];
    String docTable = args[3];
    String user = args[4];
    String pass = args[5];
    int numTerms = Integer.parseInt(args[6]);
    long iterations = Long.MAX_VALUE;
    if (args.length > 7)
      iterations = Long.parseLong(args[7]);
    
    ZooKeeperInstance zki = new ZooKeeperInstance(instance, zooKeepers);
    Connector conn = zki.getConnector(user, pass.getBytes());
    
    ArrayList<Text[]> randTerms = findRandomTerms(conn.createScanner(docTable, Constants.NO_AUTHS), numTerms);
    
    Random rand = new Random();
    
    BatchScanner bs = conn.createBatchScanner(table, Constants.NO_AUTHS, 20);
    
    for (long i = 0; i < iterations; i += 1) {
      Text[] columns = randTerms.get(rand.nextInt(randTerms.size()));
      
      bs.clearScanIterators();
      bs.clearColumns();
      
      IteratorSetting ii = new IteratorSetting(20, "ii", IntersectingIterator.class);
      IntersectingIterator.setColumnFamilies(ii, columns);
      bs.addScanIterator(ii);
      bs.setRanges(Collections.singleton(new Range()));
      
      long t1 = System.currentTimeMillis();
      int count = 0;
      for (@SuppressWarnings("unused")
      Entry<Key,Value> entry : bs) {
        count++;
      }
      long t2 = System.currentTimeMillis();
      
      System.out.printf("  %s %,d %6.3f\n", Arrays.asList(columns), count, (t2 - t1) / 1000.0);
    }
    
    bs.close();
    
  }
  
  private static ArrayList<Text[]> findRandomTerms(Scanner scanner, int numTerms) {
    
    Text currentRow = null;
    
    ArrayList<Text> words = new ArrayList<Text>();
    ArrayList<Text[]> ret = new ArrayList<Text[]>();
    
    Random rand = new Random();
    
    for (Entry<Key,Value> entry : scanner) {
      Key key = entry.getKey();
      
      if (currentRow == null)
        currentRow = key.getRow();
      
      if (!currentRow.equals(key.getRow())) {
        selectRandomWords(words, ret, rand, numTerms);
        words.clear();
        currentRow = key.getRow();
      }
      
      words.add(key.getColumnFamily());
      
    }
    
    selectRandomWords(words, ret, rand, numTerms);
    
    return ret;
  }
  
  private static void selectRandomWords(ArrayList<Text> words, ArrayList<Text[]> ret, Random rand, int numTerms) {
    if (words.size() >= numTerms) {
      Collections.shuffle(words, rand);
      Text docWords[] = new Text[numTerms];
      for (int i = 0; i < docWords.length; i++) {
        docWords[i] = words.get(i);
      }
      
      ret.add(docWords);
    }
  }
}
