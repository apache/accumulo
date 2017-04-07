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
package org.apache.accumulo.test;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.OpTimer;
import org.apache.accumulo.tserver.NativeMap;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NativeMapStressTest {

  private static final Logger log = LoggerFactory.getLogger(NativeMapStressTest.class);

  public static void main(String[] args) {
    testLotsOfMapDeletes(true);
    testLotsOfMapDeletes(false);
    testLotsOfOverwrites();
    testLotsOfGetsAndScans();
  }

  private static void put(NativeMap nm, String row, String val, int mc) {
    Mutation m = new Mutation(new Text(row));
    m.put(new Text(), new Text(), Long.MAX_VALUE, new Value(val.getBytes(UTF_8)));
    nm.mutate(m, mc);
  }

  private static void testLotsOfGetsAndScans() {

    ArrayList<Thread> threads = new ArrayList<>();

    final int numThreads = 8;
    final int totalGets = 100000000;
    final int mapSizePerThread = (int) (4000000 / (double) numThreads);
    final int getsPerThread = (int) (totalGets / (double) numThreads);

    for (int tCount = 0; tCount < numThreads; tCount++) {
      Runnable r = new Runnable() {
        @Override
        public void run() {
          NativeMap nm = new NativeMap();

          Random r = new Random();

          OpTimer timer = null;
          AtomicLong nextOpid = new AtomicLong();

          if (log.isInfoEnabled()) {
            log.info("tid={} oid={} Creating map of size {}", Thread.currentThread().getId(), nextOpid.get(), mapSizePerThread);
            timer = new OpTimer().start();
          }

          for (int i = 0; i < mapSizePerThread; i++) {
            String row = String.format("r%08d", i);
            String val = row + "v";
            put(nm, row, val, i);
          }

          if (timer != null) {

            // stop and log created elapsed time
            timer.stop();
            log.info("tid={} oid={} Created map of size {} in {}", Thread.currentThread().getId(), nextOpid.getAndIncrement(), nm.size(),
                String.format("%.3f secs", timer.scale(TimeUnit.SECONDS)));

            // start timer for gets
            log.info("tid={} oid={} Doing {} gets()", Thread.currentThread().getId(), nextOpid.get(), getsPerThread);
            timer.reset().start();
          }

          for (int i = 0; i < getsPerThread; i++) {
            String row = String.format("r%08d", r.nextInt(mapSizePerThread));
            String val = row + "v";

            Value value = nm.get(new Key(new Text(row)));
            if (value == null || !value.toString().equals(val)) {
              log.error("nm.get({}) failed", row);
            }
          }

          if (timer != null) {

            // stop and log created elapsed time
            timer.stop();
            log.info("tid={} oid={} Finished {} gets in {}", Thread.currentThread().getId(), nextOpid.getAndIncrement(), getsPerThread,
                String.format("%.3f secs", timer.scale(TimeUnit.SECONDS)));

            // start timer for random iterations
            log.info("tid={} oid={} Doing {} random iterations", Thread.currentThread().getId(), nextOpid.get(), getsPerThread);
            timer.reset().start();
          }

          int scanned = 0;

          for (int i = 0; i < getsPerThread; i++) {
            int startRow = r.nextInt(mapSizePerThread);
            String row = String.format("r%08d", startRow);

            Iterator<Entry<Key,Value>> iter = nm.iterator(new Key(new Text(row)));

            int count = 0;

            while (iter.hasNext() && count < 10) {
              String row2 = String.format("r%08d", startRow + count);
              String val2 = row2 + "v";

              Entry<Key,Value> entry = iter.next();
              if (!entry.getValue().toString().equals(val2) || !entry.getKey().equals(new Key(new Text(row2)))) {
                log.error("nm.iter({}) failed row = {} count = {} row2 = {} val2 = {}", row2, row, count, row, val2);
              }

              count++;
            }

            scanned += count;
          }

          if (timer != null) {

            // stop and log created elapsed time
            timer.stop();
            log.info("tid={} oid={} Finished {}  random iterations (scanned = {}) in {}", Thread.currentThread().getId(), nextOpid.getAndIncrement(),
                getsPerThread, scanned, String.format("%.3f secs", timer.scale(TimeUnit.SECONDS)));

          }

          nm.delete();
        }
      };

      Thread t = new Thread(r);
      t.start();

      threads.add(t);
    }

    for (Thread thread : threads) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        log.error("Could not join thread '{}'.", thread.getName(), e);
        throw new RuntimeException(e);
      }
    }
  }

  private static void testLotsOfMapDeletes(final boolean doRemoves) {
    final int numThreads = 8;
    final int rowRange = 10000;
    final int mapsPerThread = 50;
    final int totalInserts = 100000000;
    final int insertsPerMapPerThread = (int) (totalInserts / (double) numThreads / mapsPerThread);

    System.out.println("insertsPerMapPerThread " + insertsPerMapPerThread);

    ArrayList<Thread> threads = new ArrayList<>();

    for (int i = 0; i < numThreads; i++) {
      Runnable r = new Runnable() {
        @Override
        public void run() {

          int inserts = 0;
          int removes = 0;

          for (int i = 0; i < mapsPerThread; i++) {

            NativeMap nm = new NativeMap();

            for (int j = 0; j < insertsPerMapPerThread; j++) {
              String row = String.format("r%08d", j % rowRange);
              String val = row + "v";
              put(nm, row, val, j);
              inserts++;
            }

            if (doRemoves) {
              Iterator<Entry<Key,Value>> iter = nm.iterator();
              while (iter.hasNext()) {
                iter.next();
                iter.remove();
                removes++;
              }
            }

            nm.delete();
          }

          System.out.println("inserts " + inserts + " removes " + removes + " " + Thread.currentThread().getName());
        }
      };

      Thread t = new Thread(r);
      t.start();

      threads.add(t);
    }

    for (Thread thread : threads) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        log.error("Could not join thread '{}'.", thread.getName(), e);
        throw new RuntimeException(e);
      }
    }
  }

  private static void testLotsOfOverwrites() {
    final Map<Integer,NativeMap> nativeMaps = new HashMap<>();

    int numThreads = 8;
    final int insertsPerThread = (int) (100000000 / (double) numThreads);
    final int rowRange = 10000;
    final int numMaps = 50;

    ArrayList<Thread> threads = new ArrayList<>();

    for (int i = 0; i < numThreads; i++) {
      Runnable r = new Runnable() {
        @Override
        public void run() {
          Random r = new Random();
          int inserts = 0;

          for (int i = 0; i < insertsPerThread / 100.0; i++) {
            int map = r.nextInt(numMaps);

            NativeMap nm;

            synchronized (nativeMaps) {
              nm = nativeMaps.get(map);
              if (nm == null) {
                nm = new NativeMap();
                nativeMaps.put(map, nm);

              }
            }

            synchronized (nm) {
              for (int j = 0; j < 100; j++) {
                String row = String.format("r%08d", r.nextInt(rowRange));
                String val = row + "v";
                put(nm, row, val, j);
                inserts++;
              }
            }
          }

          System.out.println("inserts " + inserts + " " + Thread.currentThread().getName());
        }
      };

      Thread t = new Thread(r);
      t.start();

      threads.add(t);
    }

    for (Thread thread : threads) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        log.error("Could not join thread '{}'.", thread.getName(), e);
        throw new RuntimeException(e);
      }
    }

    Set<Entry<Integer,NativeMap>> es = nativeMaps.entrySet();
    for (Entry<Integer,NativeMap> entry : es) {
      entry.getValue().delete();
    }
  }

}
