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
package org.apache.accumulo.test.fate.zookeeper;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.harness.AccumuloITBase.ZOOKEEPER_TESTING_SERVER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.harness.WithTestNames;
import org.apache.accumulo.test.zookeeper.ZooKeeperTestingServer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.google.common.hash.Hashing;

@Tag(ZOOKEEPER_TESTING_SERVER)
public class ZooMutatorIT extends WithTestNames {

  @TempDir
  private static File tempDir;

  /**
   * This test uses multiple threads to update the data in a single zookeeper node using
   * {@link ZooReaderWriter#mutateOrCreate(String, byte[], ZooReaderWriter.Mutator)} and tries to
   * detect errors and race conditions in that code. Each thread uses {@link #nextValue(String)} to
   * compute a new value for the ZK node based on the current value, producing a new unique value.
   * Its expected that multiple threads calling {@link #nextValue(String)} as previously described
   * should yield the same final value as a single thread repeatedly calling
   * {@link #nextValue(String)} the same number of times. There are many things that can go wrong in
   * the multithreaded case. This test tries to ensure the following are true for the multithreaded
   * case.
   *
   * <ul>
   * <li>All expected updates are made, none were skipped.
   * <li>No updates are made twice. For example if two threads wrote the exact same value to the
   * node this should be detected by the test. Would expect each update to be unique.
   * <li>The updates are made in the same order as a single thread repeatedly calling
   * {@link #nextValue(String)}.
   * </ul>
   *
   * <p>
   * If any of the expectations above are not met it should cause the hash, count, and/or count
   * tracking done in the test to not match the what is computed by the single threaded code at the
   * end of the test.
   *
   * <p>
   * A hash and a counter are stored in ZK. The hashes form a chain of hashes as each new value is
   * written because its a hash of the previous value. The chain of hashes is useful for detecting
   * missing and out of order updates, but not duplicates. The counter and associated map that
   * tracks which counts were seen is useful for detecting missing and duplicate updates. The
   * counter is also used to weakly check for out of order updates, but the chain of hashes provides
   * a much strong check for this.
   */
  @Test
  public void concurrentMutatorTest() throws Exception {
    File newFolder = new File(tempDir, testName() + "/");
    assertTrue(newFolder.isDirectory() || newFolder.mkdir(), "failed to create dir: " + newFolder);
    try (ZooKeeperTestingServer szk = new ZooKeeperTestingServer(newFolder)) {
      szk.initPaths("/accumulo/" + InstanceId.of(UUID.randomUUID()));
      ZooReaderWriter zk = szk.getZooReaderWriter();

      var executor = Executors.newFixedThreadPool(16);

      String initialData = hash("Accumulo Zookeeper Mutator test data") + " 0";

      List<Future<?>> futures = new ArrayList<>();

      // This map is used to ensure multiple threads do not successfully write the same value and no
      // values are skipped. The hash in the value also verifies similar things in a different way.
      ConcurrentHashMap<Integer,Integer> countCounts = new ConcurrentHashMap<>();

      for (int i = 0; i < 16; i++) {
        futures.add(executor.submit(() -> {
          try {

            int count = -1;
            while (count < 200) {
              byte[] val =
                  zk.mutateOrCreate("/test-zm", initialData.getBytes(UTF_8), this::nextValue);
              int nextCount = getCount(val);
              assertTrue(nextCount > count, "nextCount <= count " + nextCount + " " + count);
              count = nextCount;
              countCounts.merge(count, 1, Integer::sum);
            }

          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }));
      }

      // wait and check for errors in background threads
      for (Future<?> future : futures) {
        future.get();
      }
      executor.shutdown();

      byte[] actual = zk.getData("/test-zm");
      int settledCount = getCount(actual);

      assertTrue(settledCount >= 200);

      String expected = initialData;

      assertEquals(1, (int) countCounts.get(0));

      for (int i = 1; i <= settledCount; i++) {
        assertEquals(1, (int) countCounts.get(i));
        expected = nextValue(expected);
      }

      assertEquals(settledCount + 1, countCounts.size());
      assertEquals(expected, new String(actual, UTF_8));
    }
  }

  private String hash(String data) {
    return Hashing.sha256().hashString(data, UTF_8).toString();
  }

  private String nextValue(String currString) {
    String[] tokens = currString.split(" ");
    String currHash = tokens[0];
    int count = Integer.parseInt(tokens[1]);
    return (hash(currHash) + " " + (count + 1));
  }

  private byte[] nextValue(byte[] curr) {
    return nextValue(new String(curr, UTF_8)).getBytes(UTF_8);
  }

  private int getCount(byte[] val) {
    return Integer.parseInt(new String(val, UTF_8).split(" ")[1]);
  }
}
