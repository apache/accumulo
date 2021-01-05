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
package org.apache.accumulo.test.functional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.test.categories.MiniClusterOnlyTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.io.BaseEncoding;

@Category(MiniClusterOnlyTests.class)
public class ZooMutatorIT extends AccumuloClusterHarness {
  /**
   * A simple stress test that looks for race conditions in
   * {@link ZooReaderWriter#mutateOrCreate(String, byte[], org.apache.accumulo.fate.zookeeper.ZooReaderWriter.Mutator)}
   */
  @Test
  public void concurrentMutatorTest() throws Exception {
    try (var client = Accumulo.newClient().from(getClientProps()).build();
        var context = (ClientContext) client) {
      String secret = cluster.getSiteConfiguration().get(Property.INSTANCE_SECRET);

      ZooReaderWriter zk = new ZooReaderWriter(context.getZooKeepers(),
          context.getZooKeepersSessionTimeOut(), secret);

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
              assertTrue("nextCount <= count " + nextCount + " " + count, nextCount > count);
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

      System.out.println(initialData);
      System.out.println(expected);

      assertEquals(settledCount + 1, countCounts.size());
      assertEquals(expected, new String(actual, UTF_8));
    }
  }

  private String hash(String data) {
    MessageDigest md;
    try {
      md = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
    return BaseEncoding.base16().encode(md.digest(data.getBytes(UTF_8)));
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
