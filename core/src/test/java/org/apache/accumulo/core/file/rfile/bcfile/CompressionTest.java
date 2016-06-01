/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.accumulo.core.file.rfile.bcfile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.HashSet;

import org.apache.accumulo.core.file.rfile.bcfile.Compression.Algorithm;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class CompressionTest {

  @Test
  public void testSingle() throws IOException {
    Assert.assertNotNull(Compression.Algorithm.GZ.getCodec());

    Assert.assertNotNull(Compression.Algorithm.GZ.getCodec());
  }

  @Test
  public void testManyStartNotNull() throws IOException {
    final CompressionCodec codec = Algorithm.GZ.getCodec();

    Assert.assertNotNull(codec);

    ExecutorService service = Executors.newFixedThreadPool(10);

    for (int i = 0; i < 30; i++) {
      service.submit(new Callable<Boolean>()

      {

        @Override
        public Boolean call() throws Exception {
          Assert.assertNotNull(Compression.Algorithm.GZ.getCodec());
          return true;
        }

      });
    }

    service.shutdown();

    Assert.assertNotNull(codec);

  }

  // don't start until we have created the codec
  @Test
  public void testManyDontStartUntilThread() throws IOException {

    ExecutorService service = Executors.newFixedThreadPool(10);

    for (int i = 0; i < 30; i++) {

      service.submit(new Callable<Boolean>() {

        @Override
        public Boolean call() throws Exception {
          Assert.assertNotNull(Compression.Algorithm.GZ.getCodec());
          return true;
        }

      });
    }

    service.shutdown();

  }

  // don't start until we have created the codec
  @Test
  public void testThereCanBeOnlyOne() throws IOException, InterruptedException {

    ExecutorService service = Executors.newFixedThreadPool(20);

    ArrayList<Callable<Boolean>> list = Lists.newArrayList();

    // keep track of the system's identity hashcodes.
    final HashSet<Integer> testSet = Sets.newHashSet();

    for (int i = 0; i < 40; i++) {
      list.add(new Callable<Boolean>() {

        @Override
        public Boolean call() throws Exception {
          CompressionCodec codec = Compression.Algorithm.GZ.getCodec();
          Assert.assertNotNull(codec);
          // add the identity hashcode to the set.
          testSet.add(System.identityHashCode(codec));
          return true;
        }
      });
    }

    service.invokeAll(list);
    // ensure that we
    Assert.assertEquals(1, testSet.size());
    service.shutdown();

  }

}
