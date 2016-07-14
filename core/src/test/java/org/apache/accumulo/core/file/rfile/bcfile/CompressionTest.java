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
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.file.rfile.bcfile.Compression.Algorithm;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CompressionTest {

  HashMap<Compression.Algorithm,Boolean> isSupported = new HashMap<>();

  @Before
  public void testSupport() {
    // we can safely assert that GZ exists by virtue of it being the DefaultCodec
    isSupported.put(Compression.Algorithm.GZ, true);

    Configuration myConf = new Configuration();

    String extClazz = System.getProperty(Compression.Algorithm.CONF_LZO_CLASS);
    String clazz = (extClazz != null) ? extClazz : "org.apache.hadoop.io.compress.LzoCodec";
    try {
      CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(Class.forName(clazz), myConf);

      Assert.assertNotNull(codec);
      isSupported.put(Compression.Algorithm.LZO, true);

    } catch (ClassNotFoundException e) {
      // that is okay
    }

    extClazz = System.getProperty(Compression.Algorithm.CONF_SNAPPY_CLASS);
    clazz = (extClazz != null) ? extClazz : "org.apache.hadoop.io.compress.SnappyCodec";
    try {
      CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(Class.forName(clazz), myConf);

      Assert.assertNotNull(codec);

      isSupported.put(Compression.Algorithm.SNAPPY, true);

    } catch (ClassNotFoundException e) {
      // that is okay
    }

  }

  @Test
  public void testSingle() throws IOException {

    for (final Algorithm al : Algorithm.values()) {
      if (isSupported.get(al) != null && isSupported.get(al) == true) {

        // first call to issupported should be true
        Assert.assertTrue(al + " is not supported, but should be", al.isSupported());

        Assert.assertNotNull(al + " should have a non-null codec", al.getCodec());

        Assert.assertNotNull(al + " should have a non-null codec", al.getCodec());
      }
    }
  }

  @Test
  public void testSingleNoSideEffect() throws IOException {

    for (final Algorithm al : Algorithm.values()) {
      if (isSupported.get(al) != null && isSupported.get(al) == true) {

        Assert.assertTrue(al + " is not supported, but should be", al.isSupported());

        Assert.assertNotNull(al + " should have a non-null codec", al.getCodec());

        // assert that additional calls to create will not create
        // additional codecs

        Assert.assertNotEquals(al + " should have created a new codec, but did not", System.identityHashCode(al.getCodec()), al.createNewCodec(88 * 1024));
      }
    }
  }

  @Test(timeout = 60 * 1000)
  public void testManyStartNotNull() throws IOException, InterruptedException, ExecutionException {

    for (final Algorithm al : Algorithm.values()) {
      if (isSupported.get(al) != null && isSupported.get(al) == true) {

        // first call to issupported should be true
        Assert.assertTrue(al + " is not supported, but should be", al.isSupported());

        final CompressionCodec codec = al.getCodec();

        Assert.assertNotNull(al + " should not be null", codec);

        ExecutorService service = Executors.newFixedThreadPool(10);

        ArrayList<Future<Boolean>> results = new ArrayList<>();

        for (int i = 0; i < 30; i++) {
          results.add(service.submit(new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {
              Assert.assertNotNull(al + " should not be null", al.getCodec());
              return true;
            }

          }));
        }

        service.shutdown();

        Assert.assertNotNull(al + " should not be null", codec);

        while (!service.awaitTermination(1, TimeUnit.SECONDS)) {
          // wait
        }

        for (Future<Boolean> result : results) {
          Assert.assertTrue(al + " resulted in a failed call to getcodec within the thread pool", result.get());
        }
      }
    }

  }

  // don't start until we have created the codec
  @Test(timeout = 60 * 1000)
  public void testManyDontStartUntilThread() throws IOException, InterruptedException, ExecutionException {

    for (final Algorithm al : Algorithm.values()) {
      if (isSupported.get(al) != null && isSupported.get(al) == true) {

        // first call to issupported should be true
        Assert.assertTrue(al + " is not supported, but should be", al.isSupported());

        ExecutorService service = Executors.newFixedThreadPool(10);

        ArrayList<Future<Boolean>> results = new ArrayList<>();

        for (int i = 0; i < 30; i++) {

          results.add(service.submit(new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {
              Assert.assertNotNull(al + " should have a non-null codec", al.getCodec());
              return true;
            }

          }));
        }

        service.shutdown();

        while (!service.awaitTermination(1, TimeUnit.SECONDS)) {
          // wait
        }

        for (Future<Boolean> result : results) {
          Assert.assertTrue(al + " resulted in a failed call to getcodec within the thread pool", result.get());
        }
      }
    }

  }

  @Test(timeout = 60 * 1000)
  public void testThereCanBeOnlyOne() throws IOException, InterruptedException, ExecutionException {

    for (final Algorithm al : Algorithm.values()) {
      if (isSupported.get(al) != null && isSupported.get(al) == true) {

        // first call to issupported should be true
        Assert.assertTrue(al + " is not supported, but should be", al.isSupported());

        ExecutorService service = Executors.newFixedThreadPool(20);

        ArrayList<Callable<Boolean>> list = new ArrayList<>();

        ArrayList<Future<Boolean>> results = new ArrayList<>();

        // keep track of the system's identity hashcodes.
        final HashSet<Integer> testSet = new HashSet<>();

        for (int i = 0; i < 40; i++) {
          list.add(new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {
              CompressionCodec codec = al.getCodec();
              Assert.assertNotNull(al + " resulted in a non-null codec", codec);
              // add the identity hashcode to the set.
              synchronized (testSet) {
                testSet.add(System.identityHashCode(codec));
              }
              return true;
            }
          });
        }

        results.addAll(service.invokeAll(list));
        // ensure that we
        Assert.assertEquals(al + " created too many codecs", 1, testSet.size());
        service.shutdown();

        while (!service.awaitTermination(1, TimeUnit.SECONDS)) {
          // wait
        }

        for (Future<Boolean> result : results) {
          Assert.assertTrue(al + " resulted in a failed call to getcodec within the thread pool", result.get());
        }
      }
    }
  }

}
