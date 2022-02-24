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
package org.apache.accumulo.core.file.rfile.bcfile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.spi.file.rfile.compression.Bzip2;
import org.apache.accumulo.core.spi.file.rfile.compression.Gz;
import org.apache.accumulo.core.spi.file.rfile.compression.Lz4;
import org.apache.accumulo.core.spi.file.rfile.compression.Lzo;
import org.apache.accumulo.core.spi.file.rfile.compression.Snappy;
import org.apache.accumulo.core.spi.file.rfile.compression.ZStandard;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class CompressionTest {

  HashMap<DefaultCompressionAlgorithm,Boolean> isSupported = new HashMap<>();

  @BeforeEach
  public void testSupport() throws ClassNotFoundException {
    Configuration myConf = new Configuration();

    Gz gz = new Gz();
    String extClazz = gz.getCodecClassNameProperty();
    String clazz = (extClazz != null) ? extClazz : gz.getCodecClassName();
    CompressionCodec codec =
        (CompressionCodec) ReflectionUtils.newInstance(Class.forName(clazz), myConf);

    assertNotNull(codec);
    isSupported.put(new DefaultCompressionAlgorithm(gz, myConf), true);

    Lzo lzo = new Lzo();
    extClazz = lzo.getCodecClassNameProperty();
    clazz = (extClazz != null) ? extClazz : lzo.getCodecClassName();
    try {
      codec = (CompressionCodec) ReflectionUtils.newInstance(Class.forName(clazz), myConf);

      assertNotNull(codec);
      isSupported.put(new DefaultCompressionAlgorithm(lzo, myConf), true);

    } catch (ClassNotFoundException e) {
      // that is okay
    }

    Lz4 lz4 = new Lz4();
    extClazz = lz4.getCodecClassNameProperty();
    clazz = (extClazz != null) ? extClazz : lz4.getCodecClassName();
    try {
      codec = (CompressionCodec) ReflectionUtils.newInstance(Class.forName(clazz), myConf);

      assertNotNull(codec);

      isSupported.put(new DefaultCompressionAlgorithm(lz4, myConf), true);

    } catch (ClassNotFoundException e) {
      // that is okay
    }

    Bzip2 bzip = new Bzip2();
    extClazz = bzip.getCodecClassNameProperty();
    clazz = (extClazz != null) ? extClazz : bzip.getCodecClassName();
    try {
      codec = (CompressionCodec) ReflectionUtils.newInstance(Class.forName(clazz), myConf);

      assertNotNull(codec);

      isSupported.put(new DefaultCompressionAlgorithm(bzip, myConf), true);

    } catch (ClassNotFoundException e) {
      // that is okay
    }

    Snappy snappy = new Snappy();
    extClazz = snappy.getCodecClassNameProperty();
    clazz = (extClazz != null) ? extClazz : snappy.getCodecClassName();
    try {
      codec = (CompressionCodec) ReflectionUtils.newInstance(Class.forName(clazz), myConf);

      assertNotNull(codec);

      isSupported.put(new DefaultCompressionAlgorithm(snappy, myConf), true);

    } catch (ClassNotFoundException e) {
      // that is okay
    }

    ZStandard zstd = new ZStandard();
    extClazz = zstd.getCodecClassNameProperty();
    clazz = (extClazz != null) ? extClazz : zstd.getCodecClassName();
    try {
      codec = (CompressionCodec) ReflectionUtils.newInstance(Class.forName(clazz), myConf);

      assertNotNull(codec);

      isSupported.put(new DefaultCompressionAlgorithm(zstd, myConf), true);

    } catch (ClassNotFoundException e) {
      // that is okay
    }

  }

  @Test
  public void testSingle() {

    for (final String name : Compression.getSupportedAlgorithms()) {
      DefaultCompressionAlgorithm al = Compression.getCompressionAlgorithmByName(name);
      if (isSupported.get(al) != null && isSupported.get(al)) {

        // first call to isSupported should be true
        assertTrue(al.isSupported(), al + " is not supported, but should be");

        assertNotNull(al.getCodec(), al + " should have a non-null codec");

        assertNotNull(al.getCodec(), al + " should have a non-null codec");
      }
    }
  }

  @Test
  public void testSingleNoSideEffect() {

    for (final String name : Compression.getSupportedAlgorithms()) {
      DefaultCompressionAlgorithm al = Compression.getCompressionAlgorithmByName(name);
      if (isSupported.get(al) != null && isSupported.get(al)) {

        assertTrue(al.isSupported(), al + " is not supported, but should be");

        assertNotNull(al.getCodec(), al + " should have a non-null codec");

        // assert that additional calls to create will not create
        // additional codecs

        assertNotEquals(System.identityHashCode(al.getCodec()), al.createNewCodec(88 * 1024),
            al + " should have created a new codec, but did not");
      }
    }
  }

  @Test
  @Timeout(60_000)
  public void testManyStartNotNull() throws InterruptedException, ExecutionException {

    for (final String name : Compression.getSupportedAlgorithms()) {
      DefaultCompressionAlgorithm al = Compression.getCompressionAlgorithmByName(name);
      if (isSupported.get(al) != null && isSupported.get(al)) {

        // first call to isSupported should be true
        assertTrue(al.isSupported(), al + " is not supported, but should be");

        final CompressionCodec codec = al.getCodec();

        assertNotNull(codec, al + " should not be null");

        ExecutorService service = Executors.newFixedThreadPool(10);

        ArrayList<Future<Boolean>> results = new ArrayList<>();

        for (int i = 0; i < 30; i++) {
          results.add(service.submit(() -> {
            assertNotNull(al.getCodec(), al + " should not be null");
            return true;
          }));
        }

        service.shutdown();

        assertNotNull(codec, al + " should not be null");

        while (!service.awaitTermination(1, TimeUnit.SECONDS)) {
          // wait
        }

        for (Future<Boolean> result : results) {
          assertTrue(result.get(),
              al + " resulted in a failed call to getcodec within the thread pool");
        }
      }
    }

  }

  // don't start until we have created the codec
  @Test
  @Timeout(60_000)
  public void testManyDontStartUntilThread() throws InterruptedException, ExecutionException {

    for (final String name : Compression.getSupportedAlgorithms()) {
      DefaultCompressionAlgorithm al = Compression.getCompressionAlgorithmByName(name);
      if (isSupported.get(al) != null && isSupported.get(al)) {

        // first call to isSupported should be true
        assertTrue(al.isSupported(), al + " is not supported, but should be");

        ExecutorService service = Executors.newFixedThreadPool(10);

        ArrayList<Future<Boolean>> results = new ArrayList<>();

        for (int i = 0; i < 30; i++) {

          results.add(service.submit(() -> {
            assertNotNull(al.getCodec(), al + " should have a non-null codec");
            return true;
          }));
        }

        service.shutdown();

        while (!service.awaitTermination(1, TimeUnit.SECONDS)) {
          // wait
        }

        for (Future<Boolean> result : results) {
          assertTrue(result.get(),
              al + " resulted in a failed call to getcodec within the thread pool");
        }
      }
    }

  }

  @Test
  @Timeout(60_000)
  public void testThereCanBeOnlyOne() throws InterruptedException, ExecutionException {

    for (final String name : Compression.getSupportedAlgorithms()) {
      DefaultCompressionAlgorithm al = Compression.getCompressionAlgorithmByName(name);
      if (isSupported.get(al) != null && isSupported.get(al)) {

        // first call to isSupported should be true
        assertTrue(al.isSupported(), al + " is not supported, but should be");

        ExecutorService service = Executors.newFixedThreadPool(20);

        ArrayList<Callable<Boolean>> list = new ArrayList<>();

        ArrayList<Future<Boolean>> results = new ArrayList<>();

        // keep track of the system's identity hashcodes.
        final HashSet<Integer> testSet = new HashSet<>();

        for (int i = 0; i < 40; i++) {
          list.add(() -> {
            CompressionCodec codec = al.getCodec();
            assertNotNull(codec, al + " resulted in a non-null codec");
            // add the identity hashcode to the set.
            synchronized (testSet) {
              testSet.add(System.identityHashCode(codec));
            }
            return true;
          });
        }

        results.addAll(service.invokeAll(list));
        // ensure that we
        assertEquals(1, testSet.size(), al + " created too many codecs");
        service.shutdown();

        while (!service.awaitTermination(1, TimeUnit.SECONDS)) {
          // wait
        }

        for (Future<Boolean> result : results) {
          assertTrue(result.get(),
              al + " resulted in a failed call to getcodec within the thread pool");
        }
      }
    }
  }

}
