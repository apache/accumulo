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
package org.apache.accumulo.core.file.rfile.bcfile.codec;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.file.rfile.bcfile.Compression;
import org.apache.accumulo.core.file.rfile.bcfile.Compression.Algorithm;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CompressorFactoryTest {

  HashMap<Compression.Algorithm,Boolean> isSupported = new HashMap<Compression.Algorithm,Boolean>();

  @Before
  public void testSupport() {
    // we can safely assert that GZ exists by virtue of it being the DefaultCodec
    isSupported.put(Compression.Algorithm.GZ, true);

    Configuration myConf = new Configuration();

    String extClazz = System.getProperty(Compression.Algorithm.CONF_LZO_CLASS);
    String clazz = (extClazz != null) ? extClazz : "org.apache.hadoop.io.compress.LzoCodec";
    try {
      CompressionCodec codec =
          (CompressionCodec) ReflectionUtils.newInstance(Class.forName(clazz), myConf);

      Assert.assertNotNull(codec);
      isSupported.put(Compression.Algorithm.LZO, true);

    } catch (ClassNotFoundException e) {
      // that is okay
    }

  }

  @Test
  public void testAlgoreithms() throws IOException {
    CompressorFactory factory = new DefaultCompressorFactory(DefaultConfiguration.getInstance());
    for (final Algorithm al : Algorithm.values()) {
      if (isSupported.get(al) != null && isSupported.get(al) == true) {

        Compressor compressor = factory.getCompressor(al);
        Assert.assertNotNull(compressor);
        factory.releaseCompressor(al, compressor);

        Decompressor decompressor = factory.getDecompressor(al);
        Assert.assertNotNull(decompressor);
        factory.releaseDecompressor(al, decompressor);
      }
    }
  }

  @Test
  public void testMultipleNotTheSameCompressors() throws IOException {
    CompressorFactory factory = new DefaultCompressorFactory(DefaultConfiguration.getInstance());
    for (final Algorithm al : Algorithm.values()) {
      if (isSupported.get(al) != null && isSupported.get(al) == true) {

        Set<Integer> compressorHashCodes = new HashSet<>();
        ArrayList<Compressor> compressors = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
          Compressor compressor = factory.getCompressor(al);
          Assert.assertNotNull(compressor);
          compressors.add(compressor);
          compressorHashCodes.add(Integer.valueOf(System.identityHashCode(compressor)));
        }

        // assert that we have 25 with this particular factory.
        Assert.assertEquals(25, compressorHashCodes.size());

        // free them for posterity sake
        for (Compressor compressor : compressors) {
          factory.releaseCompressor(al, compressor);
        }
      }
    }

  }

  @Test
  public void testMultipleNotTheSameDecompressors() throws IOException {
    CompressorFactory factory = new DefaultCompressorFactory(DefaultConfiguration.getInstance());
    for (final Algorithm al : Algorithm.values()) {
      if (isSupported.get(al) != null && isSupported.get(al) == true) {

        Set<Integer> compressorHashCodes = new HashSet<>();
        ArrayList<Decompressor> decompressors = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
          Decompressor decompressor = factory.getDecompressor(al);
          Assert.assertNotNull(decompressor);
          decompressors.add(decompressor);
          compressorHashCodes.add(Integer.valueOf(System.identityHashCode(decompressor)));
        }

        // assert that we have 25 with this particular factory.
        Assert.assertEquals(25, compressorHashCodes.size());

        // free them for posterity sake
        for (Decompressor decompressor : decompressors) {
          factory.releaseDecompressor(al, decompressor);
        }
      }
    }

  }

  @Test
  public void returnNull() {

    CompressorFactory factory = new DefaultCompressorFactory(DefaultConfiguration.getInstance());
    for (final Algorithm al : Algorithm.values()) {
      if (isSupported.get(al) != null && isSupported.get(al) == true) {
        try {
          factory.releaseCompressor(null, null);
          fail("Should have caught null when passing null algorithm");
        } catch (NullPointerException npe) {

        }

        try {
          factory.releaseCompressor(al, null);
          fail("Should have caught null when passing null compressor");
        } catch (NullPointerException npe) {

        }

        try {
          factory.releaseDecompressor(null, null);
          fail("Should have caught null when passing null algorithm");
        } catch (NullPointerException npe) {

        }

        try {
          factory.releaseDecompressor(al, null);
          fail("Should have caught null when passing null decompressor");
        } catch (NullPointerException npe) {

        }
      }
    }
  }

}
