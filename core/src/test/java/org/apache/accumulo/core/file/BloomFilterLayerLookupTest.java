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
package org.apache.accumulo.core.file;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;

import org.apache.accumulo.core.WithTestNames;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.keyfunctor.ColumnFamilyFunctor;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.spi.crypto.NoCryptoServiceFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths not set by user input")
public class BloomFilterLayerLookupTest extends WithTestNames {

  private static final Logger log = LoggerFactory.getLogger(BloomFilterLayerLookupTest.class);
  private static final SecureRandom random = new SecureRandom();

  @TempDir
  private static File tempDir;

  @Test
  public void test() throws IOException {
    HashSet<Integer> valsSet = new HashSet<>();
    for (int i = 0; i < 100000; i++) {
      valsSet.add(random.nextInt(Integer.MAX_VALUE));
    }

    ArrayList<Integer> vals = new ArrayList<>(valsSet);
    Collections.sort(vals);

    ConfigurationCopy acuconf = new ConfigurationCopy(DefaultConfiguration.getInstance());
    acuconf.set(Property.TABLE_BLOOM_ENABLED, "true");
    acuconf.set(Property.TABLE_BLOOM_KEY_FUNCTOR, ColumnFamilyFunctor.class.getName());
    acuconf.set(Property.TABLE_FILE_TYPE, RFile.EXTENSION);
    acuconf.set(Property.TABLE_BLOOM_LOAD_THRESHOLD, "1");
    acuconf.set(Property.TSERV_BLOOM_LOAD_MAXCONCURRENT, "1");

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    // get output file name
    String suffix = FileOperations.getNewFileExtension(acuconf);
    String fname = new File(tempDir, testName() + "." + suffix).getAbsolutePath();
    FileSKVWriter bmfw = FileOperations.getInstance().newWriterBuilder()
        .forFile(fname, fs, conf, NoCryptoServiceFactory.NONE).withTableConfiguration(acuconf)
        .build();

    // write data to file
    long t1 = System.currentTimeMillis();
    bmfw.startDefaultLocalityGroup();
    for (Integer i : vals) {
      String fi = String.format("%010d", i);
      bmfw.append(new Key(new Text("r" + fi), new Text("cf1")), new Value("v" + fi));
      bmfw.append(new Key(new Text("r" + fi), new Text("cf2")), new Value("v" + fi));
    }
    long t2 = System.currentTimeMillis();

    log.debug(String.format("write rate %6.2f%n", vals.size() / ((t2 - t1) / 1000.0)));
    bmfw.close();

    t1 = System.currentTimeMillis();
    FileSKVIterator bmfr = FileOperations.getInstance().newReaderBuilder()
        .forFile(fname, fs, conf, NoCryptoServiceFactory.NONE).withTableConfiguration(acuconf)
        .build();
    t2 = System.currentTimeMillis();
    log.debug("Opened {} in {}", fname, (t2 - t1));

    int hits = 0;
    t1 = System.currentTimeMillis();
    for (int i = 0; i < 5000; i++) {
      int row = random.nextInt(Integer.MAX_VALUE);
      seek(bmfr, row);
      if (valsSet.contains(row)) {
        hits++;
        assertTrue(bmfr.hasTop());
      }
    }
    t2 = System.currentTimeMillis();

    double rate1 = 5000 / ((t2 - t1) / 1000.0);
    log.debug(String.format("random lookup rate : %6.2f%n", rate1));
    log.debug("hits = {}", hits);

    int count = 0;
    t1 = System.currentTimeMillis();
    for (Integer row : valsSet) {
      seek(bmfr, row);
      assertTrue(bmfr.hasTop());
      count++;
      if (count >= 500) {
        break;
      }
    }
    t2 = System.currentTimeMillis();

    double rate2 = 500 / ((t2 - t1) / 1000.0);
    log.debug(String.format("existing lookup rate %6.2f%n", rate2));
    log.debug("expected hits 500.  Receive hits: {}", count);
    bmfr.close();

    assertTrue(rate1 > rate2);
  }

  private void seek(FileSKVIterator bmfr, int row) throws IOException {
    String fi = String.format("%010d", row);
    // bmfr.seek(new Range(new Text("r"+fi)));
    Key k1 = new Key(new Text("r" + fi), new Text("cf1"));
    bmfr.seek(new Range(k1, true, k1.followingKey(PartialKey.ROW_COLFAM), false), new ArrayList<>(),
        false);
  }

}
