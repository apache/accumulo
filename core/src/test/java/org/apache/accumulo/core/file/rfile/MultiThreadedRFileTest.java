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
package org.apache.accumulo.core.file.rfile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.sample.Sampler;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.rfile.RFile.Reader;
import org.apache.accumulo.core.file.streams.PositionedOutputs;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.ColumnFamilySkippingIterator;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.sample.impl.SamplerFactory;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.NamingThreadFactory;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class MultiThreadedRFileTest {

  private static final Logger LOG = Logger.getLogger(MultiThreadedRFileTest.class);
  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<>();

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));

  private static void checkIndex(Reader reader) throws IOException {
    FileSKVIterator indexIter = reader.getIndex();

    if (indexIter.hasTop()) {
      Key lastKey = new Key(indexIter.getTopKey());

      if (reader.getFirstKey().compareTo(lastKey) > 0)
        throw new RuntimeException("First key out of order " + reader.getFirstKey() + " " + lastKey);

      indexIter.next();

      while (indexIter.hasTop()) {
        if (lastKey.compareTo(indexIter.getTopKey()) > 0)
          throw new RuntimeException("Indext out of order " + lastKey + " " + indexIter.getTopKey());

        lastKey = new Key(indexIter.getTopKey());
        indexIter.next();

      }

      if (!reader.getLastKey().equals(lastKey)) {
        throw new RuntimeException("Last key out of order " + reader.getLastKey() + " " + lastKey);
      }
    }
  }

  public static class TestRFile {

    private Configuration conf = CachedConfiguration.getInstance();
    public RFile.Writer writer;
    private FSDataOutputStream dos;
    private AccumuloConfiguration accumuloConfiguration;
    public Reader reader;
    public SortedKeyValueIterator<Key,Value> iter;
    public File rfile = null;
    public boolean deepCopy = false;

    public TestRFile(AccumuloConfiguration accumuloConfiguration) {
      this.accumuloConfiguration = accumuloConfiguration;
      if (this.accumuloConfiguration == null)
        this.accumuloConfiguration = DefaultConfiguration.getInstance();
    }

    public void close() throws IOException {
      if (rfile != null) {
        FileSystem fs = FileSystem.newInstance(conf);
        Path path = new Path("file://" + rfile.toString());
        fs.delete(path, false);
      }
    }

    public TestRFile deepCopy() throws IOException {
      TestRFile copy = new TestRFile(accumuloConfiguration);
      // does not copy any writer resources. This would be for read only.
      copy.reader = (Reader) reader.deepCopy(null);
      copy.rfile = rfile;
      copy.iter = new ColumnFamilySkippingIterator(copy.reader);
      copy.deepCopy = true;

      checkIndex(copy.reader);
      return copy;
    }

    public void openWriter(boolean startDLG) throws IOException {
      if (deepCopy) {
        throw new IOException("Cannot open writer on a deep copy");
      }
      if (rfile == null) {
        rfile = File.createTempFile("TestRFile", ".rf");
      }
      FileSystem fs = FileSystem.newInstance(conf);
      Path path = new Path("file://" + rfile.toString());
      dos = fs.create(path, true);
      CachableBlockFile.Writer _cbw = new CachableBlockFile.Writer(PositionedOutputs.wrap(dos), "gz", conf, accumuloConfiguration);
      SamplerConfigurationImpl samplerConfig = SamplerConfigurationImpl.newSamplerConfig(accumuloConfiguration);
      Sampler sampler = null;
      if (samplerConfig != null) {
        sampler = SamplerFactory.newSampler(samplerConfig, accumuloConfiguration);
      }
      writer = new RFile.Writer(_cbw, 1000, 1000, samplerConfig, sampler);

      if (startDLG)
        writer.startDefaultLocalityGroup();
    }

    public void openWriter() throws IOException {
      openWriter(true);
    }

    public void closeWriter() throws IOException {
      if (deepCopy) {
        throw new IOException("Cannot open writer on a deepcopy");
      }
      dos.flush();
      writer.close();
      dos.flush();
      dos.close();
    }

    public void openReader() throws IOException {
      FileSystem fs = FileSystem.newInstance(conf);
      Path path = new Path("file://" + rfile.toString());

      // the caches used to obfuscate the multithreaded issues
      CachableBlockFile.Reader _cbr = new CachableBlockFile.Reader(fs, path, conf, null, null, DefaultConfiguration.getInstance());
      reader = new RFile.Reader(_cbr);
      iter = new ColumnFamilySkippingIterator(reader);

      checkIndex(reader);
    }

    public void closeReader() throws IOException {
      reader.close();
    }

    public void seek(Key nk) throws IOException {
      iter.seek(new Range(nk, null), EMPTY_COL_FAMS, false);
    }
  }

  static Key newKey(String row, String cf, String cq, String cv, long ts) {
    return new Key(row.getBytes(), cf.getBytes(), cq.getBytes(), cv.getBytes(), ts);
  }

  static Value newValue(String val) {
    return new Value(val.getBytes());
  }

  public AccumuloConfiguration conf = null;

  @Test
  public void testMultipleReaders() throws IOException {
    final List<Throwable> threadExceptions = Collections.synchronizedList(new ArrayList<Throwable>());
    Map<String,MutableInt> messages = new HashMap<>();
    Map<String,String> stackTrace = new HashMap<>();

    final TestRFile trfBase = new TestRFile(conf);

    writeData(trfBase);

    trfBase.openReader();

    try {

      validate(trfBase);

      final TestRFile trfBaseCopy = trfBase.deepCopy();

      validate(trfBaseCopy);

      // now start up multiple RFile deepcopies
      int maxThreads = 10;
      String name = "MultiThreadedRFileTestThread";
      ThreadPoolExecutor pool = new ThreadPoolExecutor(maxThreads + 1, maxThreads + 1, 5 * 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
          new NamingThreadFactory(name));
      pool.allowCoreThreadTimeOut(true);
      try {
        Runnable runnable = new Runnable() {
          @Override
          public void run() {
            try {
              TestRFile trf = trfBase;
              synchronized (trfBaseCopy) {
                trf = trfBaseCopy.deepCopy();
              }
              validate(trf);
            } catch (Throwable t) {
              threadExceptions.add(t);
            }
          }
        };
        for (int i = 0; i < maxThreads; i++) {
          pool.submit(runnable);
        }
      } finally {
        pool.shutdown();
        try {
          pool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      for (Throwable t : threadExceptions) {
        String msg = t.getClass() + " : " + t.getMessage();
        if (!messages.containsKey(msg)) {
          messages.put(msg, new MutableInt(1));
        } else {
          messages.get(msg).increment();
        }
        StringWriter string = new StringWriter();
        PrintWriter writer = new PrintWriter(string);
        t.printStackTrace(writer);
        writer.flush();
        stackTrace.put(msg, string.getBuffer().toString());
      }
    } finally {
      trfBase.closeReader();
      trfBase.close();
    }

    for (String message : messages.keySet()) {
      LOG.error(messages.get(message) + ": " + message);
      LOG.error(stackTrace.get(message));
    }

    assertTrue(threadExceptions.isEmpty());
  }

  private void validate(TestRFile trf) throws IOException {
    Random random = new Random();
    for (int iteration = 0; iteration < 10; iteration++) {
      int part = random.nextInt(4);

      Range range = new Range(getKey(part, 0, 0), true, getKey(part, 4, 2048), true);
      trf.iter.seek(range, EMPTY_COL_FAMS, false);

      Key last = null;
      for (int locality = 0; locality < 4; locality++) {
        for (int i = 0; i < 2048; i++) {
          Key key = getKey(part, locality, i);
          Value value = getValue(i);
          assertTrue("No record found for row " + part + " locality " + locality + " index " + i, trf.iter.hasTop());
          assertEquals("Invalid key found for row " + part + " locality " + locality + " index " + i, key, trf.iter.getTopKey());
          assertEquals("Invalie value found for row " + part + " locality " + locality + " index " + i, value, trf.iter.getTopValue());
          last = trf.iter.getTopKey();
          trf.iter.next();
        }
      }
      if (trf.iter.hasTop()) {
        assertFalse("Found " + trf.iter.getTopKey() + " after " + last + " in " + range, trf.iter.hasTop());
      }

      range = new Range(getKey(4, 4, 0), true, null, true);
      trf.iter.seek(range, EMPTY_COL_FAMS, false);
      if (trf.iter.hasTop()) {
        assertFalse("Found " + trf.iter.getTopKey() + " in " + range, trf.iter.hasTop());
      }
    }

    Range range = new Range((Key) null, null);
    trf.iter.seek(range, EMPTY_COL_FAMS, false);

    Key last = null;
    for (int part = 0; part < 4; part++) {
      for (int locality = 0; locality < 4; locality++) {
        for (int i = 0; i < 2048; i++) {
          Key key = getKey(part, locality, i);
          Value value = getValue(i);
          assertTrue("No record found for row " + part + " locality " + locality + " index " + i, trf.iter.hasTop());
          assertEquals("Invalid key found for row " + part + " locality " + locality + " index " + i, key, trf.iter.getTopKey());
          assertEquals("Invalie value found for row " + part + " locality " + locality + " index " + i, value, trf.iter.getTopValue());
          last = trf.iter.getTopKey();
          trf.iter.next();
        }
      }
    }

    if (trf.iter.hasTop()) {
      assertFalse("Found " + trf.iter.getTopKey() + " after " + last + " in " + range, trf.iter.hasTop());
    }
  }

  private void writeData(TestRFile trfBase) throws IOException {
    trfBase.openWriter(false);

    try {
      for (int locality = 1; locality < 4; locality++) {
        trfBase.writer.startNewLocalityGroup("locality" + locality, Collections.singleton((ByteSequence) (new ArrayByteSequence(getCf(locality)))));
        for (int part = 0; part < 4; part++) {
          for (int i = 0; i < 2048; i++) {
            trfBase.writer.append(getKey(part, locality, i), getValue(i));
          }
        }
      }

      trfBase.writer.startDefaultLocalityGroup();
      for (int part = 0; part < 4; part++) {
        for (int i = 0; i < 2048; i++) {
          trfBase.writer.append(getKey(part, 0, i), getValue(i));
        }
      }
    } finally {
      trfBase.closeWriter();
    }
  }

  private Key getKey(int part, int locality, int index) {
    String row = "r000" + part;
    String cf = getCf(locality);
    String cq = "cq" + pad(index);

    return newKey(row, cf, cq, "", 1);
  }

  private String pad(int val) {
    String valStr = String.valueOf(val);
    switch (valStr.length()) {
      case 1:
        return "000" + valStr;
      case 2:
        return "00" + valStr;
      case 3:
        return "0" + valStr;
      default:
        return valStr;
    }
  }

  private Value getValue(int index) {
    return newValue("" + index);
  }

  private String getCf(int locality) {
    return "cf" + locality;
  }

}
