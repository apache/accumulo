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
package org.apache.accumulo.core.file.rfile;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.accumulo.core.client.sample.Sampler;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.crypto.CryptoFactoryLoader;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile.CachableBuilder;
import org.apache.accumulo.core.file.rfile.RFile.Reader;
import org.apache.accumulo.core.file.rfile.bcfile.BCFile;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.ColumnFamilySkippingIterator;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.sample.impl.SamplerFactory;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths not set by user input")
public class MultiThreadedRFileTest {

  private static final SecureRandom random = new SecureRandom();
  private static final Logger LOG = LoggerFactory.getLogger(MultiThreadedRFileTest.class);
  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<>();

  private static void checkIndex(Reader reader) throws IOException {
    FileSKVIterator indexIter = reader.getIndex();

    if (indexIter.hasTop()) {
      Key lastKey = new Key(indexIter.getTopKey());

      if (reader.getFirstKey().compareTo(lastKey) > 0) {
        throw new RuntimeException(
            "First key out of order " + reader.getFirstKey() + " " + lastKey);
      }

      indexIter.next();

      while (indexIter.hasTop()) {
        if (lastKey.compareTo(indexIter.getTopKey()) > 0) {
          throw new RuntimeException(
              "Indext out of order " + lastKey + " " + indexIter.getTopKey());
        }

        lastKey = new Key(indexIter.getTopKey());
        indexIter.next();

      }

      if (!reader.getLastKey().equals(lastKey)) {
        throw new RuntimeException("Last key out of order " + reader.getLastKey() + " " + lastKey);
      }
    }
  }

  public static class TestRFile {

    private Configuration conf = new Configuration();
    public RFile.Writer writer;
    private FSDataOutputStream dos;
    private AccumuloConfiguration accumuloConfiguration;
    public Reader reader;
    public SortedKeyValueIterator<Key,Value> iter;
    public File rfile = null;
    public boolean deepCopy = false;

    public TestRFile(AccumuloConfiguration accumuloConfiguration) {
      this.accumuloConfiguration = accumuloConfiguration;
      if (this.accumuloConfiguration == null) {
        this.accumuloConfiguration = DefaultConfiguration.getInstance();
      }
    }

    public void close() throws IOException {
      if (rfile != null) {
        FileSystem fs = FileSystem.newInstance(conf);
        Path path = new Path("file://" + rfile);
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
      Path path = new Path("file://" + rfile);
      dos = fs.create(path, true);
      BCFile.Writer _cbw = new BCFile.Writer(dos, null, "gz", conf,
          CryptoFactoryLoader.getServiceForServer(accumuloConfiguration));
      SamplerConfigurationImpl samplerConfig =
          SamplerConfigurationImpl.newSamplerConfig(accumuloConfiguration);
      Sampler sampler = null;
      if (samplerConfig != null) {
        sampler = SamplerFactory.newSampler(samplerConfig, accumuloConfiguration);
      }
      writer = new RFile.Writer(_cbw, 1000, 1000, samplerConfig, sampler);

      if (startDLG) {
        writer.startDefaultLocalityGroup();
      }
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
      Path path = new Path("file://" + rfile);
      AccumuloConfiguration defaultConf = DefaultConfiguration.getInstance();

      // the caches used to obfuscate the multithreaded issues
      CachableBuilder b = new CachableBuilder().fsPath(fs, path).conf(conf)
          .cryptoService(CryptoFactoryLoader.getServiceForServer(defaultConf));
      reader = new RFile.Reader(new CachableBlockFile.Reader(b));
      iter = new ColumnFamilySkippingIterator(reader);

      checkIndex(reader);
    }

    public void closeReader() throws IOException {
      reader.close();
    }
  }

  static Key newKey(String row, String cf, String cq, String cv, long ts) {
    return new Key(row.getBytes(), cf.getBytes(), cq.getBytes(), cv.getBytes(), ts);
  }

  static Value newValue(String val) {
    return new Value(val);
  }

  public AccumuloConfiguration conf = null;

  @SuppressFBWarnings(value = "INFORMATION_EXPOSURE_THROUGH_AN_ERROR_MESSAGE",
      justification = "information put into error message is safe and used for testing")
  @Test
  public void testMultipleReaders() throws IOException {
    final List<Throwable> threadExceptions = Collections.synchronizedList(new ArrayList<>());
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
      ThreadPoolExecutor pool = ThreadPools.getServerThreadPools().createThreadPool(maxThreads + 1,
          maxThreads + 1, 5 * 60, SECONDS, name, false);
      try {
        Runnable runnable = () -> {
          try {
            TestRFile trf = trfBase;
            synchronized (trfBaseCopy) {
              trf = trfBaseCopy.deepCopy();
            }
            validate(trf);
          } catch (Throwable t) {
            threadExceptions.add(t);
          }
        };
        for (int i = 0; i < maxThreads; i++) {
          pool.execute(runnable);
        }
      } finally {
        pool.shutdown();
        try {
          pool.awaitTermination(Long.MAX_VALUE, MILLISECONDS);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      for (Throwable t : threadExceptions) {
        String msg = t.getClass() + " : " + t.getMessage();
        if (messages.containsKey(msg)) {
          messages.get(msg).increment();
        } else {
          messages.put(msg, new MutableInt(1));
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
      LOG.error("{}: {}", messages.get(message), message);
      LOG.error("{}", stackTrace.get(message));
    }

    assertTrue(threadExceptions.isEmpty());
  }

  private void validate(TestRFile trf) throws IOException {
    random.ints(10, 0, 4).forEach(part -> {
      try {
        Range range = new Range(getKey(part, 0, 0), true, getKey(part, 4, 2048), true);
        trf.iter.seek(range, EMPTY_COL_FAMS, false);

        Key last = null;
        for (int locality = 0; locality < 4; locality++) {
          for (int i = 0; i < 2048; i++) {
            Key key = getKey(part, locality, i);
            Value value = getValue(i);
            assertTrue(trf.iter.hasTop(),
                "No record found for row " + part + " locality " + locality + " index " + i);
            assertEquals(key, trf.iter.getTopKey(),
                "Invalid key found for row " + part + " locality " + locality + " index " + i);
            assertEquals(value, trf.iter.getTopValue(),
                "Invalid value found for row " + part + " locality " + locality + " index " + i);
            last = trf.iter.getTopKey();
            trf.iter.next();
          }
        }
        if (trf.iter.hasTop()) {
          assertFalse(trf.iter.hasTop(),
              "Found " + trf.iter.getTopKey() + " after " + last + " in " + range);
        }

        range = new Range(getKey(4, 4, 0), true, null, true);
        trf.iter.seek(range, EMPTY_COL_FAMS, false);
        if (trf.iter.hasTop()) {
          assertFalse(trf.iter.hasTop(), "Found " + trf.iter.getTopKey() + " in " + range);
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    });

    Range range = new Range((Key) null, null);
    trf.iter.seek(range, EMPTY_COL_FAMS, false);

    Key last = null;
    for (int part = 0; part < 4; part++) {
      for (int locality = 0; locality < 4; locality++) {
        for (int i = 0; i < 2048; i++) {
          Key key = getKey(part, locality, i);
          Value value = getValue(i);
          assertTrue(trf.iter.hasTop(),
              "No record found for row " + part + " locality " + locality + " index " + i);
          assertEquals(key, trf.iter.getTopKey(),
              "Invalid key found for row " + part + " locality " + locality + " index " + i);
          assertEquals(value, trf.iter.getTopValue(),
              "Invalid value found for row " + part + " locality " + locality + " index " + i);
          last = trf.iter.getTopKey();
          trf.iter.next();
        }
      }
    }

    if (trf.iter.hasTop()) {
      assertFalse(trf.iter.hasTop(),
          "Found " + trf.iter.getTopKey() + " after " + last + " in " + range);
    }
  }

  private void writeData(TestRFile trfBase) throws IOException {
    trfBase.openWriter(false);

    try {
      for (int locality = 1; locality < 4; locality++) {
        trfBase.writer.startNewLocalityGroup("locality" + locality,
            Collections.singleton(new ArrayByteSequence(getCf(locality))));
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
