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
package org.apache.accumulo.server.iterators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.fate.zookeeper.TransactionWatcher.Arbitrator;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class MetadataBulkLoadFilterTest {
  static class TestArbitrator implements Arbitrator {
    @Override
    public boolean transactionAlive(String type, long tid) throws Exception {
      return tid == 5;
    }

    @Override
    public boolean transactionComplete(String type, long tid) throws Exception {
      if (tid == 9)
        throw new RuntimeException();
      return tid != 5 && tid != 7;
    }
  }

  static class TestMetadataBulkLoadFilter extends MetadataBulkLoadFilter {
    @Override
    protected Arbitrator getArbitrator() {
      return new TestArbitrator();
    }
  }

  private static void put(TreeMap<Key,Value> tm, String row, ColumnFQ cfq, String val) {
    Key k = new Key(new Text(row), cfq.getColumnFamily(), cfq.getColumnQualifier());
    tm.put(k, new Value(val.getBytes()));
  }

  private static void put(TreeMap<Key,Value> tm, String row, Text cf, String cq, String val) {
    Key k = new Key(new Text(row), cf, new Text(cq));
    if (val == null) {
      k.setDeleted(true);
      tm.put(k, new Value("".getBytes()));
    } else
      tm.put(k, new Value(val.getBytes()));
  }

  @Test
  public void testBasic() throws IOException {
    TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();
    TreeMap<Key,Value> expected = new TreeMap<Key,Value>();

    // following should not be deleted by filter
    put(tm1, "2;m", TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN, "/t1");
    put(tm1, "2;m", DataFileColumnFamily.NAME, "/t1/file1", "1,1");
    put(tm1, "2;m", TabletsSection.BulkFileColumnFamily.NAME, "/t1/file1", "5");
    put(tm1, "2;m", TabletsSection.BulkFileColumnFamily.NAME, "/t1/file3", "7");
    put(tm1, "2;m", TabletsSection.BulkFileColumnFamily.NAME, "/t1/file4", "9");
    put(tm1, "2<", TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN, "/t2");
    put(tm1, "2<", DataFileColumnFamily.NAME, "/t2/file2", "1,1");
    put(tm1, "2<", TabletsSection.BulkFileColumnFamily.NAME, "/t2/file6", "5");
    put(tm1, "2<", TabletsSection.BulkFileColumnFamily.NAME, "/t2/file7", "7");
    put(tm1, "2<", TabletsSection.BulkFileColumnFamily.NAME, "/t2/file8", "9");
    put(tm1, "2<", TabletsSection.BulkFileColumnFamily.NAME, "/t2/fileC", null);

    expected.putAll(tm1);

    // the following should be deleted by filter
    put(tm1, "2;m", TabletsSection.BulkFileColumnFamily.NAME, "/t1/file5", "8");
    put(tm1, "2<", TabletsSection.BulkFileColumnFamily.NAME, "/t2/file9", "8");
    put(tm1, "2<", TabletsSection.BulkFileColumnFamily.NAME, "/t2/fileA", "2");

    TestMetadataBulkLoadFilter iter = new TestMetadataBulkLoadFilter();
    iter.init(new SortedMapIterator(tm1), new HashMap<String,String>(), new IteratorEnvironment() {

      @Override
      public SortedKeyValueIterator<Key,Value> reserveMapFileReader(String mapFileName) throws IOException {
        return null;
      }

      @Override
      public void registerSideChannel(SortedKeyValueIterator<Key,Value> iter) {}

      @Override
      public boolean isFullMajorCompaction() {
        return false;
      }

      @Override
      public IteratorScope getIteratorScope() {
        return IteratorScope.majc;
      }

      @Override
      public AccumuloConfiguration getConfig() {
        return null;
      }
    });

    iter.seek(new Range(), new ArrayList<ByteSequence>(), false);

    TreeMap<Key,Value> actual = new TreeMap<Key,Value>();

    while (iter.hasTop()) {
      actual.put(iter.getTopKey(), iter.getTopValue());
      iter.next();
    }

    Assert.assertEquals(expected, actual);
  }
}
