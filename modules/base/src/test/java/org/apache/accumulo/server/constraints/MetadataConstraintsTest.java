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
package org.apache.accumulo.server.constraints;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.List;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.fate.zookeeper.TransactionWatcher.Arbitrator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

public class MetadataConstraintsTest {

  static class TestMetadataConstraints extends MetadataConstraints {
    @Override
    protected Arbitrator getArbitrator() {
      return new Arbitrator() {

        @Override
        public boolean transactionAlive(String type, long tid) throws Exception {
          if (tid == 9)
            throw new RuntimeException("txid 9 reserved for future use");
          return tid == 5 || tid == 7;
        }

        @Override
        public boolean transactionComplete(String type, long tid) throws Exception {
          return tid != 5 && tid != 7;
        }
      };
    }
  }

  @Test
  public void testCheck() {
    Logger.getLogger(AccumuloConfiguration.class).setLevel(Level.ERROR);
    Mutation m = new Mutation(new Text("0;foo"));
    TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.put(m, new Value("1foo".getBytes()));

    MetadataConstraints mc = new MetadataConstraints();

    List<Short> violations = mc.check(null, m);

    assertNotNull(violations);
    assertEquals(1, violations.size());
    assertEquals(Short.valueOf((short) 3), violations.get(0));

    m = new Mutation(new Text("0:foo"));
    TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.put(m, new Value("1poo".getBytes()));

    violations = mc.check(null, m);

    assertNotNull(violations);
    assertEquals(1, violations.size());
    assertEquals(Short.valueOf((short) 4), violations.get(0));

    m = new Mutation(new Text("0;foo"));
    m.put(new Text("bad_column_name"), new Text(""), new Value("e".getBytes()));

    violations = mc.check(null, m);

    assertNotNull(violations);
    assertEquals(1, violations.size());
    assertEquals(Short.valueOf((short) 2), violations.get(0));

    m = new Mutation(new Text("!!<"));
    TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.put(m, new Value("1poo".getBytes()));

    violations = mc.check(null, m);

    assertNotNull(violations);
    assertEquals(2, violations.size());
    assertEquals(Short.valueOf((short) 4), violations.get(0));
    assertEquals(Short.valueOf((short) 5), violations.get(1));

    m = new Mutation(new Text("0;foo"));
    TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.put(m, new Value("".getBytes()));

    violations = mc.check(null, m);

    assertNotNull(violations);
    assertEquals(1, violations.size());
    assertEquals(Short.valueOf((short) 6), violations.get(0));

    m = new Mutation(new Text("0;foo"));
    TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.put(m, new Value("bar".getBytes()));

    violations = mc.check(null, m);

    assertEquals(null, violations);

    m = new Mutation(new Text("!0<"));
    TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.put(m, new Value("bar".getBytes()));

    violations = mc.check(null, m);

    assertEquals(null, violations);

    m = new Mutation(new Text("!1<"));
    TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.put(m, new Value("bar".getBytes()));

    violations = mc.check(null, m);

    assertNotNull(violations);
    assertEquals(1, violations.size());
    assertEquals(Short.valueOf((short) 4), violations.get(0));

  }

  @Test
  public void testBulkFileCheck() {
    MetadataConstraints mc = new TestMetadataConstraints();
    Mutation m;
    List<Short> violations;

    // inactive txid
    m = new Mutation(new Text("0;foo"));
    m.put(TabletsSection.BulkFileColumnFamily.NAME, new Text("/someFile"), new Value("12345".getBytes()));
    m.put(DataFileColumnFamily.NAME, new Text("/someFile"), new DataFileValue(1, 1).encodeAsValue());
    violations = mc.check(null, m);
    assertNotNull(violations);
    assertEquals(1, violations.size());
    assertEquals(Short.valueOf((short) 8), violations.get(0));

    // txid that throws exception
    m = new Mutation(new Text("0;foo"));
    m.put(TabletsSection.BulkFileColumnFamily.NAME, new Text("/someFile"), new Value("9".getBytes()));
    m.put(DataFileColumnFamily.NAME, new Text("/someFile"), new DataFileValue(1, 1).encodeAsValue());
    violations = mc.check(null, m);
    assertNotNull(violations);
    assertEquals(1, violations.size());
    assertEquals(Short.valueOf((short) 8), violations.get(0));

    // active txid w/ file
    m = new Mutation(new Text("0;foo"));
    m.put(TabletsSection.BulkFileColumnFamily.NAME, new Text("/someFile"), new Value("5".getBytes()));
    m.put(DataFileColumnFamily.NAME, new Text("/someFile"), new DataFileValue(1, 1).encodeAsValue());
    violations = mc.check(null, m);
    assertNull(violations);

    // active txid w/o file
    m = new Mutation(new Text("0;foo"));
    m.put(TabletsSection.BulkFileColumnFamily.NAME, new Text("/someFile"), new Value("5".getBytes()));
    violations = mc.check(null, m);
    assertNotNull(violations);
    assertEquals(1, violations.size());
    assertEquals(Short.valueOf((short) 8), violations.get(0));

    // two active txids w/ files
    m = new Mutation(new Text("0;foo"));
    m.put(TabletsSection.BulkFileColumnFamily.NAME, new Text("/someFile"), new Value("5".getBytes()));
    m.put(DataFileColumnFamily.NAME, new Text("/someFile"), new DataFileValue(1, 1).encodeAsValue());
    m.put(TabletsSection.BulkFileColumnFamily.NAME, new Text("/someFile2"), new Value("7".getBytes()));
    m.put(DataFileColumnFamily.NAME, new Text("/someFile2"), new DataFileValue(1, 1).encodeAsValue());
    violations = mc.check(null, m);
    assertNotNull(violations);
    assertEquals(1, violations.size());
    assertEquals(Short.valueOf((short) 8), violations.get(0));

    // two files w/ one active txid
    m = new Mutation(new Text("0;foo"));
    m.put(TabletsSection.BulkFileColumnFamily.NAME, new Text("/someFile"), new Value("5".getBytes()));
    m.put(DataFileColumnFamily.NAME, new Text("/someFile"), new DataFileValue(1, 1).encodeAsValue());
    m.put(TabletsSection.BulkFileColumnFamily.NAME, new Text("/someFile2"), new Value("5".getBytes()));
    m.put(DataFileColumnFamily.NAME, new Text("/someFile2"), new DataFileValue(1, 1).encodeAsValue());
    violations = mc.check(null, m);
    assertNull(violations);

    // two loaded w/ one active txid and one file
    m = new Mutation(new Text("0;foo"));
    m.put(TabletsSection.BulkFileColumnFamily.NAME, new Text("/someFile"), new Value("5".getBytes()));
    m.put(DataFileColumnFamily.NAME, new Text("/someFile"), new DataFileValue(1, 1).encodeAsValue());
    m.put(TabletsSection.BulkFileColumnFamily.NAME, new Text("/someFile2"), new Value("5".getBytes()));
    violations = mc.check(null, m);
    assertNotNull(violations);
    assertEquals(1, violations.size());
    assertEquals(Short.valueOf((short) 8), violations.get(0));

    // active txid, mutation that looks like split
    m = new Mutation(new Text("0;foo"));
    m.put(TabletsSection.BulkFileColumnFamily.NAME, new Text("/someFile"), new Value("5".getBytes()));
    TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.put(m, new Value("/t1".getBytes()));
    violations = mc.check(null, m);
    assertNull(violations);

    // inactive txid, mutation that looks like split
    m = new Mutation(new Text("0;foo"));
    m.put(TabletsSection.BulkFileColumnFamily.NAME, new Text("/someFile"), new Value("12345".getBytes()));
    TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.put(m, new Value("/t1".getBytes()));
    violations = mc.check(null, m);
    assertNull(violations);

    // active txid, mutation that looks like a load
    m = new Mutation(new Text("0;foo"));
    m.put(TabletsSection.BulkFileColumnFamily.NAME, new Text("/someFile"), new Value("5".getBytes()));
    m.put(TabletsSection.CurrentLocationColumnFamily.NAME, new Text("789"), new Value("127.0.0.1:9997".getBytes()));
    violations = mc.check(null, m);
    assertNull(violations);

    // inactive txid, mutation that looks like a load
    m = new Mutation(new Text("0;foo"));
    m.put(TabletsSection.BulkFileColumnFamily.NAME, new Text("/someFile"), new Value("12345".getBytes()));
    m.put(TabletsSection.CurrentLocationColumnFamily.NAME, new Text("789"), new Value("127.0.0.1:9997".getBytes()));
    violations = mc.check(null, m);
    assertNull(violations);

    // deleting a load flag
    m = new Mutation(new Text("0;foo"));
    m.putDelete(TabletsSection.BulkFileColumnFamily.NAME, new Text("/someFile"));
    violations = mc.check(null, m);
    assertNull(violations);

  }

}
